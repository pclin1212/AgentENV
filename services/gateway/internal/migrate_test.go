package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// stubMigrateBackend records every call it receives so tests can assert the
// exact sequence of methods the gateway orchestrator issued.
//
// By default the create handler honors the new "preserve sandboxID" contract:
// it reads the `sandboxID` field from the request body and echoes it back in
// the response. Set echoRequestSandboxID=false to fall back to the legacy
// behavior of returning the hardcoded createSandboxID (useful to test the
// "target returned a different ID" rejection branch).
type stubMigrateBackend struct {
	snapshotCalled       atomic.Int32
	deleteCalled         atomic.Int32
	createBodies         atomic.Int32
	sandboxGetCalled     atomic.Int32
	sandboxGetStatus     int
	snapshotStatus       int
	createStatus         int
	deleteStatus         int
	createSandboxID      string
	snapshotID           string
	echoRequestSandboxID bool
	// sandboxGetState, when non-empty, is returned by the GET
	// /sandboxes/src handler in the `state` field. Tests set this to
	// "paused" to simulate a paused source sandbox and assert the gateway
	// forwards startPaused=true to the target create call.
	sandboxGetState       string
	lastCreateRequestBody atomic.Value // string

	mu    sync.Mutex
	order []string
}

func (b *stubMigrateBackend) record(step string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.order = append(b.order, step)
}

func (b *stubMigrateBackend) callOrder() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.order...)
}

func newStubMigrateBackend() *stubMigrateBackend {
	return &stubMigrateBackend{
		snapshotStatus:       http.StatusCreated,
		createStatus:         http.StatusCreated,
		deleteStatus:         http.StatusNoContent,
		sandboxGetStatus:     http.StatusOK,
		createSandboxID:      "new-sandbox-id",
		snapshotID:           "snap-1",
		echoRequestSandboxID: true,
		sandboxGetState:      "running",
	}
}

func (b *stubMigrateBackend) handler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/sandboxes/src/snapshots":
		b.record("snapshot")
		b.snapshotCalled.Add(1)
		if b.snapshotStatus >= 200 && b.snapshotStatus < 300 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(b.snapshotStatus)
			_ = json.NewEncoder(w).Encode(map[string]string{"snapshotID": b.snapshotID})
		} else {
			w.WriteHeader(b.snapshotStatus)
		}
	case "/sandboxes":
		b.record("create")
		b.createBodies.Add(1)
		body, _ := io.ReadAll(r.Body)
		b.lastCreateRequestBody.Store(string(body))
		if !bytes.Contains(body, []byte(b.snapshotID)) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if b.createStatus >= 200 && b.createStatus < 300 {
			respID := b.createSandboxID
			if b.echoRequestSandboxID {
				// Mirror the new runtime behavior: if the caller passed a
				// sandboxID in the body, return it verbatim.
				var parsed struct {
					SandboxID string `json:"sandboxID"`
				}
				if err := json.Unmarshal(body, &parsed); err == nil && parsed.SandboxID != "" {
					respID = parsed.SandboxID
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(b.createStatus)
			_ = json.NewEncoder(w).Encode(map[string]string{"sandboxID": respID})
		} else {
			w.WriteHeader(b.createStatus)
		}
	case "/sandboxes/src":
		if r.Method == http.MethodGet {
			b.record("get")
			b.sandboxGetCalled.Add(1)
			if b.sandboxGetStatus < 200 || b.sandboxGetStatus >= 300 {
				w.WriteHeader(b.sandboxGetStatus)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(b.sandboxGetStatus)
			// endAt = now + 300s so the gateway computes ~300s remaining.
			resp := map[string]interface{}{
				"sandboxID": "src",
				"endAt":     time.Now().Add(300 * time.Second).Format(time.RFC3339Nano),
			}
			if b.sandboxGetState != "" {
				resp["state"] = b.sandboxGetState
			}
			_ = json.NewEncoder(w).Encode(resp)
		} else {
			b.record("delete")
			b.deleteCalled.Add(1)
			w.WriteHeader(b.deleteStatus)
		}
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestIsMigrateRequest(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   bool
	}{
		{http.MethodPost, "/sandboxes/abc/migrate", true},
		{http.MethodPost, "/sandboxes/abc/migrate/", true},
		{http.MethodPost, "/sandboxes/abc/pause", false},
		{http.MethodGet, "/sandboxes/abc/migrate", false},
		{http.MethodPost, "/sandboxes/migrate", false},
		{http.MethodPost, "/sandboxes//migrate", false},
	}
	for _, c := range cases {
		req := httptest.NewRequest(c.method, "http://x"+c.path, nil)
		if got := isMigrateRequest(req); got != c.want {
			t.Errorf("isMigrateRequest(%s %s)=%v want %v", c.method, c.path, got, c.want)
		}
	}
}

func TestHandleMigrate_HappyPath(t *testing.T) {
	backend := newStubMigrateBackend()
	upstream := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "src" {
				t.Fatalf("lookup got %q", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-src", Endpoint: upstream.URL},
			}, nil
		},
		getNodeFunc: func(_ context.Context, req *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			if req.GetNodeId() != "node-tgt" {
				t.Fatalf("getnode got %q", req.GetNodeId())
			}
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-tgt", Endpoint: upstream.URL},
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			// RecordAssignment must be called with the *original* sandboxID,
			// since migrate now preserves the ID across nodes.
			if req.GetSandboxId() != "src" {
				t.Fatalf("record got %q, want %q (sandboxID should be preserved)", req.GetSandboxId(), "src")
			}
			if req.GetNode().GetNodeId() != "node-tgt" {
				t.Fatalf("record node got %q", req.GetNode().GetNodeId())
			}
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 30*time.Second, 4<<20)

	body := strings.NewReader(`{"targetNodeID":"node-tgt"}`)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate", body)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp migrateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Response sandboxID must equal the source sandboxID now (migrate
	// preserves the ID across nodes).
	if resp.SandboxID != "src" {
		t.Fatalf("sandboxID=%q, want %q (must be preserved)", resp.SandboxID, "src")
	}
	if resp.NodeID != "node-tgt" {
		t.Fatalf("nodeID=%q", resp.NodeID)
	}
	if resp.SnapshotID != "snap-1" {
		t.Fatalf("snapshotID=%q", resp.SnapshotID)
	}
	if backend.snapshotCalled.Load() != 1 || backend.createBodies.Load() != 1 || backend.deleteCalled.Load() != 1 || backend.sandboxGetCalled.Load() != 1 {
		t.Fatalf("unexpected backend calls: snap=%d create=%d del=%d get=%d",
			backend.snapshotCalled.Load(),
			backend.createBodies.Load(), backend.deleteCalled.Load(), backend.sandboxGetCalled.Load())
	}
	// The running flow must kill the source sandbox BEFORE creating on the
	// target, so the same sandbox ID is never live on two nodes at once.
	if got, want := strings.Join(backend.callOrder(), ","), "get,snapshot,delete,create"; got != want {
		t.Fatalf("running flow call order = %q, want %q", got, want)
	}
	// Verify the create body on target carried the source sandboxID so the
	// runtime preserves it verbatim.
	if got := backend.lastCreateRequestBody.Load().(string); !strings.Contains(got, `"sandboxID":"src"`) {
		t.Fatalf("create body missing sandboxID field: %s", got)
	}
	// Verify the create body also carried a timeout so the migrated sandbox
	// does not fall back to the runtime's short default (15s) and get paused
	// almost immediately after migration.
	if got := backend.lastCreateRequestBody.Load().(string); !strings.Contains(got, `"timeout"`) {
		t.Fatalf("create body missing timeout field: %s", got)
	}
	// Running source → target must be launched Running, so startPaused must
	// be absent or false in the create body.
	if got := backend.lastCreateRequestBody.Load().(string); strings.Contains(got, `"startPaused":true`) {
		t.Fatalf("running source should not set startPaused=true: %s", got)
	}
}

// TestHandleMigrate_PausedSource verifies that when the source sandbox is in
// the Paused state, the gateway mirrors that state on the target by sending
// startPaused=true in the create request. The target sandbox is then created
// directly in the Paused state, preserving the source's run state across
// the migration.
func TestHandleMigrate_PausedSource(t *testing.T) {
	backend := newStubMigrateBackend()
	backend.sandboxGetState = "paused"
	upstream := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-src", Endpoint: upstream.URL},
			}, nil
		},
		getNodeFunc: func(_ context.Context, _ *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-tgt", Endpoint: upstream.URL},
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			if req.GetSandboxId() != "src" {
				t.Fatalf("record got %q, want %q (sandboxID should be preserved)", req.GetSandboxId(), "src")
			}
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 30*time.Second, 4<<20)

	body := strings.NewReader(`{"targetNodeID":"node-tgt"}`)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate", body)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp migrateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// SandboxID is preserved across migration.
	if resp.SandboxID != "src" {
		t.Fatalf("sandboxID=%q, want %q", resp.SandboxID, "src")
	}
	// The gateway must have fetched source metadata (to learn it was paused).
	if backend.sandboxGetCalled.Load() != 1 {
		t.Fatalf("sandboxGet called %d times, want 1", backend.sandboxGetCalled.Load())
	}
	// The create body must carry startPaused=true so the target sandbox is
	// launched directly into the Paused state, mirroring the source.
	got := backend.lastCreateRequestBody.Load().(string)
	if !strings.Contains(got, `"startPaused":true`) {
		t.Fatalf("paused source should set startPaused=true in create body: %s", got)
	}
	// The create body must still preserve the sandboxID and inherit timeout.
	if !strings.Contains(got, `"sandboxID":"src"`) {
		t.Fatalf("create body missing sandboxID field: %s", got)
	}
	if !strings.Contains(got, `"timeout"`) {
		t.Fatalf("create body missing timeout field: %s", got)
	}
	// The paused flow keeps the create-then-delete order: the paused record
	// on the source node is the fallback when the target create fails.
	if got, want := strings.Join(backend.callOrder(), ","), "get,snapshot,create,delete"; got != want {
		t.Fatalf("paused flow call order = %q, want %q", got, want)
	}
}

func TestHandleMigrate_SnapshotFails(t *testing.T) {
	backend := newStubMigrateBackend()
	backend.snapshotStatus = http.StatusInternalServerError
	upstream := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-src", Endpoint: upstream.URL},
			}, nil
		},
		getNodeFunc: func(_ context.Context, _ *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-tgt", Endpoint: upstream.URL},
			}, nil
		},
	}, 30*time.Second, 4<<20)

	body := strings.NewReader(`{"targetNodeID":"node-tgt"}`)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate", body)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 got %d", rec.Code)
	}
	if backend.snapshotCalled.Load() != 1 {
		t.Fatalf("snapshot should be called once, got %d", backend.snapshotCalled.Load())
	}
	if backend.createBodies.Load() != 0 {
		t.Fatalf("create should NOT be called, got %d", backend.createBodies.Load())
	}
	if backend.deleteCalled.Load() != 0 {
		t.Fatalf("source delete should NOT be called when snapshot failed, got %d",
			backend.deleteCalled.Load())
	}
}

// TestHandleMigrate_CreateAndRestoreBothFail covers the terminal branch of
// the running flow: the source sandbox is killed first, the target create
// fails, and the restore-on-source create fails too. The sandbox is then
// lost except for the published snapshot, and the gateway reports 500 with
// the snapshot ID for manual recovery.
func TestHandleMigrate_CreateAndRestoreBothFail(t *testing.T) {
	backend := newStubMigrateBackend()
	backend.createStatus = http.StatusInternalServerError
	upstream := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-src", Endpoint: upstream.URL},
			}, nil
		},
		getNodeFunc: func(_ context.Context, _ *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-tgt", Endpoint: upstream.URL},
			}, nil
		},
	}, 30*time.Second, 4<<20)

	body := strings.NewReader(`{"targetNodeID":"node-tgt"}`)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate", body)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	// Terminal failure: the source was killed first, the target create
	// failed, and the restore-on-source create also failed; the sandbox is
	// lost except for the published snapshot.
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 (restore also failed) got %d body=%s", rec.Code, rec.Body.String())
	}
	// kill -> target create -> target cleanup delete -> source restore create.
	if got, want := strings.Join(backend.callOrder(), ","), "get,snapshot,delete,create,delete,create"; got != want {
		t.Fatalf("call order = %q, want %q", got, want)
	}
	if backend.deleteCalled.Load() != 2 || backend.createBodies.Load() != 2 {
		t.Fatalf("expected kill+cleanup deletes and target+restore creates, got del=%d create=%d",
			backend.deleteCalled.Load(), backend.createBodies.Load())
	}
}

// TestHandleMigrate_TargetReturnsDifferentID covers runtimes that do NOT
// honor the explicit sandboxID field: after the source sandbox was killed,
// the gateway attempts to restore it on the source node from the snapshot;
// when that restore also returns a different ID the migration is terminal
// (500) and the sandbox is lost except for the published snapshot.
func TestHandleMigrate_TargetReturnsDifferentID(t *testing.T) {
	backend := newStubMigrateBackend()
	// Force the stub to ignore the request sandboxID and always return the
	// hardcoded createSandboxID ("new-sandbox-id"), simulating a runtime
	// that doesn't support the sandboxID field.
	backend.echoRequestSandboxID = false
	upstream := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-src", Endpoint: upstream.URL},
			}, nil
		},
		getNodeFunc: func(_ context.Context, _ *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-tgt", Endpoint: upstream.URL},
			}, nil
		},
	}, 30*time.Second, 4<<20)

	body := strings.NewReader(`{"targetNodeID":"node-tgt"}`)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate", body)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 (restore also returned a different ID) got %d body=%s",
			rec.Code, rec.Body.String())
	}
	if got, want := strings.Join(backend.callOrder(), ","), "get,snapshot,delete,create,delete,create"; got != want {
		t.Fatalf("call order = %q, want %q", got, want)
	}
	if backend.deleteCalled.Load() != 2 || backend.createBodies.Load() != 2 {
		t.Fatalf("expected kill+cleanup deletes and target+restore creates, got del=%d create=%d",
			backend.deleteCalled.Load(), backend.createBodies.Load())
	}
}

// TestHandleMigrate_CreateOnTargetFailsRestoredOnSource verifies the running
// migration fallback: when the target create fails after the source sandbox
// was killed, the gateway recreates the sandbox on the SOURCE node from the
// same snapshot (same ID), re-records the assignment to the source node and
// reports 502 so the caller knows the migration did not move the sandbox.
func TestHandleMigrate_CreateOnTargetFailsRestoredOnSource(t *testing.T) {
	sourceBackend := newStubMigrateBackend()
	sourceUpstream := httptest.NewServer(http.HandlerFunc(sourceBackend.handler))
	defer sourceUpstream.Close()

	targetBackend := newStubMigrateBackend()
	targetBackend.createStatus = http.StatusInternalServerError
	targetUpstream := httptest.NewServer(http.HandlerFunc(targetBackend.handler))
	defer targetUpstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-src", Endpoint: sourceUpstream.URL},
			}, nil
		},
		getNodeFunc: func(_ context.Context, _ *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-tgt", Endpoint: targetUpstream.URL},
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			if req.GetSandboxId() != "src" {
				t.Fatalf("record got %q, want %q (sandboxID should be preserved)", req.GetSandboxId(), "src")
			}
			if req.GetNode().GetNodeId() != "node-src" {
				t.Fatalf("record node got %q, want node-src (restored on source)", req.GetNode().GetNodeId())
			}
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 30*time.Second, 4<<20)

	body := strings.NewReader(`{"targetNodeID":"node-tgt"}`)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate", body)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 (migration failed, restored on source) got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "restored on source node") {
		t.Fatalf("response should explain the source-node restore: %s", rec.Body.String())
	}
	if got, want := strings.Join(sourceBackend.callOrder(), ","), "get,snapshot,delete,create"; got != want {
		t.Fatalf("source call order = %q, want %q", got, want)
	}
	if got, want := strings.Join(targetBackend.callOrder(), ","), "create,delete"; got != want {
		t.Fatalf("target call order = %q, want %q", got, want)
	}
}

func TestHandleMigrate_SameTargetRejected(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-same", Endpoint: upstream.URL},
			}, nil
		},
	}, 30*time.Second, 4<<20)

	body := strings.NewReader(`{"targetNodeID":"node-same"}`)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate", body)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d", rec.Code)
	}
}

func TestHandleMigrate_BadBody(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 30*time.Second, 4<<20)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate",
		strings.NewReader(`{bad json`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d", rec.Code)
	}
}

func TestHandleMigrate_TargetNodeIDRequiredWithEndpointOverride(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 30*time.Second, 4<<20)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate",
		strings.NewReader(`{"targetNodeEndpoint":"http://target:8000"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleMigrate_SourceMetadataFailureStopsBeforeSnapshot(t *testing.T) {
	backend := newStubMigrateBackend()
	backend.sandboxGetStatus = http.StatusInternalServerError
	upstream := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-src", Endpoint: upstream.URL},
			}, nil
		},
		getNodeFunc: func(_ context.Context, _ *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-tgt", Endpoint: upstream.URL},
			}, nil
		},
	}, 30*time.Second, 4<<20)

	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate",
		strings.NewReader(`{"targetNodeID":"node-tgt"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 got %d body=%s", rec.Code, rec.Body.String())
	}
	if backend.snapshotCalled.Load() != 0 || backend.deleteCalled.Load() != 0 || backend.createBodies.Load() != 0 {
		t.Fatalf("migration should stop after metadata failure: snapshot=%d delete=%d create=%d",
			backend.snapshotCalled.Load(), backend.deleteCalled.Load(), backend.createBodies.Load())
	}
}

func TestHandleMigrate_TransitionalSourceStateRejected(t *testing.T) {
	backend := newStubMigrateBackend()
	backend.sandboxGetState = "pausing"
	upstream := httptest.NewServer(http.HandlerFunc(backend.handler))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-src", Endpoint: upstream.URL},
			}, nil
		},
		getNodeFunc: func(_ context.Context, _ *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-tgt", Endpoint: upstream.URL},
			}, nil
		},
	}, 30*time.Second, 4<<20)

	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate",
		strings.NewReader(`{"targetNodeID":"node-tgt"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 got %d body=%s", rec.Code, rec.Body.String())
	}
	if backend.snapshotCalled.Load() != 0 || backend.deleteCalled.Load() != 0 || backend.createBodies.Load() != 0 {
		t.Fatalf("migration should stop for transitional source state: snapshot=%d delete=%d create=%d",
			backend.snapshotCalled.Load(), backend.deleteCalled.Load(), backend.createBodies.Load())
	}
}

func TestHandleMigrate_LookupNotFound(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			return &schedulerv1.LookupNodeResponse{}, nil // empty node
		},
	}, 30*time.Second, 4<<20)

	body := strings.NewReader(`{"targetNodeID":"node-tgt"}`)
	req := httptest.NewRequest(http.MethodPost, "http://gateway/sandboxes/src/migrate", body)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 got %d", rec.Code)
	}
}

// Ensure unused imports don't trip the linter when test matrix expands.
var _ = zap.NewNop
