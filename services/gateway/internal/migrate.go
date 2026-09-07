package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// migrateRequest is the JSON body accepted by POST /sandboxes/{sandboxID}/migrate.
//
// targetNodeID is the node identifier (as known to the scheduler) that should
// take over ownership of the sandbox. targetNodeEndpoint, when set, overrides
// the scheduler-resolved endpoint for the target node; this is useful when the
// caller already knows the runtime endpoint and wants to bypass GetNode.
type migrateRequest struct {
	TargetNodeID string `json:"targetNodeID"`
	// optional, falls back to scheduler GetNode when empty
	TargetNodeEndpoint string `json:"targetNodeEndpoint,omitempty"`
}

// migrateResponse is returned on a successful migration.
type migrateResponse struct {
	SandboxID string `json:"sandboxID"`
	NodeID    string `json:"nodeID"`
	// SnapshotID is the artifact id produced on the source node and consumed
	// by the target node to start the new sandbox.
	SnapshotID string `json:"snapshotID,omitempty"`
}

// migrateTimeout caps the total orchestration budget. Migration involves
// several sequential network round-trips (LookupNode, GetNode, pause,
// snapshot, create-on-target, delete-on-source, RecordAssignment); each
// upstream call additionally has its own per-step timeout.
const migrateTimeout = 5 * time.Minute

// perStepTimeout bounds each individual upstream HTTP call.
const perStepTimeout = 30 * time.Second

// handleMigrate implements POST /sandboxes/{sandboxID}/migrate.
//
// Migration orchestration:
//
//  1. LookupNode(sandboxID)          → resolve source node endpoint
//  2. GetNode(targetNodeID)          → resolve target node endpoint
//  3. GET /sandboxes/{id} (source)   → remaining TTL + run state
//  4. POST /sandboxes/{id}/snapshots → capture snapshot (Running or Paused
//     source) and publish it, returns snapshotID
//  5. Running source: DELETE /sandboxes/{id} on the source node BEFORE the
//     target create, so a failed source delete can no longer leave the same
//     sandbox ID live on two nodes. Paused source: keep the paused record as
//     the fallback and delete it only after the target create succeeded
//     (best-effort).
//  6. POST /sandboxes {templateID:snapshotID, sandboxID, timeout,
//     startPaused} on the target → recreate the sandbox with the SAME ID
//  7. Running source whose target create failed: recreate the sandbox on
//     the SOURCE node from the same snapshot, re-record the assignment to
//     the source node and return 502. If the restore also fails the sandbox
//     is lost except for the published snapshot (500, snapshot ID included).
//  8. RecordAssignment(sandboxID, target) → rebind routing to target node
//
// The handler returns success only when the target-side create completed
// with the requested sandbox ID.
func (s *Server) handleMigrate(w http.ResponseWriter, r *http.Request) {
	sandboxID, ok := sandboxIDFromPath(r.URL.Path)
	if !ok {
		http.Error(w, "sandbox id required", http.StatusBadRequest)
		return
	}

	var req migrateRequest
	if err := decodeJSONBody(r, &req); err != nil {
		http.Error(w, fmt.Sprintf("invalid migrate body: %v", err), http.StatusBadRequest)
		return
	}
	targetNodeID := strings.TrimSpace(req.TargetNodeID)
	if targetNodeID == "" {
		http.Error(w, "targetNodeID is required", http.StatusBadRequest)
		return
	}
	targetNodeEndpoint := strings.TrimSpace(req.TargetNodeEndpoint)

	migrateCtx, cancel := context.WithTimeout(r.Context(), migrateTimeout)
	defer cancel()

	// 1. Resolve source node endpoint (also confirms the sandbox is bound).
	srcRPCTime := time.Now()
	srcResp, err := s.queryOnlyScheduler.LookupNode(migrateCtx, &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
	recordGatewaySchedulerRPC("LookupNode", srcRPCTime, err)
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}
	sourceNode := srcResp.GetNode()
	if sourceNode.GetNodeId() == "" {
		http.Error(w, "sandbox binding not found", http.StatusNotFound)
		return
	}
	if sourceNode.GetNodeId() == targetNodeID {
		http.Error(w, "target node is the same as source node", http.StatusBadRequest)
		return
	}

	// 2. Resolve target node endpoint.
	var targetNode *schedulerv1.Node
	if targetNodeEndpoint != "" {
		targetNode = &schedulerv1.Node{
			NodeId:   targetNodeID,
			Endpoint: targetNodeEndpoint,
		}
	} else {
		tgtRPCTime := time.Now()
		tgtResp, err := s.scheduler.GetNode(migrateCtx, &schedulerv1.GetNodeRequest{NodeId: targetNodeID})
		recordGatewaySchedulerRPC("GetNode", tgtRPCTime, err)
		if err != nil {
			s.writeSchedulerError(w, err)
			return
		}
		observed := tgtResp.GetNode()
		if observed == nil || observed.GetNodeId() == "" {
			http.Error(w, "target node not found", http.StatusNotFound)
			return
		}
		targetNode = &schedulerv1.Node{
			NodeId:   observed.GetNodeId(),
			Endpoint: observed.GetEndpoint(),
		}
	}
	if targetNode.GetEndpoint() == "" {
		http.Error(w, "target node endpoint is empty", http.StatusBadRequest)
		return
	}

	s.logger.Info("migrate started",
		zap.String("sandbox_id", sandboxID),
		zap.String("source_node", sourceNode.GetNodeId()),
		zap.String("source_endpoint", sourceNode.GetEndpoint()),
		zap.String("target_node", targetNode.GetNodeId()),
		zap.String("target_endpoint", targetNode.GetEndpoint()),
	)

	// 3a. Fetch source sandbox metadata to inherit its remaining TTL so the
	// migrated sandbox does not fall back to the runtime's (short) default
	// timeout and get paused almost immediately after migration. We also
	// read the source's state so we can mirror it on the target: a paused
	// source is migrated to a paused target (using startPaused=true), and
	// a running source is migrated to a running target (the legacy path).
	var srcInfo struct {
		EndAt time.Time `json:"endAt"`
		State string    `json:"state"`
	}
	if err := s.migrateCall(migrateCtx, sourceNode.GetEndpoint(), http.MethodGet,
		"/sandboxes/"+sandboxID, nil, &srcInfo); err != nil {
		s.logger.Warn("migrate: fetch source sandbox metadata failed",
			zap.String("sandbox_id", sandboxID),
			zap.String("source_node", sourceNode.GetNodeId()),
			zap.Error(err),
		)
		http.Error(w, "migrate: fetch source sandbox metadata failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	remainingSecs := 300
	if !srcInfo.EndAt.IsZero() {
		if secs := int(time.Until(srcInfo.EndAt).Seconds()); secs > 0 {
			remainingSecs = secs
		}
	}
	// Mirror the source's run state on the target. Migrating a transitional or
	// unknown state would make the target's state ambiguous, so reject it before
	// the source is snapshotted or deleted.
	var sourcePaused bool
	switch strings.ToLower(strings.TrimSpace(srcInfo.State)) {
	case "running":
		sourcePaused = false
	case "paused":
		sourcePaused = true
	default:
		http.Error(w, fmt.Sprintf("migrate: source sandbox state %q is not migratable", srcInfo.State), http.StatusConflict)
		return
	}

	// 3b. Snapshot on source node. The runtime now accepts both Running and
	// Paused source states: a Running source is briefly paused then resumed
	// (legacy path); a Paused source stays paused throughout capture so its
	// state is preserved for the migration.
	var snapshot struct {
		SnapshotID string `json:"snapshotID"`
	}
	if err := s.migrateCall(migrateCtx, sourceNode.GetEndpoint(), http.MethodPost,
		"/sandboxes/"+sandboxID+"/snapshots", nil, &snapshot); err != nil {
		s.logger.Warn("migrate: snapshot on source failed",
			zap.String("sandbox_id", sandboxID),
			zap.String("source_node", sourceNode.GetNodeId()),
			zap.Error(err),
		)
		http.Error(w, "migrate: snapshot source failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	if strings.TrimSpace(snapshot.SnapshotID) == "" {
		http.Error(w, "migrate: source snapshot returned empty id", http.StatusBadGateway)
		return
	}

	// 4. Running source: kill the sandbox on the source node BEFORE creating
	//    on the target, so the same sandbox ID is never live on two nodes at
	//    once. The published snapshot is now the only copy of the sandbox; if
	//    the target create fails, the sandbox is recreated on the source node
	//    from the same snapshot (fallback below). A paused source keeps the
	//    create-then-delete order: its paused record on the source node is
	//    itself the fallback, so it must not be deleted up front.
	if !sourcePaused {
		if err := s.migrateCall(migrateCtx, sourceNode.GetEndpoint(), http.MethodDelete,
			"/sandboxes/"+sandboxID, nil, nil); err != nil {
			s.logger.Warn("migrate: kill on source failed; aborting before target create",
				zap.String("sandbox_id", sandboxID),
				zap.String("source_node", sourceNode.GetNodeId()),
				zap.Error(err),
			)
			http.Error(w, "migrate: kill source failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		s.logger.Info("migrate: source sandbox killed",
			zap.String("sandbox_id", sandboxID),
			zap.String("source_node", sourceNode.GetNodeId()),
			zap.String("snapshot_id", snapshot.SnapshotID),
		)
	}

	// 5. Create new sandbox on target using the snapshot as templateID, while
	//    preserving the source sandbox's ID so callers can keep using the
	//    same identifier after migration. The runtime's POST /sandboxes
	//    endpoint honors an optional `sandboxID` field: when set, the runtime
	//    uses it verbatim instead of generating a fresh UUIDv7. The runtime
	//    rejects with 409 if a sandbox with that ID already exists on this node.
	//    When migrating from a paused source, `startPaused=true` tells the
	//    runtime to launch the target directly into the Paused state so the
	//    sandbox's run state is preserved across the migration.
	createBody, _ := json.Marshal(map[string]interface{}{
		"templateID":  snapshot.SnapshotID,
		"sandboxID":   sandboxID,
		"timeout":     remainingSecs,
		"startPaused": sourcePaused,
	})
	var created struct {
		SandboxID string `json:"sandboxID"`
	}
	createErr := s.migrateCall(migrateCtx, targetNode.GetEndpoint(), http.MethodPost,
		"/sandboxes", bytes.NewReader(createBody), &created)
	newSandboxID := ""
	if createErr == nil {
		newSandboxID = strings.TrimSpace(created.SandboxID)
		if newSandboxID == "" {
			// Fall back to header if backend used x-agentenv-sandbox-id.
			newSandboxID = sandboxID
		}
	}
	if createErr != nil || newSandboxID != sandboxID {
		targetErr := createErr
		if targetErr == nil {
			// The runtime was supposed to honor our explicit sandboxID, but
			// returned a different one: either an older runtime without the
			// sandboxID field, or a routing/encoding bug.
			targetErr = fmt.Errorf("target returned sandboxID %q, expected %q (target runtime may not support explicit sandboxID)",
				newSandboxID, sandboxID)
		}
		s.logger.Warn("migrate: create on target failed",
			zap.String("sandbox_id", sandboxID),
			zap.String("target_node", targetNode.GetNodeId()),
			zap.String("snapshot_id", snapshot.SnapshotID),
			zap.String("returned_sandbox_id", newSandboxID),
			zap.Error(targetErr),
		)
		if sourcePaused {
			// Paused flow: the source paused record was kept as the fallback,
			// so this is a clean failure; the sandbox stays Paused on the
			// source node and routing never moved.
			http.Error(w, "migrate: create target failed: "+targetErr.Error(), http.StatusBadGateway)
			return
		}

		// Running flow: the source sandbox was already killed. Best-effort
		// clean up anything the target may have created despite the error
		// (the create can still land after the response is lost), then
		// recreate the sandbox on the source node from the same snapshot so
		// the service is restored where it was.
		if newSandboxID != "" && newSandboxID != sandboxID {
			_ = s.migrateCall(migrateCtx, targetNode.GetEndpoint(), http.MethodDelete,
				"/sandboxes/"+newSandboxID, nil, nil)
		}
		_ = s.migrateCall(migrateCtx, targetNode.GetEndpoint(), http.MethodDelete,
			"/sandboxes/"+sandboxID, nil, nil)
		var restored struct {
			SandboxID string `json:"sandboxID"`
		}
		restoreErr := s.migrateCall(migrateCtx, sourceNode.GetEndpoint(), http.MethodPost,
			"/sandboxes", bytes.NewReader(createBody), &restored)
		restoredID := ""
		if restoreErr == nil {
			restoredID = strings.TrimSpace(restored.SandboxID)
			if restoredID == "" {
				restoredID = sandboxID
			}
		}
		if restoreErr != nil || restoredID != sandboxID {
			restoreFailErr := restoreErr
			if restoreFailErr == nil {
				restoreFailErr = fmt.Errorf("source restore returned sandboxID %q, expected %q",
					restoredID, sandboxID)
			}
			// Terminal: the sandbox no longer exists on any node; only the
			// published snapshot remains for manual recovery.
			s.logger.Error("migrate: restore on source failed; sandbox lost (snapshot remains)",
				zap.String("sandbox_id", sandboxID),
				zap.String("source_node", sourceNode.GetNodeId()),
				zap.String("snapshot_id", snapshot.SnapshotID),
				zap.NamedError("target_create_err", targetErr),
				zap.NamedError("restore_err", restoreFailErr),
			)
			if restoredID != "" && restoredID != sandboxID {
				_ = s.migrateCall(migrateCtx, sourceNode.GetEndpoint(), http.MethodDelete,
					"/sandboxes/"+restoredID, nil, nil)
			}
			http.Error(w, fmt.Sprintf(
				"migrate: create on target failed (%v) and restore on source node failed (%v); sandbox %q is lost, snapshot %q remains for manual recovery",
				targetErr, restoreFailErr, sandboxID, snapshot.SnapshotID), http.StatusInternalServerError)
			return
		}
		// Sandbox restored on the source node. Routing never moved, but
		// re-record the assignment so the scheduler binding stays explicit.
		recCtx, cancelRec := context.WithTimeout(r.Context(), recordAssignmentTimeout(s.requestTimeout))
		defer cancelRec()
		recRPCTime := time.Now()
		_, recErr := s.scheduler.RecordAssignment(recCtx, &schedulerv1.RecordAssignmentRequest{
			SandboxId: sandboxID,
			Node:      sourceNode,
		})
		recordGatewaySchedulerRPC("RecordAssignment", recRPCTime, recErr)
		if recErr != nil {
			s.logger.Warn("migrate: re-record assignment to source after restore failed",
				zap.String("sandbox_id", sandboxID),
				zap.String("source_node", sourceNode.GetNodeId()),
				zap.Error(recErr),
			)
		}
		http.Error(w, fmt.Sprintf(
			"migrate: create on target failed (%v); sandbox restored on source node %s from snapshot %s",
			targetErr, sourceNode.GetNodeId(), snapshot.SnapshotID), http.StatusBadGateway)
		return
	}

	// 6. Best-effort delete on the source node. Only reached for paused
	//    sources: a running source was already killed before the create.
	var delErr error
	if sourcePaused {
		delErr = s.migrateCall(migrateCtx, sourceNode.GetEndpoint(), http.MethodDelete,
			"/sandboxes/"+sandboxID, nil, nil)
		if delErr != nil {
			s.logger.Warn("migrate: delete on source failed (best-effort)",
				zap.String("sandbox_id", sandboxID),
				zap.String("source_node", sourceNode.GetNodeId()),
				zap.Error(delErr),
			)
		}
	}

	// 7. RecordAssignment(newID, target) so subsequent routing hits target.
	recCtx, cancelRec := context.WithTimeout(r.Context(), recordAssignmentTimeout(s.requestTimeout))
	defer cancelRec()
	recRPCTime := time.Now()
	_, recErr := s.scheduler.RecordAssignment(recCtx, &schedulerv1.RecordAssignmentRequest{
		SandboxId: newSandboxID,
		Node:      targetNode,
	})
	recordGatewaySchedulerRPC("RecordAssignment", recRPCTime, recErr)
	if recErr != nil {
		// Sandbox exists on target but routing still points to source.
		// Surface the warning to the caller as a non-2xx is misleading; the
		// migration succeeded, only the binding update failed.
		s.logger.Warn("migrate: record assignment to target failed (sandbox already created)",
			zap.String("sandbox_id", newSandboxID),
			zap.String("target_node", targetNode.GetNodeId()),
			zap.Error(recErr),
		)
	}

	s.logger.Info("migrate completed",
		zap.String("source_sandbox_id", sandboxID),
		zap.String("new_sandbox_id", newSandboxID),
		zap.String("source_node", sourceNode.GetNodeId()),
		zap.String("target_node", targetNode.GetNodeId()),
		zap.String("snapshot_id", snapshot.SnapshotID),
		zap.NamedError("source_delete_err", delErr),
		zap.NamedError("record_assignment_err", recErr),
	)

	s.writeJSON(w, http.StatusOK, migrateResponse{
		SandboxID:  newSandboxID,
		NodeID:     targetNode.GetNodeId(),
		SnapshotID: snapshot.SnapshotID,
	})
}

// migrateCall performs a single HTTP call to a backend node during migration.
// `body` may be nil for GET/DELETE/empty-POST; `out` (if non-nil) is filled
// from the JSON response body on 2xx. Non-2xx responses are surfaced as
// errors with the upstream body excerpt.
func (s *Server) migrateCall(ctx context.Context, endpoint, method, path string, body io.Reader, out any) error {
	reqURL := strings.TrimRight(endpoint, "/") + path
	reqCtx, cancel := context.WithTimeout(ctx, perStepTimeout)
	defer cancel()

	// POST/PUT/PATCH endpoints on the runtime require a JSON content-type
	// and a non-empty body; an empty body triggers a 400 "EOF while parsing
	// a value" from the JSON parser. Substitute "{}" when caller passed nil.
	if body == nil {
		switch method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			body = bytes.NewReader([]byte("{}"))
		}
	}
	req, err := http.NewRequestWithContext(reqCtx, method, reqURL, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(headerAPIKey, string(s.apiKey))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("call %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("upstream %s %s returned status %d: %s",
			method, path, resp.StatusCode, strings.TrimSpace(string(excerpt)))
	}

	if out == nil {
		// Drain to allow connection reuse.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, s.maxRespSize))
	if err != nil {
		return fmt.Errorf("read response %s %s: %w", method, path, err)
	}
	if len(raw) == 0 {
		// Allow callers to fall back to header parsing (already in caller).
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response %s %s: %w", method, path, err)
	}
	return nil
}

// decodeJSONBody parses the request body into dst and is tolerant of an empty
// body (useful when callers PUT/PATCH without a payload).
func decodeJSONBody(r *http.Request, dst any) error {
	if r.Body == nil {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, dst)
}
