#!/usr/bin/env python3
"""Compare snapshot managed-layer upload time for Mooncake and OSS/S3.

This script reads AgentENV server logs directly. It intentionally does not
consume replay-aenv results, because those results measure end-to-end latency.
In addition to upload throughput, it derives a server-side end-to-end
approximation for the user-facing snapshot-create API from existing stage logs.

Comparable metrics:

* Mooncake: ``read_ms + upload_ms`` from
  ``snapshot publish layer processed`` events where ``uploaded=true``.
* OSS/S3: ``elapsed_ms`` from ``oss file uploaded`` events for managed layers.

Mooncake's metric ends when the client PUT completes. If Mooncake later
offloads a memory replica to SSD, that asynchronous/offload time is not part of
this metric.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Iterator, TextIO


ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
FIELD_RE = re.compile(
    r"(?:^|\s)([A-Za-z_][A-Za-z0-9_.-]*)="
    r"(\"(?:\\.|[^\"])*\"|'(?:\\.|[^'])*'|[^\s]+)"
)

MOONCAKE_MESSAGE = "snapshot publish layer processed"
OSS_MESSAGE = "oss file uploaded"
SANDBOX_STAGE_MESSAGE = "sandbox stage elapsed"
SNAPSHOT_CAPTURED_MESSAGE = "snapshot captured"
PUBLISH_STAGE_MESSAGE = "snapshot publish stage elapsed"
SNAPSHOT_API_SPAN = "sandboxes_sandbox_id_snapshots_post"

OSS_ARTIFACTS = {
    "rootfs_layer": "rootfs",
    "memory_layer": "memory",
    "attached_drive_layer": "attached_drive",
}


@dataclass(frozen=True)
class UploadRecord:
    backend: str
    artifact: str
    size_bytes: int
    remote_ms: float
    source_file: str
    line_number: int
    snapshot_id: str = ""
    layer_index: int | None = None
    key: str = ""
    digest: str = ""
    read_ms: float = 0.0
    upload_ms: float = 0.0
    snapshot_api: bool = False


@dataclass(frozen=True)
class StageRecord:
    stage: str
    elapsed_ms: float
    source_file: str
    line_number: int
    sandbox_id: str = ""
    snapshot_id: str = ""


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().rstrip(",").lower() == "true"


def parse_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(str(value).strip().rstrip(",")))
    except (TypeError, ValueError):
        return default


def parse_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip().rstrip(","))
    except (TypeError, ValueError):
        return default


def decode_compact_value(value: str) -> str:
    value = value.rstrip(",")
    if value.startswith('"') and value.endswith('"'):
        try:
            return str(json.loads(value))
        except json.JSONDecodeError:
            return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    return value


def parse_log_line(line: str) -> tuple[str, dict[str, object]]:
    """Parse compact or tracing JSON output into message and event fields."""
    line = ANSI_RE.sub("", line.strip())
    if not line:
        return "", {}

    if line.startswith("{"):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            pass
        else:
            fields: dict[str, object] = {}
            for span in payload.get("spans", []) or []:
                if isinstance(span, dict):
                    fields.update(span)
            if isinstance(payload.get("span"), dict):
                fields.update(payload["span"])
            event_fields = payload.get("fields", {})
            if isinstance(event_fields, dict):
                fields.update(event_fields)
            message = fields.get("message", payload.get("message", ""))
            return str(message), fields

    fields = {
        key: decode_compact_value(value)
        for key, value in FIELD_RE.findall(line)
    }
    if MOONCAKE_MESSAGE in line:
        return MOONCAKE_MESSAGE, fields
    if OSS_MESSAGE in line:
        return OSS_MESSAGE, fields
    if SANDBOX_STAGE_MESSAGE in line:
        return SANDBOX_STAGE_MESSAGE, fields
    if SNAPSHOT_CAPTURED_MESSAGE in line:
        return SNAPSHOT_CAPTURED_MESSAGE, fields
    if PUBLISH_STAGE_MESSAGE in line:
        return PUBLISH_STAGE_MESSAGE, fields
    return "", fields


def parse_upload_line(
    line: str, source_file: str, line_number: int
) -> UploadRecord | None:
    message, fields = parse_log_line(line)

    if message == MOONCAKE_MESSAGE:
        if str(fields.get("component", "")) != "mooncake":
            return None
        if not parse_bool(fields.get("uploaded", False)):
            return None
        if parse_bool(fields.get("external", False)):
            return None

        artifact = str(fields.get("layer_group", "unknown"))
        read_ms = parse_float(fields.get("read_ms"))
        upload_ms = parse_float(fields.get("upload_ms"))
        layer_index = (
            parse_int(fields["layer_index"])
            if "layer_index" in fields
            else None
        )
        return UploadRecord(
            backend="mooncake",
            artifact=artifact,
            size_bytes=parse_int(fields.get("size_bytes")),
            remote_ms=read_ms + upload_ms,
            read_ms=read_ms,
            upload_ms=upload_ms,
            source_file=source_file,
            line_number=line_number,
            snapshot_id=str(fields.get("snapshot_id", "")),
            layer_index=layer_index,
            digest=str(fields.get("digest", "")),
            snapshot_api=SNAPSHOT_API_SPAN in line,
        )

    if message == OSS_MESSAGE:
        raw_artifact = str(fields.get("artifact", ""))
        artifact = OSS_ARTIFACTS.get(raw_artifact)
        if artifact is None:
            return None
        return UploadRecord(
            backend="s3",
            artifact=artifact,
            size_bytes=parse_int(fields.get("size_bytes")),
            remote_ms=parse_float(fields.get("elapsed_ms")),
            source_file=source_file,
            line_number=line_number,
            snapshot_id=str(fields.get("snapshot_id", "")),
            key=str(fields.get("key", "")),
            snapshot_api=SNAPSHOT_API_SPAN in line,
        )

    return None


def parse_stage_line(
    line: str, source_file: str, line_number: int
) -> StageRecord | None:
    # Publishing stays in the HTTP request span, while capture runs through an
    # independently instrumented/cancellation-safe orchestrator task and may
    # retain only the capture_snapshot span. Accept both contexts here and
    # apply the appropriate one to each stage below.
    in_snapshot_api = SNAPSHOT_API_SPAN in line
    in_snapshot_capture = "capture_snapshot" in line
    if not in_snapshot_api and not in_snapshot_capture:
        return None
    message, fields = parse_log_line(line)
    operation = str(fields.get("operation", ""))
    raw_stage = str(fields.get("stage", ""))
    stage = ""

    if (
        message == SNAPSHOT_CAPTURED_MESSAGE
        and operation == "snapshot"
        and in_snapshot_capture
    ):
        stage = "capture_total"
    elif message == SANDBOX_STAGE_MESSAGE:
        if operation == "snapshot" and raw_stage == "publish" and in_snapshot_api:
            stage = "publish_total"
        elif (
            operation == "snapshot"
            and raw_stage in {"backend_snapshot", "fc_vm_resume"}
            and in_snapshot_capture
        ):
            stage = raw_stage
        elif (
            operation == "pause"
            and raw_stage
            in {
                "fc_vm_pause",
                "memory_to_overlaybd",
                "rootfs_snapshot",
                "extra_drives",
            }
            and in_snapshot_capture
        ):
            # These Firecracker events use operation="pause" even when they
            # run inside snapshot create. Requiring capture_snapshot excludes
            # events from standalone pause.
            stage = raw_stage
    elif message == PUBLISH_STAGE_MESSAGE and in_snapshot_api:
        component = str(fields.get("component", ""))
        if component == "manager" and raw_stage == "total":
            stage = "manager_publish_total"
        elif component == "manager" and raw_stage == "repository_publish":
            stage = "repository_publish"
        elif component == "manager" and raw_stage == "p2p_publish":
            stage = "p2p_publish"

    if not stage:
        return None
    return StageRecord(
        stage=stage,
        elapsed_ms=parse_float(fields.get("elapsed_ms")),
        source_file=source_file,
        line_number=line_number,
        sandbox_id=str(fields.get("sandbox_id", "")),
        snapshot_id=str(fields.get("snapshot_id", "")),
    )


def open_log(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def expand_log_paths(paths: Iterable[str]) -> list[Path]:
    """Expand files/directories and remove duplicate resolved paths."""
    result: list[Path] = []
    seen: set[Path] = set()
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        candidates: Iterator[Path]
        if path.is_dir():
            candidates = (candidate for candidate in path.rglob("*") if candidate.is_file())
        elif path.is_file():
            candidates = iter((path,))
        else:
            raise FileNotFoundError(f"log path does not exist: {path}")

        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved not in seen:
                seen.add(resolved)
                result.append(resolved)
    return result


def parse_logs(
    paths: Iterable[Path],
) -> tuple[list[UploadRecord], dict[str, list[StageRecord]]]:
    records: list[UploadRecord] = []
    per_file: list[tuple[Path, list[UploadRecord], list[StageRecord]]] = []
    for path in paths:
        file_uploads: list[UploadRecord] = []
        file_stages: list[StageRecord] = []
        try:
            handle = open_log(path)
            with handle:
                for line_number, line in enumerate(handle, 1):
                    record = parse_upload_line(line, str(path), line_number)
                    if record is not None:
                        file_uploads.append(record)
                    stage = parse_stage_line(line, str(path), line_number)
                    if stage is not None:
                        file_stages.append(stage)
        except (OSError, UnicodeError) as error:
            print(f"warning: skip {path}: {error}", file=sys.stderr)
            continue
        records.extend(file_uploads)
        per_file.append((path, file_uploads, file_stages))

    detected_backends = {record.backend for record in records}
    stages_by_backend: dict[str, list[StageRecord]] = defaultdict(list)
    for path, file_uploads, file_stages in per_file:
        file_backends = {record.backend for record in file_uploads}
        if len(file_backends) == 1:
            backend = next(iter(file_backends))
        elif not file_backends and len(detected_backends) == 1:
            backend = next(iter(detected_backends))
        else:
            if file_stages:
                print(
                    f"warning: cannot assign snapshot stages in {path} to one backend; "
                    "use one clean log file per backend run",
                    file=sys.stderr,
                )
            continue
        stages_by_backend[backend].extend(file_stages)

    return records, dict(stages_by_backend)


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    ratio = position - lower
    return ordered[lower] * (1.0 - ratio) + ordered[upper] * ratio


def summarize(records: Iterable[UploadRecord]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[UploadRecord]] = defaultdict(list)
    for record in records:
        grouped[(record.backend, record.artifact)].append(record)

    summaries: list[dict[str, object]] = []
    for (backend, artifact), items in sorted(grouped.items()):
        durations = [item.remote_ms for item in items]
        total_bytes = sum(item.size_bytes for item in items)
        total_ms = sum(durations)
        throughput_mib_s = (
            total_bytes / (1024 * 1024) / (total_ms / 1000)
            if total_ms > 0
            else None
        )
        summaries.append(
            {
                "backend": backend,
                "artifact": artifact,
                "count": len(items),
                "total_bytes": total_bytes,
                "total_remote_ms": total_ms,
                "avg_ms": total_ms / len(items),
                "p50_ms": percentile(durations, 0.50),
                "p95_ms": percentile(durations, 0.95),
                "min_ms": min(durations),
                "max_ms": max(durations),
                "weighted_mib_s": throughput_mib_s,
            }
        )
    return summaries


def summarize_values(values: list[float]) -> dict[str, object]:
    total = sum(values)
    return {
        "count": len(values),
        "total_ms": total,
        "avg_ms": total / len(values) if values else 0.0,
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
    }


def summarize_end_to_end(
    records: Iterable[UploadRecord],
    stages_by_backend: dict[str, list[StageRecord]],
) -> list[dict[str, object]]:
    uploads_by_backend: dict[str, list[UploadRecord]] = defaultdict(list)
    for record in records:
        uploads_by_backend[record.backend].append(record)

    results: list[dict[str, object]] = []
    for backend in sorted(set(uploads_by_backend) | set(stages_by_backend)):
        stage_values: dict[str, list[float]] = defaultdict(list)
        for record in stages_by_backend.get(backend, []):
            stage_values[record.stage].append(record.elapsed_ms)

        capture = stage_values["capture_total"]
        publish = stage_values["publish_total"]
        publish_source = "api_publish"
        if not publish:
            publish = stage_values["manager_publish_total"]
            publish_source = "manager_total"

        capture_stats = summarize_values(capture)
        publish_stats = summarize_values(publish)
        backend_snapshot_total = sum(stage_values["backend_snapshot"])
        remote_layer_total = sum(
            record.remote_ms for record in uploads_by_backend.get(backend, [])
        )
        capture_total = float(capture_stats["total_ms"])
        publish_total = float(publish_stats["total_ms"])
        capture_other = capture_total - backend_snapshot_total
        publish_other = publish_total - remote_layer_total
        counts_match = bool(capture) and len(capture) == len(publish)
        sample_count = len(capture) if counts_match else 0
        estimated_e2e_total = capture_total + publish_total

        nested = {
            stage: summarize_values(stage_values[stage])
            for stage in (
                "backend_snapshot",
                "fc_vm_pause",
                "memory_to_overlaybd",
                "rootfs_snapshot",
                "extra_drives",
                "fc_vm_resume",
                "repository_publish",
                "p2p_publish",
                "manager_publish_total",
            )
            if stage_values[stage]
        }
        results.append(
            {
                "backend": backend,
                "capture": capture_stats,
                "publish": publish_stats,
                "publish_source": publish_source,
                "counts_match": counts_match,
                "snapshot_count": sample_count,
                "estimated_e2e_total_ms": estimated_e2e_total,
                "estimated_e2e_avg_ms": (
                    estimated_e2e_total / sample_count if sample_count else None
                ),
                "remote_layer_total_ms": remote_layer_total,
                "remote_share_of_publish": (
                    remote_layer_total / publish_total if publish_total > 0 else None
                ),
                "breakdown": {
                    "backend_snapshot_ms": backend_snapshot_total,
                    "capture_other_ms": capture_other,
                    "remote_layer_io_ms": remote_layer_total,
                    "publish_other_ms": publish_other,
                },
                "breakdown_is_additive": (
                    counts_match and capture_other >= 0 and publish_other >= 0
                ),
                "nested_stages": nested,
            }
        )
    return results


def format_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}TiB"


def render_summary(summaries: list[dict[str, object]]) -> None:
    print(
        f"{'BACKEND':<10} {'ARTIFACT':<16} {'COUNT':>6} {'BYTES':>12} "
        f"{'AVG':>9} {'P50':>9} {'P95':>9} {'TOTAL':>10} {'MiB/s':>10}"
    )
    print("-" * 101)
    for item in summaries:
        throughput = item["weighted_mib_s"]
        throughput_text = "-" if throughput is None else f"{throughput:.1f}"
        print(
            f"{item['backend']:<10} {item['artifact']:<16} "
            f"{item['count']:>6} {format_bytes(int(item['total_bytes'])):>12} "
            f"{item['avg_ms']:>7.1f}ms {item['p50_ms']:>7.1f}ms "
            f"{item['p95_ms']:>7.1f}ms {item['total_remote_ms']:>8.1f}ms "
            f"{throughput_text:>10}"
        )


def render_end_to_end(results: list[dict[str, object]]) -> None:
    if not results:
        return

    print("\nSnapshot end-to-end approximation:")
    print("  estimated API time = capture_total + publish_total")
    print("  remote_layer_io includes all uploaded rootfs/memory/attached-drive layers")
    print(
        f"{'BACKEND':<10} {'CAP N':>6} {'CAP AVG':>10} {'PUB N':>6} "
        f"{'PUB AVG':>10} {'E2E AVG':>10} {'REMOTE/PUB':>11}"
    )
    print("-" * 80)
    for result in results:
        capture = result["capture"]
        publish = result["publish"]
        e2e_avg = result["estimated_e2e_avg_ms"]
        e2e_text = "-" if e2e_avg is None else f"{e2e_avg:.1f}ms"
        share = result["remote_share_of_publish"]
        share_text = "-" if share is None else f"{share * 100:.1f}%"
        print(
            f"{result['backend']:<10} {capture['count']:>6} "
            f"{capture['avg_ms']:>8.1f}ms {publish['count']:>6} "
            f"{publish['avg_ms']:>8.1f}ms {e2e_text:>10} {share_text:>11}"
        )

    for result in results:
        print(f"\n[{result['backend']}] non-overlapping aggregate breakdown")
        if not result["counts_match"]:
            print(
                "  warning: capture/publish sample counts differ; an additive "
                "per-snapshot average cannot be calculated reliably."
            )
        breakdown = result["breakdown"]
        if not result["breakdown_is_additive"]:
            print(
                "  warning: stage coverage is inconsistent. The log may include "
                "unrelated pull/pause uploads or incomplete/rotated records."
            )
        denominator = result["estimated_e2e_total_ms"]
        for label, key in (
            ("capture/backend_snapshot", "backend_snapshot_ms"),
            ("capture/other", "capture_other_ms"),
            ("publish/remote_layer_io", "remote_layer_io_ms"),
            ("publish/other", "publish_other_ms"),
        ):
            value = breakdown[key]
            share = value / denominator * 100 if denominator > 0 else 0.0
            print(f"  {label:<28} {value:>10.1f} ms  {share:>6.1f}%")
        print(f"  {'estimated_e2e':<28} {denominator:>10.1f} ms  100.0%")

        nested = result["nested_stages"]
        if nested:
            print("  nested stage detail (already included above; do not add again):")
            for stage in (
                "fc_vm_pause",
                "memory_to_overlaybd",
                "rootfs_snapshot",
                "extra_drives",
                "fc_vm_resume",
                "backend_snapshot",
                "repository_publish",
                "p2p_publish",
                "manager_publish_total",
            ):
                stats = nested.get(stage)
                if stats is None:
                    continue
                print(
                    f"    {stage:<24} n={stats['count']:<4} "
                    f"avg={stats['avg_ms']:.1f}ms "
                    f"p50={stats['p50_ms']:.1f}ms "
                    f"p95={stats['p95_ms']:.1f}ms"
                )


def render_records(records: Iterable[UploadRecord]) -> None:
    print("\nPer-upload records:")
    print(
        f"{'BACKEND':<10} {'ARTIFACT':<16} {'SIZE':>12} {'REMOTE':>10} "
        f"{'READ':>9} {'UPLOAD':>9} {'SNAPSHOT':<20} {'LAYER':>6}"
    )
    print("-" * 110)
    for record in records:
        snapshot = record.snapshot_id[:18] or "-"
        layer = "-" if record.layer_index is None else str(record.layer_index)
        print(
            f"{record.backend:<10} {record.artifact:<16} "
            f"{format_bytes(record.size_bytes):>12} {record.remote_ms:>8.1f}ms "
            f"{record.read_ms:>7.1f}ms {record.upload_ms:>7.1f}ms "
            f"{snapshot:<20} {layer:>6}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize snapshot remote uploads and server-side end-to-end stages "
            "from AgentENV logs."
        )
    )
    parser.add_argument("logs", nargs="+", help="AgentENV log files or directories")
    parser.add_argument(
        "--backend",
        choices=("all", "mooncake", "s3"),
        default="all",
        help="only include one backend (default: all)",
    )
    parser.add_argument(
        "--artifact",
        choices=("all", "rootfs", "memory", "attached_drive"),
        default="all",
        help=(
            "filter the upload table; end-to-end remote I/O always includes "
            "all managed-layer groups (default: all)"
        ),
    )
    parser.add_argument(
        "--per-upload", action="store_true", help="also print every matched upload"
    )
    parser.add_argument("--json", action="store_true", help="emit JSON")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        paths = expand_log_paths(args.logs)
    except FileNotFoundError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    all_records, stages_by_backend = parse_logs(paths)
    # Keep the upload table and end-to-end breakdown on the same request
    # population. This excludes template pulls and automatic pause publish
    # activity that may be present in the same server log.
    snapshot_records = [record for record in all_records if record.snapshot_api]
    records = snapshot_records
    if args.backend != "all":
        records = [record for record in records if record.backend == args.backend]
        stages_by_backend = {
            backend: stages
            for backend, stages in stages_by_backend.items()
            if backend == args.backend
        }
    if args.artifact != "all":
        records = [record for record in records if record.artifact == args.artifact]

    summaries = summarize(records)
    e2e_records = snapshot_records
    if args.backend != "all":
        e2e_records = [
            record for record in e2e_records if record.backend == args.backend
        ]
    end_to_end = summarize_end_to_end(e2e_records, stages_by_backend)
    if args.json:
        payload: dict[str, object] = {
            "metric_definition": {
                "mooncake": "read_ms + upload_ms",
                "s3": "oss file uploaded elapsed_ms",
                "estimated_end_to_end": "capture_total + publish_total",
                "scope": "user-facing snapshot create API only",
                "note": "Mooncake asynchronous SSD offload completion is excluded.",
            },
            "summary": summaries,
            "end_to_end": end_to_end,
        }
        if args.per_upload:
            payload["uploads"] = [asdict(record) for record in records]
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("Metric: Mooncake=read_ms+upload_ms; S3=oss upload elapsed_ms")
        print("Scope: user-facing snapshot create API only (pull/pause publish excluded)")
        print("Note: Mooncake asynchronous SSD offload completion is not included.\n")
        if summaries:
            render_summary(summaries)
            render_end_to_end(end_to_end)
            if args.per_upload:
                render_records(records)
        else:
            print("No matching managed-layer upload events found.")
            print("Ensure AgentENV info-level snapshot publish logs are included.")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
