#!/usr/bin/env python3
"""Compare snapshot managed-layer upload time for Mooncake and OSS/S3.

This script reads AgentENV server logs directly. It intentionally does not
consume replay-aenv results, because those results measure end-to-end latency.

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
        )

    return None


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


def parse_logs(paths: Iterable[Path]) -> list[UploadRecord]:
    records: list[UploadRecord] = []
    for path in paths:
        try:
            handle = open_log(path)
            with handle:
                for line_number, line in enumerate(handle, 1):
                    record = parse_upload_line(line, str(path), line_number)
                    if record is not None:
                        records.append(record)
        except (OSError, UnicodeError) as error:
            print(f"warning: skip {path}: {error}", file=sys.stderr)
    return records


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
        description="Summarize snapshot managed-layer remote upload time from AgentENV logs."
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
        help="only include one managed-layer group (default: all)",
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

    records = parse_logs(paths)
    if args.backend != "all":
        records = [record for record in records if record.backend == args.backend]
    if args.artifact != "all":
        records = [record for record in records if record.artifact == args.artifact]

    summaries = summarize(records)
    if args.json:
        payload: dict[str, object] = {
            "metric_definition": {
                "mooncake": "read_ms + upload_ms",
                "s3": "oss file uploaded elapsed_ms",
                "note": "Mooncake asynchronous SSD offload completion is excluded.",
            },
            "summary": summaries,
        }
        if args.per_upload:
            payload["uploads"] = [asdict(record) for record in records]
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print("Metric: Mooncake=read_ms+upload_ms; S3=oss upload elapsed_ms")
        print("Note: Mooncake asynchronous SSD offload completion is not included.\n")
        if summaries:
            render_summary(summaries)
            if args.per_upload:
                render_records(records)
        else:
            print("No matching managed-layer upload events found.")
            print("Ensure AgentENV info-level snapshot publish logs are included.")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
