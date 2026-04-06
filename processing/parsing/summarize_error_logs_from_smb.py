#!/usr/bin/env python3
"""Summarize host-side JSON-line error logs written to error.log."""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SETTINGS_FILE = REPO_ROOT / "experiment-settings.yaml"
DEFAULT_DATA_ROOT = Path(r"\\10.128.48.9\elliit") if os.name == "nt" else None
DEFAULT_JSON_OUTPUT = None

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read host error.log files from experiment storage and print a summary "
            "grouped by error type, hostname, experiment, and capture type."
        )
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=DEFAULT_SETTINGS_FILE,
        help="Experiment settings file used to resolve the SMB storage root.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help=(
            "Path to the directory that contains the hostname folders. "
            "On Windows this defaults to the UNC network path "
            r"'\\10.128.48.9\elliit'. Overrides experiment_config.storage_path."
        ),
    )
    parser.add_argument(
        "--host",
        action="append",
        default=[],
        help="Only include entries from the given host folder or hostname. Can be repeated.",
    )
    parser.add_argument(
        "--error-type",
        action="append",
        default=[],
        help="Only include entries whose error_type matches one of these values. Can be repeated.",
    )
    parser.add_argument(
        "--tail",
        type=int,
        default=10,
        help="Number of most recent entries to show in the human-readable summary.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=DEFAULT_JSON_OUTPUT,
        help="Optional path to write the aggregated summary as JSON.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print the summary and suppress info logs.",
    )
    args = parser.parse_args()
    if args.tail < 0:
        parser.error("--tail must be >= 0.")
    return args


def configure_logging(quiet: bool) -> None:
    logging.basicConfig(
        level=logging.WARNING if quiet else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def load_yaml_mapping(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}, got {type(data).__name__}.")
    return data


def is_smb_path(storage_path: str) -> bool:
    return storage_path.startswith("\\\\") or storage_path.startswith("//")


def resolve_data_root(args: argparse.Namespace, settings: dict[str, Any]) -> Path:
    if args.data_root is not None:
        data_root = args.data_root.expanduser().resolve()
        if not data_root.exists():
            raise FileNotFoundError(f"Data root does not exist: {data_root}")
        logger.info("Using explicit data root: %s", data_root)
        return data_root

    experiment_config = settings.get("experiment_config") or {}
    storage_path = experiment_config.get("storage_path")
    if not storage_path:
        raise ValueError(
            f"Missing experiment_config.storage_path in {args.config_file.resolve()}"
        )

    if is_smb_path(str(storage_path)):
        smb_candidate = Path(str(storage_path).replace("\\", "/"))
        if smb_candidate.exists():
            logger.info("Using directly accessible SMB path: %s", smb_candidate)
            return smb_candidate

        if os.name == "nt":
            data_root = Path(storage_path)
            if not data_root.exists():
                raise FileNotFoundError(f"SMB path does not exist: {data_root}")
            logger.info("Using Windows SMB path: %s", data_root)
            return data_root

        client_dir = REPO_ROOT / "client"
        import sys

        if str(client_dir) not in sys.path:
            sys.path.insert(0, str(client_dir))
        import runtime_storage

        logger.info("Mounting SMB storage via runtime_storage.prepare_storage_base()")
        data_root = runtime_storage.prepare_storage_base(
            storage_path=str(storage_path),
            settings_path=args.config_file.resolve(),
            experiment_config=experiment_config,
        )
        if not data_root.exists():
            raise FileNotFoundError(f"Resolved SMB mount does not exist: {data_root}")
        logger.info("Using mounted SMB data root: %s", data_root)
        return data_root

    data_root = Path(storage_path).expanduser()
    if not data_root.is_absolute():
        data_root = (args.config_file.resolve().parent / data_root).resolve()
    if not data_root.exists():
        raise FileNotFoundError(f"Data root does not exist: {data_root}")
    logger.info("Using local data root from config: %s", data_root)
    return data_root


def normalize_filters(values: list[str]) -> set[str]:
    return {str(value).strip().upper() for value in values if str(value).strip()}


def scan_error_logs(
    data_root: Path,
    host_filter: set[str],
    error_type_filter: set[str],
) -> tuple[list[dict[str, Any]], list[Path], list[str]]:
    host_dirs = sorted(path for path in data_root.iterdir() if path.is_dir())
    entries: list[dict[str, Any]] = []
    scanned_logs: list[Path] = []
    malformed_lines: list[str] = []

    for host_dir in host_dirs:
        host_name = host_dir.name
        error_log_path = host_dir / "error.log"
        if not error_log_path.exists():
            continue

        if host_filter and host_name.upper() not in host_filter:
            continue

        scanned_logs.append(error_log_path)
        logger.info("Reading %s", error_log_path)
        with error_log_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                stripped = raw_line.strip()
                if not stripped:
                    continue
                try:
                    record = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    malformed_lines.append(f"{error_log_path}:{line_number}: {exc}")
                    continue
                if not isinstance(record, dict):
                    malformed_lines.append(
                        f"{error_log_path}:{line_number}: expected JSON object, got {type(record).__name__}"
                    )
                    continue

                record.setdefault("host_folder", host_name)
                if "hostname" not in record or str(record["hostname"]).strip() == "":
                    record["hostname"] = host_name
                if error_type_filter:
                    error_type = str(record.get("error_type", "")).strip().upper()
                    if error_type not in error_type_filter:
                        continue
                entries.append(record)

    return entries, scanned_logs, malformed_lines


def sort_counter(counter: Counter[str]) -> list[tuple[str, int]]:
    return sorted(counter.items(), key=lambda item: (-item[1], item[0]))


def summarize_entries(
    entries: list[dict[str, Any]],
    scanned_logs: list[Path],
    malformed_lines: list[str],
    tail: int,
) -> dict[str, Any]:
    error_type_counts: Counter[str] = Counter()
    hostname_counts: Counter[str] = Counter()
    experiment_counts: Counter[str] = Counter()
    capture_type_counts: Counter[str] = Counter()
    host_error_type_counts: dict[str, Counter[str]] = defaultdict(Counter)

    earliest_timestamp: str | None = None
    latest_timestamp: str | None = None

    for entry in entries:
        error_type = str(entry.get("error_type", "UNKNOWN")).strip() or "UNKNOWN"
        hostname = str(entry.get("hostname") or entry.get("host_folder") or "UNKNOWN").strip() or "UNKNOWN"
        experiment_id = str(entry.get("experiment_id", "")).strip()
        capture_type = str(entry.get("capture_type", "")).strip()
        timestamp = str(entry.get("timestamp_utc", "")).strip()

        error_type_counts[error_type] += 1
        hostname_counts[hostname] += 1
        host_error_type_counts[hostname][error_type] += 1
        if experiment_id:
            experiment_counts[experiment_id] += 1
        if capture_type:
            capture_type_counts[capture_type] += 1

        if timestamp:
            if earliest_timestamp is None or timestamp < earliest_timestamp:
                earliest_timestamp = timestamp
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp

    def entry_sort_key(entry: dict[str, Any]) -> tuple[str, str, str]:
        timestamp = str(entry.get("timestamp_utc", "")).strip()
        hostname = str(entry.get("hostname") or entry.get("host_folder") or "")
        error_type = str(entry.get("error_type", ""))
        return (timestamp, hostname, error_type)

    recent_entries = sorted(entries, key=entry_sort_key, reverse=True)[:tail] if tail else []

    summary = {
        "scanned_logs": [str(path) for path in scanned_logs],
        "scanned_log_count": len(scanned_logs),
        "entry_count": len(entries),
        "malformed_line_count": len(malformed_lines),
        "malformed_lines": malformed_lines,
        "earliest_timestamp_utc": earliest_timestamp,
        "latest_timestamp_utc": latest_timestamp,
        "error_type_counts": dict(sort_counter(error_type_counts)),
        "hostname_counts": dict(sort_counter(hostname_counts)),
        "experiment_id_counts": dict(sort_counter(experiment_counts)),
        "capture_type_counts": dict(sort_counter(capture_type_counts)),
        "host_error_type_counts": {
            hostname: dict(sort_counter(counter))
            for hostname, counter in sorted(host_error_type_counts.items())
        },
        "recent_entries": [
            {
                "timestamp_utc": entry.get("timestamp_utc"),
                "hostname": entry.get("hostname") or entry.get("host_folder"),
                "experiment_id": entry.get("experiment_id"),
                "cycle_id": entry.get("cycle_id"),
                "capture_type": entry.get("capture_type"),
                "error_type": entry.get("error_type"),
                "message": entry.get("message"),
            }
            for entry in recent_entries
        ],
    }
    return summary


def print_counter_section(title: str, counts: dict[str, int]) -> None:
    print(f"{title}:")
    if not counts:
        print("  <none>")
        return
    for key, count in counts.items():
        print(f"  {key}: {count}")


def print_summary(summary: dict[str, Any], data_root: Path) -> None:
    print(f"Data root: {data_root}")
    print(f"Scanned error logs: {summary['scanned_log_count']}")
    print(f"Parsed entries: {summary['entry_count']}")
    print(f"Malformed lines: {summary['malformed_line_count']}")
    if summary.get("earliest_timestamp_utc") or summary.get("latest_timestamp_utc"):
        print(
            "Time range (UTC): "
            f"{summary.get('earliest_timestamp_utc') or '<unknown>'} -> "
            f"{summary.get('latest_timestamp_utc') or '<unknown>'}"
        )
    print()

    print_counter_section("By Error Type", summary["error_type_counts"])
    print()
    print_counter_section("By Hostname", summary["hostname_counts"])
    print()
    print_counter_section("By Experiment ID", summary["experiment_id_counts"])
    print()
    print_counter_section("By Capture Type", summary["capture_type_counts"])
    print()

    print("Per Host Error Types:")
    host_error_type_counts = summary["host_error_type_counts"]
    if not host_error_type_counts:
        print("  <none>")
    else:
        for hostname, counts in host_error_type_counts.items():
            joined_counts = ", ".join(f"{error_type}={count}" for error_type, count in counts.items())
            print(f"  {hostname}: {joined_counts}")
    print()

    recent_entries = summary["recent_entries"]
    print("Most Recent Entries:")
    if not recent_entries:
        print("  <none>")
    else:
        for entry in recent_entries:
            timestamp = entry.get("timestamp_utc") or "<unknown-time>"
            hostname = entry.get("hostname") or "<unknown-host>"
            experiment_id = entry.get("experiment_id") or "-"
            cycle_id = entry.get("cycle_id") if entry.get("cycle_id") not in ("", None) else "-"
            capture_type = entry.get("capture_type") or "-"
            error_type = entry.get("error_type") or "UNKNOWN"
            message = entry.get("message") or ""
            print(
                f"  {timestamp} host={hostname} exp={experiment_id} cycle={cycle_id} "
                f"capture={capture_type} type={error_type} message={message}"
            )


def write_json_summary(summary: dict[str, Any], output_path: Path) -> Path:
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    return output_path


def main() -> int:
    args = parse_args()
    configure_logging(args.quiet)
    args.config_file = args.config_file.expanduser().resolve()

    logger.info("Loading experiment settings from %s", args.config_file)
    settings = load_yaml_mapping(args.config_file)
    data_root = resolve_data_root(args, settings)

    host_filter = normalize_filters(args.host)
    error_type_filter = normalize_filters(args.error_type)

    entries, scanned_logs, malformed_lines = scan_error_logs(
        data_root,
        host_filter=host_filter,
        error_type_filter=error_type_filter,
    )
    summary = summarize_entries(
        entries,
        scanned_logs=scanned_logs,
        malformed_lines=malformed_lines,
        tail=args.tail,
    )
    print_summary(summary, data_root)

    if args.json_output is not None:
        output_path = write_json_summary(summary, args.json_output)
        print()
        print(f"Wrote JSON summary to {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
