#!/usr/bin/env python3
"""Check per-experiment/per-cycle completeness across RF, rover, and acoustic data."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import importlib.util
import json
import math
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import xarray as xr


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RESULTS_DIR = REPO_ROOT / "results"
DEFAULT_POSITIONS_ROOT = REPO_ROOT / "server" / "record" / "data"
DEFAULT_REPORT_JSON = DEFAULT_RESULTS_DIR / "measurement_completeness_report.json"
DEFAULT_REPORT_MD = DEFAULT_RESULTS_DIR / "measurement_completeness_report.md"
DATASET_DOWNLOAD_SCRIPT = (
    REPO_ROOT / "processing" / "dataset-download" / "download_acoustic_datasets.py"
)


@dataclass(frozen=True)
class AcousticDownloadSummary:
    available_dataset_names: list[str]
    downloaded_dataset_names: list[str]
    skipped_dataset_names: list[str]


@dataclass(frozen=True)
class AcousticInspectionResult:
    cycles_by_experiment: dict[str, set[int]]
    unreadable_dataset_errors: dict[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download acoustic datasets, then check per-experiment/per-cycle presence for "
            "RF, rover position, and acoustic measurements."
        )
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Directory that contains the RF dataset and receives acoustic downloads.",
    )
    parser.add_argument(
        "--positions-root",
        type=Path,
        default=DEFAULT_POSITIONS_ROOT,
        help="Directory that contains exp-<experiment_id>-positions.csv files.",
    )
    parser.add_argument(
        "--rf-dataset",
        type=Path,
        default=None,
        help="RF NetCDF file to inspect. Defaults to the newest results/csi*.nc file.",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=DEFAULT_REPORT_JSON,
        help="JSON report path.",
    )
    parser.add_argument(
        "--report-md",
        type=Path,
        default=DEFAULT_REPORT_MD,
        help="Markdown report path.",
    )
    parser.add_argument(
        "--overwrite-acoustic",
        action="store_true",
        help="Redownload acoustic datasets even if they already exist in results/.",
    )
    parser.add_argument(
        "--acoustic-timeout-seconds",
        type=float,
        default=30.0,
        help="Timeout used when querying/downloading acoustic datasets.",
    )
    parser.add_argument(
        "--skip-acoustic-download",
        action="store_true",
        help="Skip the acoustic download step and only inspect already-present files.",
    )
    return parser.parse_args()


def load_module_from_path(module_path: Path, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module {module_name!r} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def decode_hdf5_scalar(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, np.ndarray):
        if value.shape == ():
            return decode_hdf5_scalar(value[()])
        if value.size == 1:
            return decode_hdf5_scalar(value.reshape(()).item())
    return str(value)


def newest_rf_dataset(results_dir: Path) -> Path:
    candidate_paths = sorted(results_dir.glob("csi*.nc"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not candidate_paths:
        raise FileNotFoundError(f"Could not find any RF dataset matching csi*.nc in {results_dir}")
    return candidate_paths[0].resolve()


def markdown_table(headers: list[str], rows: list[list[object]]) -> str:
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join("---" for _ in headers) + " |"
    body_rows = ["| " + " | ".join(str(value) for value in row) + " |" for row in rows]
    return "\n".join([header_row, separator_row, *body_rows])


def summarize_cycles(cycle_ids: set[int], *, limit: int = 20) -> str:
    if not cycle_ids:
        return "-"
    ordered = sorted(cycle_ids)
    preview = ", ".join(str(cycle_id) for cycle_id in ordered[:limit])
    if len(ordered) <= limit:
        return preview
    return f"{preview}, ... ({len(ordered) - limit} more)"


def download_all_acoustic_datasets(
    *,
    results_dir: Path,
    overwrite: bool,
    timeout_seconds: float,
    skip_download: bool,
) -> AcousticDownloadSummary:
    download_module = load_module_from_path(DATASET_DOWNLOAD_SCRIPT, "download_acoustic_datasets")
    available_dataset_names = download_module.fetch_available_dataset_names(
        download_module.DEFAULT_BASE_URL,
        timeout_seconds,
    )

    downloaded_dataset_names: list[str] = []
    skipped_dataset_names: list[str] = []
    if skip_download:
        return AcousticDownloadSummary(
            available_dataset_names=available_dataset_names,
            downloaded_dataset_names=downloaded_dataset_names,
            skipped_dataset_names=available_dataset_names,
        )

    for dataset_name in available_dataset_names:
        destination, _num_bytes, skipped = download_module.download_dataset(
            dataset_name,
            base_url=download_module.DEFAULT_BASE_URL,
            output_dir=results_dir,
            overwrite=overwrite,
            timeout_seconds=timeout_seconds,
        )
        if skipped:
            skipped_dataset_names.append(destination.name)
        else:
            downloaded_dataset_names.append(destination.name)

    return AcousticDownloadSummary(
        available_dataset_names=available_dataset_names,
        downloaded_dataset_names=downloaded_dataset_names,
        skipped_dataset_names=skipped_dataset_names,
    )


def load_rf_cycles(rf_dataset_path: Path) -> dict[str, set[int]]:
    rf_cycles_by_experiment: dict[str, set[int]] = {}
    with xr.open_dataset(rf_dataset_path) as ds:
        experiment_ids = ds["experiment_id"].values.astype(str)
        cycle_ids = ds["cycle_id"].values.astype(int)
        csi_present = np.any(ds["csi_available"].values > 0, axis=2)
        for experiment_index, experiment_id in enumerate(experiment_ids):
            present_cycle_ids = {
                int(cycle_id)
                for cycle_id, is_present in zip(cycle_ids, csi_present[experiment_index], strict=True)
                if bool(is_present)
            }
            rf_cycles_by_experiment[experiment_id] = present_cycle_ids
    return rf_cycles_by_experiment


def load_rover_cycles(positions_root: Path) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    valid_cycles_by_experiment: dict[str, set[int]] = {}
    invalid_cycles_by_experiment: dict[str, set[int]] = {}
    for csv_path in sorted(positions_root.glob("exp-*-positions.csv")):
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                experiment_id = str(row.get("experiment_id", "")).strip()
                cycle_text = str(row.get("cycle_id", "")).strip()
                if not experiment_id or not cycle_text:
                    continue
                try:
                    cycle_id = int(cycle_text)
                except ValueError:
                    continue

                position_status = str(row.get("position_status", "")).strip().lower()
                try:
                    xyz = [float(row.get(axis, "")) for axis in ("x", "y", "z")]
                    xyz_are_finite = all(math.isfinite(value) for value in xyz)
                except ValueError:
                    xyz_are_finite = False

                is_valid = position_status == "ok" and xyz_are_finite
                target = valid_cycles_by_experiment if is_valid else invalid_cycles_by_experiment
                target.setdefault(experiment_id, set()).add(cycle_id)

    return valid_cycles_by_experiment, invalid_cycles_by_experiment


def try_load_acoustic_cycles_from_file(dataset_path: Path) -> tuple[str, set[int]]:
    with h5py.File(dataset_path, "r") as handle:
        if "experiment_id" not in handle or "cycle_id" not in handle:
            raise KeyError(f"{dataset_path.name} does not contain experiment_id and cycle_id datasets.")
        experiment_values = handle["experiment_id"][()]
        experiment_id = decode_hdf5_scalar(experiment_values[0] if np.ndim(experiment_values) else experiment_values)
        cycle_ids = {int(value) for value in np.asarray(handle["cycle_id"][()]).astype(int).tolist()}
    return experiment_id, cycle_ids


def load_acoustic_cycles(
    results_dir: Path,
    *,
    timeout_seconds: float,
) -> AcousticInspectionResult:
    acoustic_cycles_by_experiment: dict[str, set[int]] = {}
    unreadable_dataset_errors: dict[str, str] = {}
    download_module = load_module_from_path(DATASET_DOWNLOAD_SCRIPT, "download_acoustic_datasets_for_repair")
    for dataset_path in sorted(results_dir.glob("acoustic_*.nc")):
        try:
            experiment_id, cycle_ids = try_load_acoustic_cycles_from_file(dataset_path)
        except (OSError, KeyError, ValueError) as exc:
            try:
                download_module.download_dataset(
                    dataset_path.name,
                    base_url=download_module.DEFAULT_BASE_URL,
                    output_dir=results_dir,
                    overwrite=True,
                    timeout_seconds=timeout_seconds,
                )
                experiment_id, cycle_ids = try_load_acoustic_cycles_from_file(dataset_path)
            except Exception as repair_exc:
                unreadable_dataset_errors[dataset_path.name] = str(repair_exc if repair_exc is not None else exc)
                continue

        acoustic_cycles_by_experiment.setdefault(experiment_id, set()).update(cycle_ids)

    return AcousticInspectionResult(
        cycles_by_experiment=acoustic_cycles_by_experiment,
        unreadable_dataset_errors=unreadable_dataset_errors,
    )


def build_report_payload(
    *,
    rf_dataset_path: Path,
    positions_root: Path,
    acoustic_download_summary: AcousticDownloadSummary,
    rf_cycles_by_experiment: dict[str, set[int]],
    rover_cycles_by_experiment: dict[str, set[int]],
    invalid_rover_cycles_by_experiment: dict[str, set[int]],
    acoustic_inspection: AcousticInspectionResult,
) -> dict[str, Any]:
    acoustic_cycles_by_experiment = acoustic_inspection.cycles_by_experiment
    experiment_ids = sorted(
        set(rf_cycles_by_experiment)
        | set(rover_cycles_by_experiment)
        | set(invalid_rover_cycles_by_experiment)
        | set(acoustic_cycles_by_experiment)
    )

    experiment_reports: list[dict[str, Any]] = []
    total_union_count = 0
    total_complete_count = 0
    for experiment_id in experiment_ids:
        rf_cycles = rf_cycles_by_experiment.get(experiment_id, set())
        rover_cycles = rover_cycles_by_experiment.get(experiment_id, set())
        invalid_rover_cycles = invalid_rover_cycles_by_experiment.get(experiment_id, set())
        acoustic_cycles = acoustic_cycles_by_experiment.get(experiment_id, set())
        union_cycles = rf_cycles | rover_cycles | acoustic_cycles | invalid_rover_cycles
        complete_cycles = rf_cycles & rover_cycles & acoustic_cycles
        missing_rf = union_cycles - rf_cycles
        missing_rover = union_cycles - rover_cycles
        missing_acoustic = union_cycles - acoustic_cycles

        total_union_count += len(union_cycles)
        total_complete_count += len(complete_cycles)

        experiment_reports.append(
            {
                "experiment_id": experiment_id,
                "cycle_union_count": len(union_cycles),
                "complete_cycle_count": len(complete_cycles),
                "rf_cycle_count": len(rf_cycles),
                "rover_cycle_count": len(rover_cycles),
                "invalid_rover_cycle_count": len(invalid_rover_cycles),
                "acoustic_cycle_count": len(acoustic_cycles),
                "missing_rf_cycles": sorted(missing_rf),
                "missing_rover_cycles": sorted(missing_rover),
                "missing_acoustic_cycles": sorted(missing_acoustic),
                "complete_cycles": sorted(complete_cycles),
            }
        )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "rf_dataset_path": str(rf_dataset_path.resolve()),
        "positions_root": str(positions_root.resolve()),
        "acoustic_download": {
            "available_dataset_count": len(acoustic_download_summary.available_dataset_names),
            "available_dataset_names": acoustic_download_summary.available_dataset_names,
            "downloaded_dataset_names": acoustic_download_summary.downloaded_dataset_names,
            "skipped_dataset_names": acoustic_download_summary.skipped_dataset_names,
            "unreadable_dataset_errors": acoustic_inspection.unreadable_dataset_errors,
        },
        "summary": {
            "experiment_count": len(experiment_reports),
            "cycle_union_count": total_union_count,
            "complete_cycle_count": total_complete_count,
            "incomplete_cycle_count": total_union_count - total_complete_count,
        },
        "experiments": experiment_reports,
    }


def write_json_report(report_path: Path, payload: dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_markdown_report(report_path: Path, payload: dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    summary = payload["summary"]
    acoustic_download = payload["acoustic_download"]

    lines: list[str] = [
        "# Measurement Completeness Report",
        "",
        f"- generated at: `{payload['generated_at_utc']}`",
        f"- RF dataset: `{payload['rf_dataset_path']}`",
        f"- positions root: `{payload['positions_root']}`",
        f"- acoustic datasets visible on server: `{acoustic_download['available_dataset_count']}`",
        f"- acoustic datasets downloaded now: `{len(acoustic_download['downloaded_dataset_names'])}`",
        f"- acoustic datasets already present/skipped: `{len(acoustic_download['skipped_dataset_names'])}`",
        f"- unreadable acoustic datasets after repair attempts: `{len(acoustic_download['unreadable_dataset_errors'])}`",
        f"- experiments checked: `{summary['experiment_count']}`",
        f"- unique `(experiment_id, cycle_id)` pairs seen across any modality: `{summary['cycle_union_count']}`",
        f"- complete `(experiment_id, cycle_id)` pairs with RF + rover + acoustics: `{summary['complete_cycle_count']}`",
        f"- incomplete `(experiment_id, cycle_id)` pairs: `{summary['incomplete_cycle_count']}`",
        "",
        "## Per-Experiment Summary",
        "",
    ]

    rows: list[list[object]] = []
    for experiment in payload["experiments"]:
        rows.append(
            [
                experiment["experiment_id"],
                experiment["cycle_union_count"],
                experiment["complete_cycle_count"],
                experiment["rf_cycle_count"],
                experiment["rover_cycle_count"],
                experiment["acoustic_cycle_count"],
                len(experiment["missing_rf_cycles"]),
                len(experiment["missing_rover_cycles"]),
                len(experiment["missing_acoustic_cycles"]),
            ]
        )
    lines.append(
        markdown_table(
            [
                "Experiment",
                "Union Cycles",
                "Complete",
                "RF",
                "Rover",
                "Acoustic",
                "Missing RF",
                "Missing Rover",
                "Missing Acoustic",
            ],
            rows,
        )
    )

    if acoustic_download["unreadable_dataset_errors"]:
        lines.extend(["", "## Unreadable Acoustic Datasets", ""])
        for dataset_name, error_text in sorted(acoustic_download["unreadable_dataset_errors"].items()):
            lines.append(f"- `{dataset_name}`: {error_text}")

    lines.extend(["", "## Missing Cycle Details", ""])
    for experiment in payload["experiments"]:
        lines.extend(
            [
                f"### {experiment['experiment_id']}",
                "",
                f"- complete cycles: `{experiment['complete_cycle_count']}` / `{experiment['cycle_union_count']}`",
                f"- invalid rover rows excluded from rover-present count: `{experiment['invalid_rover_cycle_count']}`",
                f"- missing RF cycles: {summarize_cycles(set(experiment['missing_rf_cycles']))}",
                f"- missing rover cycles: {summarize_cycles(set(experiment['missing_rover_cycles']))}",
                f"- missing acoustic cycles: {summarize_cycles(set(experiment['missing_acoustic_cycles']))}",
                "",
            ]
        )

    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    results_dir = args.results_dir.expanduser().resolve()
    positions_root = args.positions_root.expanduser().resolve()
    rf_dataset_path = (
        args.rf_dataset.expanduser().resolve()
        if args.rf_dataset is not None
        else newest_rf_dataset(results_dir)
    )

    acoustic_download_summary = download_all_acoustic_datasets(
        results_dir=results_dir,
        overwrite=args.overwrite_acoustic,
        timeout_seconds=float(args.acoustic_timeout_seconds),
        skip_download=bool(args.skip_acoustic_download),
    )
    rf_cycles_by_experiment = load_rf_cycles(rf_dataset_path)
    rover_cycles_by_experiment, invalid_rover_cycles_by_experiment = load_rover_cycles(positions_root)
    acoustic_inspection = load_acoustic_cycles(
        results_dir,
        timeout_seconds=float(args.acoustic_timeout_seconds),
    )

    payload = build_report_payload(
        rf_dataset_path=rf_dataset_path,
        positions_root=positions_root,
        acoustic_download_summary=acoustic_download_summary,
        rf_cycles_by_experiment=rf_cycles_by_experiment,
        rover_cycles_by_experiment=rover_cycles_by_experiment,
        invalid_rover_cycles_by_experiment=invalid_rover_cycles_by_experiment,
        acoustic_inspection=acoustic_inspection,
    )

    report_json_path = args.report_json.expanduser().resolve()
    report_md_path = args.report_md.expanduser().resolve()
    write_json_report(report_json_path, payload)
    write_markdown_report(report_md_path, payload)

    print(f"RF dataset: {rf_dataset_path}")
    print(f"Acoustic datasets on server: {len(acoustic_download_summary.available_dataset_names)}")
    print(f"Acoustic datasets downloaded now: {len(acoustic_download_summary.downloaded_dataset_names)}")
    print(f"Acoustic datasets skipped/already present: {len(acoustic_download_summary.skipped_dataset_names)}")
    print(f"Unreadable acoustic datasets after repair attempts: {len(acoustic_inspection.unreadable_dataset_errors)}")
    print(f"JSON report: {report_json_path}")
    print(f"Markdown report: {report_md_path}")


if __name__ == "__main__":
    main()
