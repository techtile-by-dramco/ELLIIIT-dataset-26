#!/usr/bin/env python3
"""Build an xarray dataset from host JSON result files and rover positions."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import xarray as xr
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
CLIENT_DIR = REPO_ROOT / "client"
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

import runtime_storage
import tools


logger = logging.getLogger(__name__)


DEFAULT_SETTINGS_FILE = REPO_ROOT / "experiment-settings.yaml"
DEFAULT_CABLE_FILE = REPO_ROOT / "client" / "ref-RF-cable.yml"
DEFAULT_POSITIONS_ROOT = REPO_ROOT / "server" / "record" / "data"
DEFAULT_ROVER_CONFIG = REPO_ROOT / "client" / "rover" / "config.yaml"
DEFAULT_RESULTS_DIR = REPO_ROOT / "results"
DEFAULT_OUTPUT_FILE = DEFAULT_RESULTS_DIR / "csi.nc"
DEFAULT_DATA_ROOT = Path(r"\\10.128.48.9\elliit") if os.name == "nt" else None
DEFAULT_WORKERS = min(16, max(4, os.cpu_count() or 1))
EXPERIMENTS = ["EXP003", "EXP005", "EXP006", "EXP007", "EXP008"]  # Set to [] or None to include all experiments by default.
RESULT_FILE_SUFFIXES = {".json", ".jsonl", ".txt", ".log"}
FLOAT_PATTERN = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
INTEGER_PATTERN = r"[-+]?\d+"
POSITION_FILE_PATTERN = re.compile(r"^exp-(?P<experiment_id>.+)-positions\.csv$")
DEFAULT_SOURCE_FORMAT = "result_file"

PHASE_PRIMARY_ALIASES = (
    "pilot_phase",
    "pilot_phase_rad",
    "phi_rp",
    "phi_rp_rad",
    "pilot_phi",
    "pilot_phi_rad",
)
PHASE_FALLBACK_ALIASES = ("phase", "phi")
AMPLITUDE_PRIMARY_ALIASES = (
    "pilot_amplitude",
    "pilot_amplitude_rms",
    "pilot_amp",
    "pilot_ampl",
)
AMPLITUDE_FALLBACK_ALIASES = ("amplitude", "ampl", "amp")
HOSTNAME_ALIASES = ("hostname", "host", "tile")
FILE_NAME_ALIASES = ("file_name", "filename", "file")
EXPERIMENT_ID_ALIASES = ("experiment_id", "exp_id", "experiment")
CYCLE_ID_ALIASES = ("cycle_id", "meas_id", "measurement_id", "measurement", "cycle")

TEXT_PATTERNS = {
    "pilot_phase": [
        re.compile(
            rf"(?:phase pilot reference signal|pilot[_ ]phase|phi[_ ]?rp)\s*[:=]\s*({FLOAT_PATTERN})",
            re.IGNORECASE,
        ),
    ],
    "pilot_amplitude": [
        re.compile(
            rf"(?:pilot[_ ]amplitude|pilot[_ ]amp(?:l)?)\s*[:=]\s*({FLOAT_PATTERN})",
            re.IGNORECASE,
        ),
    ],
    "cycle_id": [
        re.compile(r"(?:cycle[_ ]id|meas(?:urement)?[_ ]id)\s*[:=]\s*([^\s,;]+)", re.IGNORECASE),
    ],
    "experiment_id": [
        re.compile(r"(?:experiment[_ ]id|exp[_ ]id)\s*[:=]\s*([^\s,;]+)", re.IGNORECASE),
    ],
    "file_name": [
        re.compile(r"(?:file[_ ]name|filename)\s*[:=]\s*([^\s,;]+)", re.IGNORECASE),
    ],
    "hostname": [
        re.compile(r"(?:hostname|host|tile)\s*[:=]\s*([^\s,;]+)", re.IGNORECASE),
    ],
}
KEY_VALUE_PATTERN = re.compile(r"^\s*([A-Za-z0-9_. -]+?)\s*[:=]\s*(.*?)\s*$")


def parse_args() -> argparse.Namespace:
    default_experiment_ids = normalize_experiment_ids(EXPERIMENTS)
    parser = argparse.ArgumentParser(
        description=(
            "Read host JSON/JSONL result files from experiment storage, join them "
            "with server/record/data position logs, and write one xarray NetCDF dataset."
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
        "--positions-root",
        type=Path,
        default=DEFAULT_POSITIONS_ROOT,
        help="Directory that contains exp-<experiment_id>-positions.csv files.",
    )
    parser.add_argument(
        "--cable-file",
        type=Path,
        default=DEFAULT_CABLE_FILE,
        help="YAML file that maps hostname to phi_cable in degrees.",
    )
    parser.add_argument(
        "--rover-config",
        type=Path,
        default=DEFAULT_ROVER_CONFIG,
        help=(
            "Rover config used to read grid.min_spacing for duplicate-position filtering. "
            "Consecutive positions within min_spacing/5 per axis are removed."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help="NetCDF file written with the joined xarray dataset.",
    )
    parser.add_argument(
        "--max-measurements",
        type=int,
        default=None,
        help=(
            "Only keep the first N extracted measurements per hostname folder, "
            "sorted by parsed experiment and cycle ID."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of worker threads used for per-file CSI extraction.",
    )
    parser.add_argument(
        "--experiment-id",
        dest="experiment_ids",
        action="append",
        default=None,
        metavar="EXP_ID",
        help=(
            "Only include the given experiment ID in the output dataset. "
            "Repeat this option or pass comma-separated values to combine multiple "
            "experiment IDs into one NetCDF file. If omitted, the top-level "
            f"EXPERIMENTS variable is used: {default_experiment_ids or 'all experiments'}."
        ),
    )
    args = parser.parse_args()
    if args.max_measurements is not None and args.max_measurements <= 0:
        parser.error("--max-measurements must be a positive integer.")
    if args.workers <= 0:
        parser.error("--workers must be a positive integer.")
    args.experiment_ids = normalize_experiment_ids(args.experiment_ids, parser)
    if args.experiment_ids is None:
        args.experiment_ids = list(default_experiment_ids) if default_experiment_ids else None
    return args


def normalize_experiment_ids(
    raw_values: Iterable[str] | None,
    parser: argparse.ArgumentParser | None = None,
) -> list[str] | None:
    if not raw_values:
        return None

    experiment_ids: list[str] = []
    for raw_value in raw_values:
        for experiment_id in str(raw_value).split(","):
            experiment_id = experiment_id.strip()
            if experiment_id:
                experiment_ids.append(experiment_id)

    experiment_ids = list(dict.fromkeys(experiment_ids))
    if not experiment_ids and parser is not None:
        parser.error("--experiment-id must include at least one non-empty value.")
    return experiment_ids or None


def load_yaml_mapping(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}, got {type(data).__name__}.")
    return data


def load_duplicate_position_filter_settings(rover_config_path: Path) -> dict[str, Any]:
    rover_config = load_yaml_mapping(rover_config_path)
    grid = rover_config.get("grid")
    if not isinstance(grid, dict):
        raise ValueError(f"Missing grid block in rover config: {rover_config_path}")

    min_spacing_raw = grid.get("min_spacing")
    if min_spacing_raw in ("", None):
        raise ValueError(f"Missing grid.min_spacing in rover config: {rover_config_path}")

    min_spacing_mm = float(min_spacing_raw)
    if min_spacing_mm <= 0:
        raise ValueError(f"grid.min_spacing must be positive in {rover_config_path}")

    axis_tolerance_m = (min_spacing_mm / 5.0) / 1000.0
    return {
        "rover_config_path": str(rover_config_path),
        "min_spacing_mm": min_spacing_mm,
        "axis_tolerance_m": axis_tolerance_m,
    }


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def warn(message: str) -> None:
    logger.warning(message)


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
        logger.info("Resolving SMB storage path from config: %s", storage_path)
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


def load_cable_phases(cable_file: Path) -> dict[str, float]:
    logger.info("Reading cable phases from %s", cable_file)
    cable_data = load_yaml_mapping(cable_file)
    cable_phases = {
        str(hostname).strip().upper(): float(value)
        for hostname, value in cable_data.items()
    }
    logger.info("Loaded cable phase entries for %d hostnames", len(cable_phases))
    return cable_phases


def scalarize(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        if value.shape == ():
            value = value.item()
        else:
            return value.tolist()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def normalize_key(key: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(key).strip().lower()).strip("_")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def parse_float(value: Any) -> float:
    value = scalarize(value)
    if isinstance(value, (int, float)):
        return float(value)

    match = re.search(FLOAT_PATTERN, str(value))
    if match is None:
        raise ValueError(f"Unable to parse float from {value!r}")
    return float(match.group(0))


def parse_cycle_id(value: Any) -> int:
    value = scalarize(value)
    if isinstance(value, (int, np.integer)):
        return int(value)

    match = re.search(INTEGER_PATTERN, str(value))
    if match is None:
        raise ValueError(f"Unable to parse cycle_id from {value!r}")
    return int(match.group(0))


def parse_optional_float(value: Any) -> float:
    if value in ("", None):
        return np.nan
    return parse_float(value)


def pick_value(
    mapping: dict[str, Any],
    primary_aliases: Iterable[str],
    fallback_aliases: Iterable[str] = (),
) -> Any | None:
    for alias in primary_aliases:
        if alias in mapping and mapping[alias] not in ("", None):
            return mapping[alias]
    for alias in fallback_aliases:
        if alias in mapping and mapping[alias] not in ("", None):
            return mapping[alias]
    return None


def parse_experiment_id_from_result_file(path: Path, host_candidates: Iterable[str]) -> str | None:
    stem = path.stem
    for host in host_candidates:
        host = str(host).strip()
        if not host:
            continue
        pattern = re.compile(
            rf"^data_{re.escape(host)}_(?P<experiment_id>.+?)_(?P<cycle_id>{INTEGER_PATTERN})(?:_.+)?$"
        )
        match = pattern.match(stem)
        if match is not None:
            experiment_id = match.group("experiment_id").strip("_")
            if experiment_id:
                return experiment_id
    return None


def parse_ids_from_file_name(file_name: str, host_candidates: Iterable[str]) -> tuple[str | None, int | None]:
    file_name = str(file_name).strip()
    if not file_name:
        return None, None

    file_stem = Path(file_name).stem
    for host in host_candidates:
        host = str(host).strip()
        if not host:
            continue
        pattern = re.compile(
            rf"^data_{re.escape(host)}_(?P<experiment_id>.+?)_(?P<cycle_id>{INTEGER_PATTERN})(?:_.+)?$"
        )
        match = pattern.match(file_stem)
        if match is not None:
            return match.group("experiment_id"), parse_cycle_id(match.group("cycle_id"))

    return None, None


def is_matching_result_file(path: Path, host_folder: str) -> bool:
    if path.suffix.lower() not in RESULT_FILE_SUFFIXES:
        return False
    host = str(host_folder).strip()
    if not host:
        return False
    return path.stem.startswith(f"data_{host}_")


def measurement_file_sort_key(path: Path, host_folder: str) -> tuple[str, int, str]:
    experiment_id, cycle_id = parse_ids_from_file_name(path.name, [host_folder])
    if cycle_id is None:
        cycle_id = sys.maxsize
    return (experiment_id or "", cycle_id, path.name)


def sanitize_filename_component(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value).strip())
    sanitized = sanitized.strip("-_.")
    if not sanitized:
        return "unknown"
    return sanitized


def build_output_path(output_file: Path, experiment_ids: Iterable[str]) -> Path:
    labels = [
        sanitize_filename_component(experiment_id)
        for experiment_id in experiment_ids
        if str(experiment_id).strip()
    ]
    labels = sorted(set(labels))
    if not labels:
        return output_file

    experiment_label = "__".join(labels)
    if output_file.stem.endswith(f"_{experiment_label}"):
        return output_file
    return output_file.with_name(
        f"{output_file.stem}_{experiment_label}{output_file.suffix}"
    )


def canonicalize_record(
    raw_mapping: dict[str, Any],
    host_folder: str,
    source_format: str,
    source_path: Path,
) -> dict[str, Any] | None:
    normalized = {
        normalize_key(key): scalarize(value)
        for key, value in raw_mapping.items()
    }

    phase_value = pick_value(normalized, PHASE_PRIMARY_ALIASES, PHASE_FALLBACK_ALIASES)
    amplitude_value = pick_value(normalized, AMPLITUDE_PRIMARY_ALIASES, AMPLITUDE_FALLBACK_ALIASES)
    if phase_value is None or amplitude_value is None:
        return None

    hostname_value = pick_value(normalized, HOSTNAME_ALIASES)
    file_name_value = pick_value(normalized, FILE_NAME_ALIASES)

    hostname = str(hostname_value).strip() if hostname_value not in ("", None) else host_folder
    host_candidates = (hostname, host_folder)

    experiment_id_value = pick_value(normalized, EXPERIMENT_ID_ALIASES)
    cycle_id_value = pick_value(normalized, CYCLE_ID_ALIASES)

    parsed_experiment_id = None
    parsed_cycle_id = None
    if file_name_value not in ("", None):
        parsed_experiment_id, parsed_cycle_id = parse_ids_from_file_name(
            str(file_name_value),
            host_candidates,
        )

    if experiment_id_value in ("", None):
        experiment_id_value = parsed_experiment_id
    if experiment_id_value in ("", None):
        experiment_id_value = parse_experiment_id_from_result_file(source_path, host_candidates)
    if experiment_id_value in ("", None):
        return None

    if cycle_id_value in ("", None):
        cycle_id_value = parsed_cycle_id
    if cycle_id_value in ("", None):
        return None

    return {
        "host_folder": host_folder,
        "hostname": hostname,
        "file_name": str(file_name_value).strip() if file_name_value not in ("", None) else "",
        "experiment_id": str(experiment_id_value).strip(),
        "cycle_id": parse_cycle_id(cycle_id_value),
        "pilot_phase": parse_float(phase_value),
        "pilot_amplitude": parse_float(amplitude_value),
        "source_format": source_format,
    }


def iter_nested_mappings(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for nested_value in value.values():
            yield from iter_nested_mappings(nested_value)
    elif isinstance(value, list):
        for item in value:
            yield from iter_nested_mappings(item)


def deduplicate_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for record in records:
        signature = (
            record["experiment_id"],
            record["cycle_id"],
            record["hostname"],
            record["pilot_phase"],
            record["pilot_amplitude"],
            record["source_format"],
        )
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(record)
    return deduped


def extract_records_from_structured_value(
    value: Any,
    host_folder: str,
    source_format: str,
    source_path: Path,
) -> list[dict[str, Any]]:
    records = []
    for mapping in iter_nested_mappings(value):
        record = canonicalize_record(mapping, host_folder, source_format, source_path)
        if record is not None:
            records.append(record)
    return deduplicate_records(records)


def extract_records_from_json_file(path: Path, host_folder: str) -> list[dict[str, Any]]:
    data = json.loads(read_text(path))
    return extract_records_from_structured_value(data, host_folder, "json", path)


def extract_record_from_pilot_iq(
    pilot_iq: Any,
    values: dict[str, Any],
    host_folder: str,
    source_path: Path,
) -> dict[str, Any] | None:
    iq_samples = np.asarray(pilot_iq)
    if iq_samples.ndim != 2 or iq_samples.shape[0] < 2 or iq_samples.shape[1] == 0:
        return None

    phase_ch0, _, _ = tools.get_phases_and_apply_bandpass(iq_samples[0, :])
    phase_ch1, _, _ = tools.get_phases_and_apply_bandpass(iq_samples[1, :])
    phase_diff = tools.to_min_pi_plus_pi(phase_ch0 - phase_ch1, deg=False)
    pilot_phase = float(tools.circmean(phase_diff, deg=False))
    pilot_amplitude = float(np.sqrt(np.mean(np.abs(iq_samples[1, :]) ** 2)))

    raw_record: dict[str, Any] = {
        "hostname": scalarize(values.get("hostname", host_folder)),
        "file_name": scalarize(values.get("file_name", source_path.stem)),
        "pilot_phase": pilot_phase,
        "pilot_amplitude": pilot_amplitude,
    }
    for key in ("experiment_id", "cycle_id", "meas_id"):
        if key in values:
            raw_record[key] = scalarize(values[key])

    return canonicalize_record(raw_record, host_folder, "npz_iq", source_path)


def extract_records_from_npz_file(path: Path, host_folder: str) -> list[dict[str, Any]]:
    values: dict[str, Any] = {}
    with np.load(path, allow_pickle=True) as archive:
        for key in archive.files:
            values[key] = scalarize(archive[key])

    records = extract_records_from_structured_value(values, host_folder, "npz", path)
    if records:
        return records

    for iq_key in ("pilot_iq", "iq_capture"):
        if iq_key in values:
            record = extract_record_from_pilot_iq(values[iq_key], values, host_folder, path)
            if record is not None:
                return [record]

    return []


def extract_records_from_yaml_file(path: Path, host_folder: str) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        documents = list(yaml.safe_load_all(handle))
    records: list[dict[str, Any]] = []
    for document in documents:
        records.extend(extract_records_from_structured_value(document, host_folder, "yaml", path))
    return deduplicate_records(records)


def extract_records_from_csv_file(path: Path, host_folder: str) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        records = []
        for row in reader:
            record = canonicalize_record(dict(row), host_folder, "csv", path)
            if record is not None:
                records.append(record)
    return deduplicate_records(records)


def parse_text_line(line: str) -> dict[str, str]:
    updates: dict[str, str] = {}

    for field_name, patterns in TEXT_PATTERNS.items():
        for pattern in patterns:
            match = pattern.search(line)
            if match is not None:
                updates[field_name] = match.group(1).strip()
                break

    key_value_match = KEY_VALUE_PATTERN.match(line)
    if key_value_match is None:
        return updates

    key = normalize_key(key_value_match.group(1))
    value = key_value_match.group(2).strip()
    if not value:
        return updates

    key_aliases = {
        "pilot_phase": PHASE_PRIMARY_ALIASES + PHASE_FALLBACK_ALIASES,
        "pilot_amplitude": AMPLITUDE_PRIMARY_ALIASES + AMPLITUDE_FALLBACK_ALIASES,
        "hostname": HOSTNAME_ALIASES,
        "file_name": FILE_NAME_ALIASES,
        "experiment_id": EXPERIMENT_ID_ALIASES,
        "cycle_id": CYCLE_ID_ALIASES,
    }
    for field_name, aliases in key_aliases.items():
        if key in aliases and field_name not in updates:
            updates[field_name] = value
            break

    return updates


def record_has_measurement_fields(record: dict[str, Any]) -> bool:
    return "pilot_phase" in record and "pilot_amplitude" in record


def starts_new_record(current: dict[str, Any], updates: dict[str, Any]) -> bool:
    if not record_has_measurement_fields(current):
        return False
    if "pilot_phase" in updates or "pilot_amplitude" in updates:
        return True
    if "cycle_id" in updates and updates["cycle_id"] != current.get("cycle_id"):
        return True
    if "file_name" in updates and updates["file_name"] != current.get("file_name"):
        return True
    return False


def finalize_text_record(
    current: dict[str, Any],
    host_folder: str,
    source_path: Path,
) -> dict[str, Any] | None:
    if not record_has_measurement_fields(current):
        return None
    return canonicalize_record(current, host_folder, "text", source_path)


def extract_records_from_json_lines(text: str, host_folder: str, source_path: Path) -> list[dict[str, Any]]:
    records = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("{") or not stripped.endswith("}"):
            continue
        try:
            record_value = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        records.extend(
            extract_records_from_structured_value(record_value, host_folder, "jsonl", source_path)
        )
    return deduplicate_records(records)


def extract_records_from_text_file(path: Path, host_folder: str) -> list[dict[str, Any]]:
    text = read_text(path)
    records = extract_records_from_json_lines(text, host_folder, path)

    current: dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            record = finalize_text_record(current, host_folder, path)
            if record is not None:
                records.append(record)
            current = {}
            continue

        updates = parse_text_line(line)
        if not updates:
            continue

        if starts_new_record(current, updates):
            record = finalize_text_record(current, host_folder, path)
            if record is not None:
                records.append(record)
            current = {}

        current.update(updates)

    record = finalize_text_record(current, host_folder, path)
    if record is not None:
        records.append(record)

    return deduplicate_records(records)


def extract_records_from_file(path: Path, host_folder: str) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".npz":
        return extract_records_from_npz_file(path, host_folder)
    if suffix == ".json":
        return extract_records_from_json_file(path, host_folder)
    if suffix in {".yaml", ".yml"}:
        return extract_records_from_yaml_file(path, host_folder)
    if suffix == ".csv":
        return extract_records_from_csv_file(path, host_folder)
    if suffix in {".jsonl", ".txt", ".log"}:
        return extract_records_from_text_file(path, host_folder)
    return []


def find_cable_phase(
    cable_phases: dict[str, float],
    host_folder: str,
    hostname: str,
) -> float | None:
    for candidate in (hostname, host_folder):
        if not candidate:
            continue
        cable_phase = cable_phases.get(candidate.strip().upper())
        if cable_phase is not None:
            return cable_phase
    return None


def build_csi_row(
    record: dict[str, Any],
    phi_cable_deg: float,
    measurement_timestamp: str,
    measurement_source_path: Path,
) -> dict[str, Any]:
    pilot_phase = float(record["pilot_phase"])
    pilot_amplitude = float(record["pilot_amplitude"])
    phi_cable_rad = float(np.deg2rad(phi_cable_deg))
    csi_phase_raw = float(pilot_phase - phi_cable_rad)
    csi = complex(pilot_amplitude * np.exp(1j * csi_phase_raw))

    return {
        "experiment_id": record["experiment_id"],
        "cycle_id": record["cycle_id"],
        "hostname": record["hostname"],
        "csi_real": float(np.real(csi)),
        "csi_imag": float(np.imag(csi)),
        "measurement_timestamp": measurement_timestamp,
        "measurement_source_path": str(measurement_source_path),
    }


def extract_csi_rows_from_file(
    result_file: Path,
    host_folder: str,
    cable_phases: dict[str, float],
) -> list[dict[str, Any]]:
    logger.info("Reading result file %s", result_file)
    measurement_timestamp = datetime.fromtimestamp(
        result_file.stat().st_mtime
    ).astimezone().isoformat(timespec="seconds")
    records = extract_records_from_file(result_file, host_folder)
    if not records:
        logger.info("No usable records found in %s", result_file)
        return []

    rows: list[dict[str, Any]] = []
    for record in records:
        phi_cable_deg = find_cable_phase(
            cable_phases,
            record["host_folder"],
            record["hostname"],
        )
        if phi_cable_deg is None:
            raise KeyError(
                f"No cable phase found for hostname '{record['hostname']}' "
                f"(folder '{record['host_folder']}')."
            )
        rows.append(
            build_csi_row(
                record,
                phi_cable_deg,
                measurement_timestamp,
                result_file,
            )
        )
    return rows


def collect_csi_rows(
    data_root: Path,
    cable_phases: dict[str, float],
    max_measurements: int | None = None,
    workers: int = DEFAULT_WORKERS,
    experiment_ids: Iterable[str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    skipped_files = 0
    selected_experiment_ids = set(str(experiment_id) for experiment_id in (experiment_ids or []))
    host_dirs = sorted(path for path in data_root.iterdir() if path.is_dir())
    logger.info("Found %d hostname folders under %s", len(host_dirs), data_root)
    work_items: list[tuple[str, Path]] = []

    for host_dir in host_dirs:
        candidate_files = sorted(
            (
                path
                for path in host_dir.iterdir()
                if path.is_file()
                and is_matching_result_file(path, host_dir.name)
            ),
            key=lambda path: measurement_file_sort_key(path, host_dir.name),
        )
        if not candidate_files:
            logger.info(
                "Skipping host folder %s: no matching data_<HOST>_*.json/.jsonl/.txt/.log files",
                host_dir.name,
            )
            continue

        logger.info(
            "Scanning host folder %s with %d result files",
            host_dir.name,
            len(candidate_files),
        )
        if selected_experiment_ids:
            filtered_candidate_files: list[Path] = []
            for result_file in candidate_files:
                parsed_experiment_id = parse_experiment_id_from_result_file(result_file, [host_dir.name])
                if (
                    parsed_experiment_id is not None
                    and parsed_experiment_id not in selected_experiment_ids
                ):
                    continue
                filtered_candidate_files.append(result_file)
            candidate_files = filtered_candidate_files
            logger.info(
                "Host folder %s: %d result files remain after experiment filter",
                host_dir.name,
                len(candidate_files),
            )
            if not candidate_files:
                continue
        for result_file in candidate_files:
            work_items.append((host_dir.name, result_file))

    logger.info(
        "Processing %d measurement files using %d worker thread(s)",
        len(work_items),
        workers,
    )
    if workers == 1 or len(work_items) <= 1:
        for host_folder, result_file in work_items:
            try:
                rows.extend(extract_csi_rows_from_file(result_file, host_folder, cable_phases))
            except Exception as exc:
                skipped_files += 1
                warn(f"Skipping {result_file}: {exc}")
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_item = {
                executor.submit(
                    extract_csi_rows_from_file,
                    result_file,
                    host_folder,
                    cable_phases,
                ): (host_folder, result_file)
                for host_folder, result_file in work_items
            }
            for future in as_completed(future_to_item):
                _, result_file = future_to_item[future]
                try:
                    rows.extend(future.result())
                except Exception as exc:
                    skipped_files += 1
                    warn(f"Skipping {result_file}: {exc}")

    deduped: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in rows:
        deduped[(row["experiment_id"], row["cycle_id"], row["hostname"])] = row
    deduped_rows = sorted(
        deduped.values(),
        key=lambda row: (str(row["experiment_id"]), int(row["cycle_id"]), str(row["hostname"])),
    )
    if selected_experiment_ids:
        filtered_rows = [
            row for row in deduped_rows if str(row["experiment_id"]) in selected_experiment_ids
        ]
        logger.info(
            "Applied experiment filter: kept %d of %d unique CSI rows for %s",
            len(filtered_rows),
            len(deduped_rows),
            ", ".join(sorted(selected_experiment_ids)),
        )
        deduped_rows = filtered_rows
    if max_measurements is not None:
        limited_rows: list[dict[str, Any]] = []
        host_counts: dict[str, int] = {}
        for row in deduped_rows:
            host_folder = str(row["hostname"])
            host_count = host_counts.get(host_folder, 0)
            if host_count >= max_measurements:
                continue
            limited_rows.append(row)
            host_counts[host_folder] = host_count + 1
        logger.info(
            "Applied max-measurements limit: kept %d rows across %d hostnames",
            len(limited_rows),
            len(host_counts),
        )
        deduped_rows = limited_rows
    logger.info(
        "Collected %d CSI rows (%d unique experiment/cycle/hostname combinations)",
        len(rows),
        len(deduped_rows),
    )
    return deduped_rows, skipped_files


def load_positions(
    positions_root: Path,
    experiment_ids: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    positions_root = positions_root.expanduser().resolve()
    if not positions_root.exists():
        raise FileNotFoundError(f"Positions directory does not exist: {positions_root}")

    position_rows: dict[tuple[str, int], dict[str, Any]] = {}
    selected_experiment_ids = set(str(experiment_id) for experiment_id in (experiment_ids or []))
    position_files = sorted(positions_root.glob("exp-*-positions.csv"))
    logger.info(
        "Reading positions from %s (%d matching files)",
        positions_root,
        len(position_files),
    )
    for path in position_files:
        logger.info("Reading positions file %s", path)
        match = POSITION_FILE_PATTERN.match(path.name)
        if match is None:
            continue

        fallback_experiment_id = match.group("experiment_id")
        if selected_experiment_ids and fallback_experiment_id not in selected_experiment_ids:
            logger.info(
                "Skipping positions file %s: experiment_id %s not selected",
                path,
                fallback_experiment_id,
            )
            continue
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                experiment_id = str(row.get("experiment_id") or fallback_experiment_id).strip()
                cycle_raw = row.get("cycle_id")
                if not experiment_id or cycle_raw in ("", None):
                    continue
                if selected_experiment_ids and experiment_id not in selected_experiment_ids:
                    continue

                try:
                    cycle_id = parse_cycle_id(cycle_raw)
                except ValueError:
                    warn(f"Skipping position row with invalid cycle_id in {path}: {cycle_raw!r}")
                    continue

                position_rows[(experiment_id, cycle_id)] = {
                    "experiment_id": experiment_id,
                    "cycle_id": cycle_id,
                    "x": parse_optional_float(row.get("x")),
                    "y": parse_optional_float(row.get("y")),
                    "z": parse_optional_float(row.get("z")),
                    "position_available": 1.0 if str(row.get("position_status", "")).strip().lower() == "ok" else 0.0,
                }

    rows = list(position_rows.values())
    logger.info("Loaded %d unique position rows", len(rows))
    return rows


def summarize_position_coverage(
    csi_rows: list[dict[str, Any]],
    position_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    relevant_pairs = {
        (str(row["experiment_id"]), int(row["cycle_id"]))
        for row in csi_rows
    }
    position_row_map = {
        (str(row["experiment_id"]), int(row["cycle_id"])): row
        for row in position_rows
    }
    relevant_position_rows = [
        position_row_map[pair]
        for pair in sorted(relevant_pairs)
        if pair in position_row_map
    ]
    missing_position_rows = len(relevant_pairs) - len(relevant_position_rows)

    invalid_status_rows = 0
    missing_coordinate_rows = 0
    invalid_or_missing_rows = 0
    for row in relevant_position_rows:
        has_valid_status = bool(row["position_available"] > 0)
        has_coordinates = bool(
            np.isfinite([row["x"], row["y"], row["z"]]).all()
        )
        if not has_valid_status:
            invalid_status_rows += 1
        if not has_coordinates:
            missing_coordinate_rows += 1
        if not (has_valid_status and has_coordinates):
            invalid_or_missing_rows += 1

    valid_position_rows = len(relevant_position_rows) - invalid_or_missing_rows
    coverage = {
        "csi_pair_count": len(relevant_pairs),
        "matched_position_rows": len(relevant_position_rows),
        "missing_position_rows": missing_position_rows,
        "valid_position_rows": valid_position_rows,
        "invalid_status_rows": invalid_status_rows,
        "missing_coordinate_rows": missing_coordinate_rows,
        "invalid_or_missing_position_rows": (
            missing_position_rows + invalid_or_missing_rows
        ),
    }
    return relevant_position_rows, coverage


def filter_duplicate_position_cycles(
    csi_rows: list[dict[str, Any]],
    position_rows: list[dict[str, Any]],
    *,
    axis_tolerance_m: float,
    min_spacing_mm: float,
    rover_config_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    dropped_pairs: set[tuple[str, int]] = set()
    filtered_position_rows: list[dict[str, Any]] = []
    last_kept_coords_by_experiment: dict[str, np.ndarray] = {}

    for row in sorted(position_rows, key=lambda item: (str(item["experiment_id"]), int(item["cycle_id"]))):
        experiment_id = str(row["experiment_id"])
        pair = (experiment_id, int(row["cycle_id"]))
        coords = np.asarray([row["x"], row["y"], row["z"]], dtype=float)

        previous_coords = last_kept_coords_by_experiment.get(experiment_id)
        if previous_coords is not None and np.isfinite(coords).all():
            if bool(np.all(np.abs(coords - previous_coords) <= axis_tolerance_m)):
                dropped_pairs.add(pair)
                continue

        filtered_position_rows.append(row)
        if np.isfinite(coords).all():
            last_kept_coords_by_experiment[experiment_id] = coords

    filtered_csi_rows = [
        row
        for row in csi_rows
        if (str(row["experiment_id"]), int(row["cycle_id"])) not in dropped_pairs
    ]
    removed_csi_rows = len(csi_rows) - len(filtered_csi_rows)

    summary = {
        "duplicate_position_filter_enabled": 1,
        "duplicate_position_filter_rover_config_path": str(rover_config_path),
        "duplicate_position_filter_min_spacing_mm": float(min_spacing_mm),
        "duplicate_position_filter_axis_tolerance_m": float(axis_tolerance_m),
        "duplicate_position_filtered_cycles": len(dropped_pairs),
        "duplicate_position_filtered_position_rows": len(position_rows) - len(filtered_position_rows),
        "duplicate_position_filtered_csi_rows": removed_csi_rows,
    }

    if dropped_pairs:
        logger.info(
            "Filtered %d duplicate-position cycles using axis tolerance %.6f m "
            "(min_spacing=%.3f mm from %s); removed %d CSI rows",
            summary["duplicate_position_filtered_cycles"],
            axis_tolerance_m,
            min_spacing_mm,
            rover_config_path,
            removed_csi_rows,
        )
    else:
        logger.info(
            "Duplicate-position filter found no repeated cycles using axis tolerance %.6f m "
            "(min_spacing=%.3f mm from %s)",
            axis_tolerance_m,
            min_spacing_mm,
            rover_config_path,
        )

    return filtered_csi_rows, filtered_position_rows, summary


def build_dataset(
    csi_rows: list[dict[str, Any]],
    position_rows: list[dict[str, Any]],
) -> xr.Dataset:
    if not csi_rows:
        raise ValueError("No CSI rows were extracted from the host result files.")

    relevant_pairs = {
        (str(row["experiment_id"]), int(row["cycle_id"]))
        for row in csi_rows
    }
    experiment_ids = sorted({experiment_id for experiment_id, _ in relevant_pairs})
    cycle_ids = sorted({cycle_id for _, cycle_id in relevant_pairs})
    hostnames = sorted({row["hostname"] for row in csi_rows})

    exp_index = {experiment_id: idx for idx, experiment_id in enumerate(experiment_ids)}
    cycle_index = {cycle_id: idx for idx, cycle_id in enumerate(cycle_ids)}
    host_index = {hostname: idx for idx, hostname in enumerate(hostnames)}

    position_shape = (len(experiment_ids), len(cycle_ids))
    csi_shape = (len(experiment_ids), len(cycle_ids), len(hostnames))

    x = np.full(position_shape, np.nan, dtype=np.float64)
    y = np.full(position_shape, np.nan, dtype=np.float64)
    z = np.full(position_shape, np.nan, dtype=np.float64)
    position_available = np.zeros(position_shape, dtype=np.float32)

    relevant_position_rows, position_coverage = summarize_position_coverage(
        csi_rows,
        position_rows,
    )
    ignored_position_rows = len(position_rows) - position_coverage["matched_position_rows"]
    if ignored_position_rows:
        logger.info(
            "Ignoring %d position rows without matching CSI data",
            ignored_position_rows,
        )
    if position_coverage["missing_position_rows"]:
        logger.warning(
            "Missing %d position rows for extracted CSI experiment_id/cycle_id pairs",
            position_coverage["missing_position_rows"],
        )
    logger.info(
        "Position coverage for CSI pairs: valid=%d invalid_or_missing=%d "
        "(invalid_status=%d missing_coordinates=%d missing_rows=%d total_pairs=%d)",
        position_coverage["valid_position_rows"],
        position_coverage["invalid_or_missing_position_rows"],
        position_coverage["invalid_status_rows"],
        position_coverage["missing_coordinate_rows"],
        position_coverage["missing_position_rows"],
        position_coverage["csi_pair_count"],
    )
    if not relevant_position_rows:
        logger.warning(
            "No position rows matched the extracted CSI experiment_id/cycle_id pairs. "
            "Rover position variables will remain NaN."
        )

    for row in relevant_position_rows:
        exp_idx = exp_index[row["experiment_id"]]
        cyc_idx = cycle_index[int(row["cycle_id"])]
        x[exp_idx, cyc_idx] = row["x"]
        y[exp_idx, cyc_idx] = row["y"]
        z[exp_idx, cyc_idx] = row["z"]
        position_available[exp_idx, cyc_idx] = row["position_available"]

    csi_real = np.full(csi_shape, np.nan, dtype=np.float64)
    csi_imag = np.full(csi_shape, np.nan, dtype=np.float64)
    csi_available = np.zeros(csi_shape, dtype=np.float32)

    for row in csi_rows:
        exp_idx = exp_index[row["experiment_id"]]
        cyc_idx = cycle_index[int(row["cycle_id"])]
        host_idx = host_index[row["hostname"]]
        csi_real[exp_idx, cyc_idx, host_idx] = row["csi_real"]
        csi_imag[exp_idx, cyc_idx, host_idx] = row["csi_imag"]
        csi_available[exp_idx, cyc_idx, host_idx] = 1.0

    logger.info(
        "Building xarray dataset: experiments=%d cycles=%d hostnames=%d",
        len(experiment_ids),
        len(cycle_ids),
        len(hostnames),
    )
    dataset_attrs = {
        "description": "Rover positions and per-host CSI joined on experiment_id and cycle_id.",
        "csi_definition": (
            "CSI is stored as csi_real + 1j * csi_imag. "
            "Phase is np.angle(csi_real + 1j * csi_imag)."
        ),
        **position_coverage,
    }
    last_measurement = summarize_last_measurement(csi_rows)
    if last_measurement is not None:
        dataset_attrs["last_measurement_timestamp"] = last_measurement["timestamp"]
        dataset_attrs["last_measurement_timestamp_source"] = "source file mtime"
        dataset_attrs["last_measurement_source_path"] = last_measurement["source_path"]
    dataset = xr.Dataset(
        data_vars={
            "rover_x": (("experiment_id", "cycle_id"), x),
            "rover_y": (("experiment_id", "cycle_id"), y),
            "rover_z": (("experiment_id", "cycle_id"), z),
            "position_available": (("experiment_id", "cycle_id"), position_available),
            "csi_real": (("experiment_id", "cycle_id", "hostname"), csi_real),
            "csi_imag": (("experiment_id", "cycle_id", "hostname"), csi_imag),
            "csi_available": (("experiment_id", "cycle_id", "hostname"), csi_available),
        },
        coords={
            "experiment_id": np.asarray(experiment_ids, dtype=str),
            "cycle_id": np.asarray(cycle_ids, dtype=np.int64),
            "hostname": np.asarray(hostnames, dtype=str),
        },
        attrs=dataset_attrs,
    )
    return dataset


def write_dataset(dataset: xr.Dataset, output_file: Path) -> Path:
    output_file = output_file.expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_target = output_file
    if write_target.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        write_target = output_file.with_name(f"{output_file.stem}_{timestamp}{output_file.suffix}")
        suffix_counter = 1
        while write_target.exists():
            write_target = output_file.with_name(
                f"{output_file.stem}_{timestamp}_{suffix_counter:02d}{output_file.suffix}"
            )
            suffix_counter += 1
        logger.warning(
            "Output file already exists, writing to timestamped file instead: %s",
            write_target,
        )
    logger.info("Writing NetCDF dataset to %s", write_target)
    dataset.to_netcdf(str(write_target), engine="scipy", mode="w")
    return write_target


def format_summary_list(values: Iterable[Any], max_items: int = 6) -> str:
    items = [str(value) for value in values]
    if len(items) <= max_items:
        return ", ".join(items)
    visible_items = items[:max_items]
    return ", ".join(visible_items) + f", ... (+{len(items) - max_items} more)"


def find_first_position_issue(
    cycle_ids: np.ndarray,
    valid_position_mask: np.ndarray,
) -> dict[str, Any] | None:
    cycle_ids = np.asarray(cycle_ids, dtype=int)
    valid_position_mask = np.asarray(valid_position_mask, dtype=bool)
    issue_indices = np.flatnonzero(~valid_position_mask)
    if issue_indices.size == 0:
        return None

    issue_index = int(issue_indices[0])
    if issue_index == 0:
        reason = "position unavailable from first CSI cycle"
    elif valid_position_mask[issue_index - 1]:
        reason = "position lost after previous CSI cycle"
    else:
        reason = "position still unavailable"
    return {
        "cycle_id": int(cycle_ids[issue_index]),
        "reason": reason,
    }


def find_first_csi_drop(
    cycle_ids: np.ndarray,
    host_counts: np.ndarray,
) -> dict[str, Any] | None:
    cycle_ids = np.asarray(cycle_ids, dtype=int)
    host_counts = np.asarray(host_counts, dtype=int)
    if host_counts.size < 2:
        return None

    drop_indices = np.flatnonzero(host_counts[1:] < host_counts[:-1]) + 1
    if drop_indices.size == 0:
        return None

    drop_index = int(drop_indices[0])
    return {
        "cycle_id": int(cycle_ids[drop_index]),
        "previous_host_count": int(host_counts[drop_index - 1]),
        "host_count": int(host_counts[drop_index]),
    }


def summarize_last_measurement(
    csi_rows: list[dict[str, Any]],
    experiment_id: str | None = None,
) -> dict[str, str] | None:
    relevant_rows = csi_rows
    if experiment_id is not None:
        relevant_rows = [
            row for row in csi_rows if str(row["experiment_id"]) == str(experiment_id)
        ]
    relevant_rows = [
        row for row in relevant_rows if row.get("measurement_timestamp")
    ]
    if not relevant_rows:
        return None

    latest_row = max(
        relevant_rows,
        key=lambda row: (
            str(row["measurement_timestamp"]),
            str(row.get("measurement_source_path", "")),
            str(row["hostname"]),
        ),
    )
    return {
        "timestamp": str(latest_row["measurement_timestamp"]),
        "source_path": str(latest_row.get("measurement_source_path", "")),
    }


def print_extraction_summary(
    csi_rows: list[dict[str, Any]],
    dataset: xr.Dataset,
    *,
    output_path: Path,
    data_root: Path,
    positions_root: Path,
    skipped_files: int,
) -> None:
    experiment_ids = dataset.coords["experiment_id"].values.astype(str).tolist()
    cycle_ids = dataset.coords["cycle_id"].values.astype(int)
    hostnames = dataset.coords["hostname"].values.astype(str).tolist()

    csi_available = dataset["csi_available"].values > 0
    csi_pair_mask = csi_available.any(axis=2)
    csi_pair_count = int(csi_pair_mask.sum())
    csi_measurement_count = int(csi_available.sum())

    has_coordinates = (
        np.isfinite(dataset["rover_x"].values)
        & np.isfinite(dataset["rover_y"].values)
        & np.isfinite(dataset["rover_z"].values)
    )
    valid_position_mask = csi_pair_mask & (dataset["position_available"].values > 0) & has_coordinates
    invalid_or_missing_mask = csi_pair_mask & ~valid_position_mask

    print("Extraction summary:")
    print(f"  Output dataset: {output_path}")
    print(f"  CSI data root: {data_root}")
    print(f"  Positions root: {positions_root}")
    print(
        "  Dataset dims: "
        f"experiment_id={dataset.sizes['experiment_id']} "
        f"cycle_id={dataset.sizes['cycle_id']} "
        f"hostname={dataset.sizes['hostname']}"
    )
    print(
        "  Experiments: "
        f"{len(experiment_ids)} ({format_summary_list(experiment_ids)})"
    )
    print(
        "  Hostnames: "
        f"{len(hostnames)} ({format_summary_list(hostnames)})"
    )
    if cycle_ids.size:
        print(
            "  Cycle ID range: "
            f"{int(cycle_ids.min())}..{int(cycle_ids.max())} "
            f"({len(cycle_ids)} total cycle IDs in dataset)"
        )
    print(
        "  CSI coverage: "
        f"{csi_pair_count} experiment/cycle pairs with CSI, "
        f"{csi_measurement_count} host measurements"
    )
    if "duplicate_position_filtered_cycles" in dataset.attrs:
        print(
            "  Duplicate-position filter: "
            f"removed {int(dataset.attrs['duplicate_position_filtered_cycles'])} cycles "
            f"and {int(dataset.attrs['duplicate_position_filtered_csi_rows'])} CSI rows "
            f"(axis_tolerance={float(dataset.attrs['duplicate_position_filter_axis_tolerance_m']):.6f} m, "
            f"min_spacing={float(dataset.attrs['duplicate_position_filter_min_spacing_mm']):.3f} mm)"
        )
    last_measurement = summarize_last_measurement(csi_rows)
    if last_measurement is not None:
        print(
            "  Last measurement timestamp: "
            f"{last_measurement['timestamp']} "
            "(source file mtime)"
        )
    print(
        "  Position coverage for CSI pairs: "
        f"{int(dataset.attrs['valid_position_rows'])} valid, "
        f"{int(dataset.attrs['invalid_or_missing_position_rows'])} invalid_or_missing "
        f"(invalid_status={int(dataset.attrs['invalid_status_rows'])}, "
        f"missing_coordinates={int(dataset.attrs['missing_coordinate_rows'])}, "
        f"missing_rows={int(dataset.attrs['missing_position_rows'])})"
    )

    for exp_idx, experiment_id in enumerate(experiment_ids):
        experiment_csi_pair_mask = csi_pair_mask[exp_idx]
        cycles_with_csi = int(experiment_csi_pair_mask.sum())
        if not cycles_with_csi:
            continue
        experiment_cycle_ids = cycle_ids[experiment_csi_pair_mask]
        experiment_host_counts = csi_available[exp_idx, experiment_csi_pair_mask].sum(axis=1)
        first_position_issue = find_first_position_issue(
            experiment_cycle_ids,
            valid_position_mask[exp_idx, experiment_csi_pair_mask],
        )
        first_csi_drop = find_first_csi_drop(
            experiment_cycle_ids,
            experiment_host_counts,
        )
        hosts_with_csi = int(csi_available[exp_idx].any(axis=0).sum())
        valid_positions = int(valid_position_mask[exp_idx].sum())
        invalid_or_missing_positions = int(invalid_or_missing_mask[exp_idx].sum())
        if first_position_issue is None:
            first_position_issue_summary = "none"
        else:
            first_position_issue_summary = (
                f"cycle {first_position_issue['cycle_id']} "
                f"({first_position_issue['reason']})"
            )
        if first_csi_drop is None:
            first_csi_drop_summary = "none"
        else:
            first_csi_drop_summary = (
                f"cycle {first_csi_drop['cycle_id']} "
                f"({first_csi_drop['previous_host_count']} -> "
                f"{first_csi_drop['host_count']} hosts)"
            )
        experiment_last_measurement = summarize_last_measurement(csi_rows, experiment_id)
        if experiment_last_measurement is None:
            last_measurement_summary = "none"
        else:
            last_measurement_summary = experiment_last_measurement["timestamp"]
        print(
            f"  {experiment_id}: cycles_with_csi={cycles_with_csi} "
            f"hosts_with_csi={hosts_with_csi} "
            f"valid_positions={valid_positions} "
            f"invalid_or_missing_positions={invalid_or_missing_positions} "
            f"first_position_issue={first_position_issue_summary} "
            f"first_csi_drop={first_csi_drop_summary} "
            f"last_measurement_timestamp={last_measurement_summary}"
        )

    if skipped_files:
        print(f"  Skipped result files: {skipped_files}")


def main() -> int:
    configure_logging()
    args = parse_args()
    args.config_file = args.config_file.expanduser().resolve()
    args.cable_file = args.cable_file.expanduser().resolve()
    args.positions_root = args.positions_root.expanduser().resolve()
    args.rover_config = args.rover_config.expanduser().resolve()
    logger.info("Starting CSI extraction")
    logger.info("Config file: %s", args.config_file)
    logger.info("Positions root: %s", args.positions_root)
    logger.info("Rover config: %s", args.rover_config)
    logger.info("Base output file: %s", args.output)
    logger.info("Worker threads: %d", args.workers)
    if args.experiment_ids:
        logger.info("Experiment filter: %s", ", ".join(args.experiment_ids))
    if args.max_measurements is not None:
        logger.info("Max measurements per hostname folder: %d", args.max_measurements)

    logger.info("Loading experiment settings from %s", args.config_file)
    settings = load_yaml_mapping(args.config_file)
    data_root = resolve_data_root(args, settings)
    duplicate_position_filter = load_duplicate_position_filter_settings(args.rover_config)
    logger.info(
        "Duplicate-position filter axis tolerance: %.6f m (min_spacing=%.3f mm)",
        duplicate_position_filter["axis_tolerance_m"],
        duplicate_position_filter["min_spacing_mm"],
    )
    cable_phases = load_cable_phases(args.cable_file)
    csi_rows, skipped_files = collect_csi_rows(
        data_root,
        cable_phases,
        max_measurements=args.max_measurements,
        workers=args.workers,
        experiment_ids=args.experiment_ids,
    )
    if not csi_rows and args.experiment_ids:
        raise ValueError(
            "No CSI rows were found for requested experiment IDs: "
            + ", ".join(args.experiment_ids)
        )
    if args.experiment_ids:
        found_experiment_ids = {str(row["experiment_id"]) for row in csi_rows}
        missing_experiment_ids = [
            experiment_id
            for experiment_id in args.experiment_ids
            if experiment_id not in found_experiment_ids
        ]
        if missing_experiment_ids:
            logger.warning(
                "Requested experiment IDs without matching CSI data: %s",
                ", ".join(missing_experiment_ids),
            )
    position_rows = load_positions(args.positions_root, experiment_ids=args.experiment_ids)
    csi_rows, position_rows, duplicate_position_summary = filter_duplicate_position_cycles(
        csi_rows,
        position_rows,
        axis_tolerance_m=duplicate_position_filter["axis_tolerance_m"],
        min_spacing_mm=duplicate_position_filter["min_spacing_mm"],
        rover_config_path=args.rover_config,
    )
    if not csi_rows:
        raise ValueError(
            "All CSI rows were removed by the duplicate-position filter. "
            "Check the rover min_spacing setting and logged position units."
        )
    dataset = build_dataset(csi_rows, position_rows)
    dataset.attrs.update(duplicate_position_summary)
    experiment_ids = [str(experiment_id) for experiment_id in dataset.coords["experiment_id"].values.tolist()]
    output_file = build_output_path(args.output, experiment_ids)
    logger.info("Experiment IDs in dataset: %s", ", ".join(experiment_ids))
    logger.info("Output file: %s", output_file)
    output_path = write_dataset(dataset, output_file)
    print_extraction_summary(
        csi_rows,
        dataset,
        output_path=output_path,
        data_root=data_root,
        positions_root=args.positions_root,
        skipped_files=skipped_files,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
