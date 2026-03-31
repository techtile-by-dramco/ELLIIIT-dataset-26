#!/usr/bin/env python3
"""Build an xarray dataset with rover positions and CSI per host/cycle."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
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


DEFAULT_SETTINGS_FILE = REPO_ROOT / "experiment-settings.yaml"
DEFAULT_CABLE_FILE = REPO_ROOT / "client" / "ref-RF-cable.yml"
DEFAULT_POSITIONS_ROOT = REPO_ROOT / "server" / "record" / "data"
DEFAULT_OUTPUT_FILE = Path(__file__).resolve().with_name("csi_positions.nc")
SUPPORTED_RESULT_SUFFIXES = {".json", ".jsonl", ".yaml", ".yml", ".csv", ".txt", ".log"}
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
    parser = argparse.ArgumentParser(
        description=(
            "Read host result files from experiment storage, join them with "
            "server/record/data position logs, and write one xarray NetCDF dataset."
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
        default=None,
        help=(
            "Optional path to the directory that contains the hostname folders. "
            "Overrides experiment_config.storage_path."
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
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help="NetCDF file written with the joined xarray dataset.",
    )
    return parser.parse_args()


def load_yaml_mapping(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}, got {type(data).__name__}.")
    return data


def warn(message: str) -> None:
    print(f"Warning: {message}", file=sys.stderr)


def is_smb_path(storage_path: str) -> bool:
    return storage_path.startswith("\\\\") or storage_path.startswith("//")


def dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def find_mount_dir_for_source(source: str) -> Path | None:
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as mounts:
            for line in mounts:
                parts = line.split()
                if len(parts) >= 2 and parts[0] == source:
                    return Path(parts[1])
    except FileNotFoundError:
        return None
    return None


def build_mount_option_candidates_for_access(
    experiment_config: dict[str, Any],
    access_mode: str,
) -> list[str]:
    configured_version = runtime_storage.get_storage_setting(
        experiment_config,
        env_name="SMB_VERSION",
        config_key="storage_smb_version",
        default="3.0",
    )
    smb_versions = dedupe_preserve_order(
        [str(configured_version), "3.1.1", "3.0", "2.1", "2.0", "1.0"]
    )

    base_options = [
        access_mode,
        f"uid={os.getuid()}",
        f"gid={os.getgid()}",
        "file_mode=0664",
        "dir_mode=0775",
    ]

    credentials_file = runtime_storage.get_storage_setting(
        experiment_config,
        env_name="SMB_CREDENTIALS_FILE",
        config_key="storage_credentials_file",
    )
    if credentials_file:
        return [
            ",".join(base_options + [f"vers={version}", f"credentials={credentials_file}"])
            for version in smb_versions
        ]

    username = runtime_storage.get_storage_setting(
        experiment_config,
        env_name="SMB_USERNAME",
        config_key="storage_username",
    )
    password = runtime_storage.get_storage_setting(
        experiment_config,
        env_name="SMB_PASSWORD",
        config_key="storage_password",
    )
    domain = runtime_storage.get_storage_setting(
        experiment_config,
        env_name="SMB_DOMAIN",
        config_key="storage_domain",
    )

    if username:
        candidates = []
        for version in smb_versions:
            auth_options = base_options + [f"vers={version}", f"username={username}"]
            if password is not None:
                auth_options.append(f"password={password}")
            else:
                auth_options.append("password=")
            if domain:
                auth_options.append(f"domain={domain}")
            candidates.append(",".join(auth_options))
        return candidates

    guest_variants = [
        [f"vers={version}", "guest"] for version in smb_versions
    ] + [
        [f"vers={version}", "guest", "sec=none"] for version in smb_versions
    ] + [
        [f"vers={version}", "username=guest", "password="] for version in smb_versions
    ] + [
        [f"vers={version}", "username=guest", "password=", "sec=none"]
        for version in smb_versions
    ]
    return dedupe_preserve_order(
        [",".join(base_options + variant) for variant in guest_variants]
    )


def resolve_readable_storage_base(
    storage_path: str,
    settings_path: Path,
    experiment_config: dict[str, Any],
) -> Path:
    source, relative_parts = runtime_storage.parse_storage_path(storage_path)
    if source is None:
        local_base = Path(storage_path).expanduser()
        if not local_base.is_absolute():
            local_base = (settings_path.parent / local_base).resolve()
        return local_base

    existing_mount_dir = find_mount_dir_for_source(source)
    if existing_mount_dir is not None:
        return existing_mount_dir.joinpath(*relative_parts)

    mount_root = Path(
        runtime_storage.get_storage_setting(
            experiment_config,
            env_name="SMB_MOUNT_ROOT",
            config_key="storage_mount_root",
            default=runtime_storage.DEFAULT_MOUNT_ROOT,
        )
    ).expanduser()
    mount_dir = mount_root / runtime_storage.sanitize_mount_name(source)

    try:
        runtime_storage.ensure_cifs_mount(
            source=source,
            mount_dir=mount_dir,
            mount_option_candidates=runtime_storage.build_mount_option_candidates(
                experiment_config
            ),
        )
    except RuntimeError as primary_error:
        read_write_candidates = build_mount_option_candidates_for_access(
            experiment_config,
            access_mode="rw",
        )
        try:
            runtime_storage.ensure_cifs_mount(
                source=source,
                mount_dir=mount_dir,
                mount_option_candidates=read_write_candidates,
            )
        except RuntimeError as read_write_error:
            read_only_candidates = build_mount_option_candidates_for_access(
                experiment_config,
                access_mode="ro",
            )
            try:
                runtime_storage.ensure_cifs_mount(
                    source=source,
                    mount_dir=mount_dir,
                    mount_option_candidates=read_only_candidates,
                )
            except RuntimeError as read_only_error:
                raise RuntimeError(
                    f"{primary_error}\n"
                    "Expanded read/write extractor fallback also failed.\n"
                    f"{read_write_error}\n"
                    "Read-only extractor fallback also failed.\n"
                    f"{read_only_error}\n"
                    "If the share is already mounted somewhere else, pass that path with --data-root."
                ) from read_only_error

    return mount_dir.joinpath(*relative_parts)


def resolve_data_root(args: argparse.Namespace, settings: dict[str, Any]) -> Path:
    if args.data_root is not None:
        data_root = args.data_root.expanduser().resolve()
        if not data_root.exists():
            raise FileNotFoundError(f"Data root does not exist: {data_root}")
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
            return smb_candidate

        if os.name == "nt":
            data_root = Path(storage_path)
            if not data_root.exists():
                raise FileNotFoundError(f"SMB path does not exist: {data_root}")
            return data_root

        data_root = resolve_readable_storage_base(
            storage_path=str(storage_path),
            settings_path=args.config_file.resolve(),
            experiment_config=experiment_config,
        )
        if not data_root.exists():
            raise FileNotFoundError(f"Resolved SMB mount does not exist: {data_root}")
        return data_root

    data_root = Path(storage_path).expanduser()
    if not data_root.is_absolute():
        data_root = (args.config_file.resolve().parent / data_root).resolve()
    if not data_root.exists():
        raise FileNotFoundError(f"Data root does not exist: {data_root}")
    return data_root


def load_cable_phases(cable_file: Path) -> dict[str, float]:
    cable_data = load_yaml_mapping(cable_file)
    return {str(hostname).strip().upper(): float(value) for hostname, value in cable_data.items()}


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
        prefix = f"data_{host}_"
        if stem.startswith(prefix):
            experiment_id = stem[len(prefix):].strip("_")
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
        prefix = f"data_{host}_"
        if not file_stem.startswith(prefix):
            continue

        remainder = file_stem[len(prefix):]
        experiment_part, separator, cycle_part = remainder.rpartition("_")
        if not separator or not experiment_part or not cycle_part:
            continue
        try:
            return experiment_part, parse_cycle_id(cycle_part)
        except ValueError:
            continue

    return None, None


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
    }


def collect_csi_rows(
    data_root: Path,
    cable_phases: dict[str, float],
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    skipped_files = 0

    for host_dir in sorted(path for path in data_root.iterdir() if path.is_dir()):
        candidate_files = sorted(
            path
            for path in host_dir.iterdir()
            if path.is_file()
            and path.suffix.lower() in SUPPORTED_RESULT_SUFFIXES
            and not path.name.endswith("_iq.npz")
        )
        if not candidate_files:
            continue

        for result_file in candidate_files:
            try:
                records = extract_records_from_file(result_file, host_dir.name)
                if not records:
                    continue

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
                    rows.append(build_csi_row(record, phi_cable_deg))
            except Exception as exc:
                skipped_files += 1
                warn(f"Skipping {result_file}: {exc}")

    deduped: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in rows:
        deduped[(row["experiment_id"], row["cycle_id"], row["hostname"])] = row
    return list(deduped.values()), skipped_files


def load_positions(positions_root: Path) -> list[dict[str, Any]]:
    positions_root = positions_root.expanduser().resolve()
    if not positions_root.exists():
        raise FileNotFoundError(f"Positions directory does not exist: {positions_root}")

    position_rows: dict[tuple[str, int], dict[str, Any]] = {}
    for path in sorted(positions_root.glob("exp-*-positions.csv")):
        match = POSITION_FILE_PATTERN.match(path.name)
        if match is None:
            continue

        fallback_experiment_id = match.group("experiment_id")
        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                experiment_id = str(row.get("experiment_id") or fallback_experiment_id).strip()
                cycle_raw = row.get("cycle_id")
                if not experiment_id or cycle_raw in ("", None):
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

    return list(position_rows.values())


def build_dataset(
    csi_rows: list[dict[str, Any]],
    position_rows: list[dict[str, Any]],
) -> xr.Dataset:
    if not csi_rows:
        raise ValueError("No CSI rows were extracted from the host result files.")
    if not position_rows:
        raise ValueError("No position rows were found in server/record/data.")

    experiment_ids = sorted({row["experiment_id"] for row in csi_rows} | {row["experiment_id"] for row in position_rows})
    cycle_ids = sorted({int(row["cycle_id"]) for row in csi_rows} | {int(row["cycle_id"]) for row in position_rows})
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

    for row in position_rows:
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
        attrs={
            "description": "Rover positions and per-host CSI joined on experiment_id and cycle_id.",
            "csi_definition": (
                "CSI is stored as csi_real + 1j * csi_imag. "
                "Phase is np.angle(csi_real + 1j * csi_imag)."
            ),
        },
    )
    return dataset


def write_dataset(dataset: xr.Dataset, output_file: Path) -> Path:
    output_file = output_file.expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_netcdf(output_file, engine="scipy")
    return output_file


def main() -> int:
    args = parse_args()
    args.config_file = args.config_file.expanduser().resolve()
    args.cable_file = args.cable_file.expanduser().resolve()
    args.positions_root = args.positions_root.expanduser().resolve()

    settings = load_yaml_mapping(args.config_file)
    data_root = resolve_data_root(args, settings)
    cable_phases = load_cable_phases(args.cable_file)
    csi_rows, skipped_files = collect_csi_rows(data_root, cable_phases)
    position_rows = load_positions(args.positions_root)
    dataset = build_dataset(csi_rows, position_rows)
    output_path = write_dataset(dataset, args.output)

    print(f"CSI data root: {data_root}")
    print(f"Positions root: {args.positions_root}")
    print(f"Wrote dataset to {output_path}")
    print(
        "Dataset dims: "
        f"experiment_id={dataset.sizes['experiment_id']} "
        f"cycle_id={dataset.sizes['cycle_id']} "
        f"hostname={dataset.sizes['hostname']}"
    )
    if skipped_files:
        print(f"Skipped {skipped_files} result files. See stderr for details.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
