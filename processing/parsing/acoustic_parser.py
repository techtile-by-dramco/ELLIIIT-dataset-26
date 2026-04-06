#!/usr/bin/env python3
"""Parse acoustic measurement CSVs directly into one acoustic NetCDF dataset."""

from __future__ import annotations

import argparse
import ast
import csv
import json
import logging
import math
import os
import re
import sys
import time
from collections import Counter
from bisect import bisect_left, bisect_right
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from itertools import chain
from pathlib import Path
from typing import Any, Iterable

import numpy as np


logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PROCESSING_ROOT = SCRIPT_DIR.parent
PROJECT_ROOT = PROCESSING_ROOT.parent
DEFAULT_LOG_PATH = PROJECT_ROOT / "zmqclient_acoustic.log"
DEFAULT_POSITIONS_ROOT = PROJECT_ROOT
DEFAULT_SIGNAL_DIR = PROJECT_ROOT / "results"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "results" / "acoustic"
DEFAULT_WORKERS = min(32, max(4, os.cpu_count() or 1))
SIGNAL_FILE_BUFFER_SIZE = 1024 * 1024
NETCDF3_MAX_VARIABLE_BYTES = (2**31) - 4
DEFAULT_HDF5_COMPRESSION_LEVEL = 4
DEFAULT_BATCH_CYCLES = 8
EXPERIMENTS = ["EXP003","EXP005", "EXP006", "EXP007", "EXP008", "EXP009", "EXP010", "EXP011"]  # , "EXP005", "EXP006", "EXP007", "EXP008", "EXP009" Set to [] or None to include all experiments by default.
TABLE_NAMES = ("cycles", "responses", "chirps")
BATCH_ROOT_NAME = "_batches"
BATCH_MANIFESTS_NAME = "manifests"
PARQUET_COMPRESSION_CHOICES = ("none", "snappy", "gzip", "brotli", "lz4", "zstd")
PARALLELISM_CHOICES = ("threads", "processes")

LINE_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)"
    r"\s+\w+\s+(?P<message>.+)$"
)
CONTEXT_RE = re.compile(
    r"\[acoustic\]\[exp (?P<experiment_id>\S+)\]\[cycle (?P<cycle_id>\d+)\]\[meas (?P<meas_id>\d+)\]"
    r"\s+(?P<tail>.+)"
)
POSITION_FILE_PATTERN = re.compile(r"^exp-(?P<experiment_id>.+)-positions\.csv$")
SIGNAL_RE = re.compile(r"Measured_Signal_(?P<timestamp>\d{8}_\d{6})")


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def get_configured_experiment_ids() -> list[str] | None:
    if not EXPERIMENTS:
        return None
    return list(dict.fromkeys(str(experiment_id).strip() for experiment_id in EXPERIMENTS if str(experiment_id).strip()))


def dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    unique_paths: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        normalized = str(path.expanduser())
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_paths.append(Path(normalized))
    return unique_paths


def count_experiment_ids(experiment_ids: Iterable[Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for experiment_id in experiment_ids:
        normalized = str(experiment_id or "").strip()
        if normalized:
            counts[normalized] += 1
    return counts


def format_experiment_counts(counts: Counter[str] | dict[str, int], *, limit: int = 12) -> str:
    if not counts:
        return "(none)"

    items = sorted((str(experiment_id), int(count)) for experiment_id, count in counts.items() if int(count) > 0)
    preview = items[:limit]
    parts = [f"{experiment_id}={count}" for experiment_id, count in preview]
    if len(items) > limit:
        parts.append(f"... ({len(items) - limit} more)")
    return ", ".join(parts)


def format_cycle_debug(cycle: AcousticCycle) -> str:
    return (
        f"{cycle.experiment_id}/cycle {cycle.cycle_id}/meas {cycle.meas_id} "
        f"start={cycle.t_start.isoformat(sep=' ')} done={cycle.t_done.isoformat(sep=' ')}"
    )


def position_file_count(path: Path, experiment_ids: Iterable[str] | None) -> int:
    if not path.exists() or not path.is_dir():
        return 0

    selected_experiment_ids = set(str(experiment_id) for experiment_id in (experiment_ids or []))
    count = 0
    for file_path in path.glob("exp-*-positions.csv"):
        match = POSITION_FILE_PATTERN.match(file_path.name)
        if match is None:
            continue
        experiment_id = match.group("experiment_id")
        if selected_experiment_ids and experiment_id not in selected_experiment_ids:
            continue
        count += 1
    return count


def has_signal_files(path: Path, recursive: bool) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    iterator = path.rglob("Measured_Signal_*.csv") if recursive else path.glob("Measured_Signal_*.csv")
    return next(iterator, None) is not None


def resolve_existing_file(path: Path, fallback_candidates: Iterable[Path], label: str) -> Path:
    requested_path = path.expanduser()
    if requested_path.exists():
        return requested_path.resolve()

    for candidate in dedupe_paths(fallback_candidates):
        if candidate.exists():
            resolved_candidate = candidate.resolve()
            logger.info(
                "%s not found at %s; using %s",
                label,
                requested_path,
                resolved_candidate,
            )
            return resolved_candidate

    raise FileNotFoundError(f"{label} does not exist: {requested_path}")


def resolve_positions_root(path: Path, experiment_ids: Iterable[str] | None) -> Path:
    requested_path = path.expanduser()
    if position_file_count(requested_path, experiment_ids) > 0:
        return requested_path.resolve()

    fallback_candidates = [
        PROCESSING_ROOT,
        PROJECT_ROOT,
        Path.cwd(),
        PROJECT_ROOT / "server" / "record" / "data",
    ]
    for candidate in dedupe_paths(fallback_candidates):
        if position_file_count(candidate, experiment_ids) > 0:
            resolved_candidate = candidate.resolve()
            logger.info(
                "Positions root not usable at %s; using %s",
                requested_path,
                resolved_candidate,
            )
            return resolved_candidate

    raise FileNotFoundError(
        f"Positions directory does not contain matching exp-*-positions.csv files: {requested_path}"
    )


def resolve_signal_dir(path: Path, recursive: bool) -> Path:
    requested_path = path.expanduser()
    if has_signal_files(requested_path, recursive):
        return requested_path.resolve()

    fallback_candidates = [
        PROJECT_ROOT / "results",
        PROCESSING_ROOT / "results",
        Path.cwd() / "results",
        PROJECT_ROOT / "acoustic" / "results",
    ]
    for candidate in dedupe_paths(fallback_candidates):
        if has_signal_files(candidate, recursive):
            resolved_candidate = candidate.resolve()
            logger.info(
                "Signal directory not usable at %s; using %s",
                requested_path,
                resolved_candidate,
            )
            return resolved_candidate

    raise FileNotFoundError(
        f"Signal directory does not contain Measured_Signal_*.csv files: {requested_path}"
    )


def resolve_runtime_inputs(
    log_path: Path,
    signal_dir: Path,
    *,
    recursive_signal_search: bool,
) -> tuple[Path, Path]:
    resolved_log_path = resolve_existing_file(
        log_path,
        [
            PROCESSING_ROOT / "zmqclient_acoustic.log",
            PROJECT_ROOT / "zmqclient_acoustic.log",
            Path.cwd() / "zmqclient_acoustic.log",
            PROJECT_ROOT / "acoustic" / "logs" / "zmqclient_acoustic.log",
        ],
        "Acoustic log file",
    )
    resolved_signal_dir = resolve_signal_dir(signal_dir, recursive_signal_search)
    return resolved_log_path, resolved_signal_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Parse acoustic measurement logs and CSV result files into one "
            "NetCDF dataset keyed by experiment_id and cycle_id."
        )
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=DEFAULT_LOG_PATH,
        help="Acoustic client log file used to recover experiment_id/cycle_id timing.",
    )
    parser.add_argument(
        "--positions-root",
        type=Path,
        default=DEFAULT_POSITIONS_ROOT,
        help="Directory that contains exp-<experiment_id>-positions.csv files.",
    )
    parser.add_argument(
        "--signal-dir",
        type=Path,
        default=DEFAULT_SIGNAL_DIR,
        help="Directory that contains Measured_Signal_*.csv result files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory that will receive the NetCDF export.",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Only parse the first N matched acoustic cycles after filtering.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of worker threads or processes used for per-file CSV parsing.",
    )
    parser.add_argument(
        "--parallelism",
        choices=PARALLELISM_CHOICES,
        default="processes",
        help=(
            "Parallel backend for per-file parsing. "
            "Use 'processes' by default for faster NumPy-heavy parsing, "
            "or 'threads' when disk I/O is the main bottleneck."
        ),
    )
    parser.add_argument(
        "--batch-cycles",
        type=int,
        default=DEFAULT_BATCH_CYCLES,
        help="Deprecated legacy option. Ignored; the parser now writes NetCDF directly.",
    )
    parser.add_argument(
        "--signal-dtype",
        choices=("float32", "float64"),
        default="float32",
        help="Numeric dtype used for parsed response and chirp arrays.",
    )
    parser.add_argument(
        "--hdf5-compression-level",
        type=int,
        default=DEFAULT_HDF5_COMPRESSION_LEVEL,
        help=(
            "Gzip compression level for HDF5-backed NetCDF output (`h5netcdf` or `netCDF4`). "
            "Set to 0 to disable compression."
        ),
    )
    parser.add_argument(
        "--batch-compression",
        choices=PARQUET_COMPRESSION_CHOICES,
        default="none",
        help="Deprecated legacy option. Ignored; no parquet files are written.",
    )
    parser.add_argument(
        "--final-compression",
        choices=PARQUET_COMPRESSION_CHOICES,
        default="snappy",
        help="Deprecated legacy option. Ignored; no parquet files are written.",
    )
    parser.add_argument(
        "--timestamp-tolerance-s",
        type=float,
        default=1.0,
        help=(
            "Grace window added around the START_MEAS/MEAS_DONE range when "
            "matching second-resolution Measured_Signal timestamps."
        ),
    )
    parser.add_argument(
        "--no-recursive-signal-search",
        dest="recursive_signal_search",
        action="store_false",
        help="Only search the top level of --signal-dir for result files.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--skip-merge",
        dest="merge_batches",
        action="store_false",
        help="Deprecated legacy option. Ignored; the parser writes NetCDF directly.",
    )
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Deprecated legacy option. Ignored; the parser writes NetCDF directly.",
    )
    parser.add_argument(
        "--no-progress",
        dest="show_progress",
        action="store_false",
        help="Disable the processing progress bar.",
    )
    parser.set_defaults(recursive_signal_search=True)
    parser.set_defaults(merge_batches=True)
    parser.set_defaults(show_progress=True)

    args = parser.parse_args()
    if args.max_cycles is not None and args.max_cycles <= 0:
        parser.error("--max-cycles must be a positive integer.")
    if args.workers <= 0:
        parser.error("--workers must be a positive integer.")
    if args.batch_cycles <= 0:
        parser.error("--batch-cycles must be a positive integer.")
    if not 0 <= args.hdf5_compression_level <= 9:
        parser.error("--hdf5-compression-level must be between 0 and 9.")
    if args.timestamp_tolerance_s < 0:
        parser.error("--timestamp-tolerance-s must be non-negative.")
    return args


def ensure_csv_field_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


def require_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - runtime environment dependent
        raise RuntimeError(
            "pyarrow is required for acoustic parquet export. "
            "Install it with `python -m pip install pyarrow`."
        ) from exc
    return pa, pq


def require_xarray():
    try:
        import xarray as xr
    except ImportError as exc:  # pragma: no cover - runtime environment dependent
        raise RuntimeError(
            "xarray is required for acoustic NetCDF export. "
            "Install it with `python -m pip install xarray`."
        ) from exc
    return xr


def resolve_netcdf_engine() -> str:
    try:
        import h5netcdf  # noqa: F401
    except ImportError:
        pass
    else:
        return "h5netcdf"

    try:
        import netCDF4  # noqa: F401
    except ImportError:
        pass
    else:
        return "netcdf4"

    try:
        from scipy import io as _scipy_io  # noqa: F401
    except ImportError as exc:  # pragma: no cover - runtime environment dependent
        install_hint = f"`{sys.executable} -m pip install scipy`"
        raise RuntimeError(
            "A NetCDF writer is required for acoustic xarray export. "
            "This Python interpreter could not import any of: `h5netcdf`, `netCDF4`, or `scipy`. "
            f"Install one into the same interpreter, for example with {install_hint}."
        ) from exc
    return "scipy"


def resolve_parquet_compression(name: str) -> str | None:
    return None if name == "none" else name


def format_gibibytes(byte_count: int) -> str:
    return f"{byte_count / (1024 ** 3):.2f} GiB"


def build_netcdf_encoding(
    dataset: Any,
    *,
    engine: str,
    hdf5_compression_level: int,
) -> dict[str, dict[str, Any]] | None:
    if hdf5_compression_level <= 0:
        return None
    if engine not in {"h5netcdf", "netcdf4"}:
        return None

    level = int(hdf5_compression_level)
    encoding: dict[str, dict[str, Any]] = {}
    for variable_name in dataset.data_vars:
        name = str(variable_name)
        if engine == "h5netcdf":
            encoding[name] = {
                "compression": "gzip",
                "compression_opts": level,
                "shuffle": True,
            }
        else:
            encoding[name] = {
                "zlib": True,
                "complevel": level,
                "shuffle": True,
            }
    return encoding or None


def experiment_slug(experiment_ids: Iterable[str] | None) -> str:
    if experiment_ids is None:
        return ""
    normalized = [
        str(experiment_id).strip()
        for experiment_id in experiment_ids
        if str(experiment_id).strip()
    ]
    return "__".join(dict.fromkeys(normalized))


@dataclass(frozen=True)
class AcousticCycle:
    experiment_id: str
    cycle_id: int
    meas_id: int | None
    t_start: datetime
    t_done: datetime
    duration_s: float


@dataclass(frozen=True)
class PositionRow:
    experiment_id: str
    cycle_id: int
    x: float
    y: float
    z: float
    position_available: float


@dataclass(frozen=True)
class IndexedSignalFile:
    timestamp: datetime
    path: Path


@dataclass(frozen=True)
class MatchedCycle:
    cycle: AcousticCycle
    position: PositionRow | None
    signal_file: IndexedSignalFile
    signal_path: str


@dataclass(frozen=True)
class ParsedSignalFile:
    sweep_duration: float
    f_start: float
    f_stop: float
    chirp_amp: float
    chirp_excitation: np.ndarray | None
    microphones: list[dict[str, Any]]


@dataclass(frozen=True)
class CompletedBatch:
    batch_id: int
    manifest_path: Path
    files: dict[str, Path]
    cycle_keys: list[tuple[str, int]]
    cycle_count: int
    response_count: int
    chirp_count: int


class ProgressTracker:
    def __init__(
        self,
        total: int,
        *,
        enabled: bool,
        description: str,
        unit: str = "file",
    ):
        self.total = max(int(total), 0)
        self.enabled = bool(enabled and self.total > 0)
        self.description = description
        self.unit = unit
        self.current = 0
        self._bar = None
        self._start = time.monotonic()
        self._last_render = 0.0
        self._is_tty = sys.stderr.isatty()

        if not self.enabled:
            return

        try:
            from tqdm import tqdm  # type: ignore
        except ImportError:
            self._render(force=True)
            return

        self._bar = tqdm(
            total=self.total,
            desc=self.description,
            unit=self.unit,
            dynamic_ncols=True,
            leave=True,
            smoothing=0.1,
        )

    def update(self, increment: int = 1) -> None:
        if not self.enabled or increment <= 0:
            return

        self.current = min(self.total, self.current + int(increment))
        if self._bar is not None:
            self._bar.update(increment)
            return

        self._render(force=False)

    def close(self) -> None:
        if not self.enabled:
            return

        if self._bar is not None:
            self._bar.close()
            return

        self._render(force=True)
        if self._is_tty:
            sys.stderr.write("\n")
            sys.stderr.flush()

    def _render(self, *, force: bool) -> None:
        now = time.monotonic()
        if not force and (now - self._last_render) < 0.25 and self.current < self.total:
            return

        self._last_render = now
        elapsed = max(now - self._start, 1e-9)
        rate = self.current / elapsed if self.current > 0 else 0.0
        remaining = self.total - self.current
        eta = remaining / rate if rate > 0 else math.inf
        ratio = self.current / self.total if self.total else 1.0
        bar_width = 24
        filled = min(bar_width, int(round(bar_width * ratio)))
        bar = "#" * filled + "-" * (bar_width - filled)
        eta_text = self._format_seconds(eta)
        elapsed_text = self._format_seconds(elapsed)
        line = (
            f"{self.description}: [{bar}] "
            f"{self.current}/{self.total} {self.unit}s "
            f"elapsed={elapsed_text} eta={eta_text}"
        )

        if self._is_tty:
            sys.stderr.write(f"\r{line}")
            sys.stderr.flush()
        else:
            print(line, file=sys.stderr)

    @staticmethod
    def _format_seconds(seconds: float) -> str:
        if not math.isfinite(seconds):
            return "--:--"

        total_seconds = max(0, int(round(seconds)))
        minutes, secs = divmod(total_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{secs:02d}"
        return f"{minutes:02d}:{secs:02d}"


def parse_cycle_id(value: Any) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid cycle_id: {value!r}") from exc


def parse_optional_float(value: Any, default: float = math.nan) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_numeric_vector(value: Any, dtype: np.dtype[Any]) -> np.ndarray | None:
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw or raw.lower() == "unused":
        return None

    if raw[0] in "[(" and raw[-1] in "])":
        raw = raw[1:-1].strip()

    if not raw:
        return np.asarray([], dtype=dtype)

    vector = np.fromstring(raw, sep=",", dtype=dtype)
    if vector.size > 0:
        return vector

    try:
        parsed = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return None

    array = np.asarray(parsed, dtype=dtype)
    if array.ndim == 0:
        return array.reshape(1)
    return array.reshape(-1)


def value_sequence_length(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, np.ndarray):
        return int(value.size)
    try:
        return len(value)
    except TypeError:
        return 0


def value_sequence_as_array(value: Any, dtype: np.dtype[Any]) -> np.ndarray:
    if value is None:
        return np.asarray([], dtype=dtype)
    if isinstance(value, np.ndarray):
        return value.astype(dtype, copy=False).reshape(-1)
    array = np.asarray(value, dtype=dtype)
    if array.ndim == 0:
        return array.reshape(1)
    return array.reshape(-1)


def row_contains_unused_marker(row: dict[str, Any]) -> bool:
    for value in row.values():
        if isinstance(value, str) and "unused" in value.strip().lower():
            return True
    return False


def split_signal_csv_line(line: str) -> tuple[str, str, str, str, str, str, str]:
    # Acoustic signal CSV rows have a fixed 7-column layout; parsing them directly is much
    # cheaper than constructing a DictReader row for each large waveform line.
    duration_raw, f_start_raw, f_stop_raw, chirp_amp_raw, remainder = line.split(",", 4)
    if remainder.startswith('"'):
        coord_end = remainder.find('",')
        if coord_end < 0:
            raise ValueError("missing closing quote for microphone_coordinates")
        microphone_coordinates = remainder[1:coord_end]
        label_and_values = remainder[coord_end + 2 :]
    else:
        coord_sep = remainder.find(",")
        if coord_sep < 0:
            raise ValueError("missing microphone_coordinates separator")
        microphone_coordinates = remainder[:coord_sep]
        label_and_values = remainder[coord_sep + 1 :]

    label_sep = label_and_values.find(",")
    if label_sep < 0:
        raise ValueError("missing microphone_label separator")

    microphone_label = label_and_values[:label_sep].strip()
    values_raw = label_and_values[label_sep + 1 :].strip()
    if len(values_raw) >= 2 and values_raw[0] == '"' and values_raw[-1] == '"':
        values_raw = values_raw[1:-1]

    return (
        duration_raw,
        f_start_raw,
        f_stop_raw,
        chirp_amp_raw,
        microphone_coordinates,
        microphone_label,
        values_raw,
    )


def parse_signal_vector(value: str, dtype: np.dtype[Any]) -> np.ndarray | None:
    raw = value.strip()
    if not raw or raw == "unused":
        return None
    if raw[0] in "[(" and raw[-1] in "])":
        raw = raw[1:-1].strip()
    if not raw:
        return np.asarray([], dtype=dtype)

    vector = np.fromstring(raw, sep=",", dtype=dtype)
    if vector.size > 0:
        return vector

    try:
        parsed = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return None

    array = np.asarray(parsed, dtype=dtype)
    if array.ndim == 0:
        return array.reshape(1)
    return array.reshape(-1)


def parse_microphone_coordinates(value: Any) -> tuple[float, float, float] | None:
    coordinates = parse_numeric_vector(value, np.dtype(np.float64))
    if coordinates is None:
        return None

    coordinates = coordinates.reshape(-1)
    if coordinates.size != 3 or not np.all(np.isfinite(coordinates)):
        return None

    return float(coordinates[0]), float(coordinates[1]), float(coordinates[2])


def parse_acoustic_log(
    log_path: Path,
    experiment_ids: Iterable[str] | None = None,
) -> list[AcousticCycle]:
    log_path = log_path.expanduser().resolve()
    if not log_path.exists():
        raise FileNotFoundError(f"Acoustic log file does not exist: {log_path}")

    selected_experiment_ids = set(str(experiment_id) for experiment_id in (experiment_ids or []))
    starts: dict[tuple[str, int], tuple[int | None, datetime]] = {}
    ends: dict[tuple[str, int], tuple[int | None, datetime]] = {}
    observed_context_counts: Counter[str] = Counter()
    filtered_context_counts: Counter[str] = Counter()
    start_counts: Counter[str] = Counter()
    end_counts: Counter[str] = Counter()

    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue

            line_match = LINE_RE.match(line)
            if line_match is None:
                continue

            context_match = CONTEXT_RE.match(line_match.group("message"))
            if context_match is None:
                continue

            experiment_id = context_match.group("experiment_id")
            observed_context_counts[experiment_id] += 1
            if selected_experiment_ids and experiment_id not in selected_experiment_ids:
                filtered_context_counts[experiment_id] += 1
                continue

            cycle_id = parse_cycle_id(context_match.group("cycle_id"))
            meas_id = parse_cycle_id(context_match.group("meas_id"))
            timestamp = datetime.fromisoformat(line_match.group("timestamp"))
            key = (experiment_id, cycle_id)
            tail = context_match.group("tail")

            if "START_MEAS received" in tail:
                starts[key] = (meas_id, timestamp)
                start_counts[experiment_id] += 1
            elif "MEAS_DONE status=ok" in tail:
                ends[key] = (meas_id, timestamp)
                end_counts[experiment_id] += 1

    cycles: list[AcousticCycle] = []
    inverted_counts: Counter[str] = Counter()
    for key, (start_meas_id, t_start) in starts.items():
        if key not in ends:
            continue

        end_meas_id, t_done = ends[key]
        if t_done < t_start:
            inverted_counts[key[0]] += 1
            logger.warning(
                "Skipping cycle with inverted timestamps: %s cycle %s",
                key[0],
                key[1],
            )
            continue

        experiment_id, cycle_id = key
        cycles.append(
            AcousticCycle(
                experiment_id=experiment_id,
                cycle_id=cycle_id,
                meas_id=end_meas_id if end_meas_id is not None else start_meas_id,
                t_start=t_start,
                t_done=t_done,
                duration_s=float((t_done - t_start).total_seconds()),
            )
        )

    cycles.sort(key=lambda row: row.t_start)
    completed_counts = count_experiment_ids(cycle.experiment_id for cycle in cycles)
    start_without_done_counts = count_experiment_ids(experiment_id for experiment_id, _cycle_id in starts if (experiment_id, _cycle_id) not in ends)
    done_without_start_counts = count_experiment_ids(experiment_id for experiment_id, _cycle_id in ends if (experiment_id, _cycle_id) not in starts)
    logger.info("Parsed %d completed acoustic cycles from %s", len(cycles), log_path)
    logger.info("Acoustic log completed cycles by experiment: %s", format_experiment_counts(completed_counts))
    logger.info("Acoustic log START_MEAS markers by experiment: %s", format_experiment_counts(start_counts))
    logger.info("Acoustic log MEAS_DONE markers by experiment: %s", format_experiment_counts(end_counts))
    if start_without_done_counts:
        logger.warning(
            "Acoustic log cycles with START_MEAS but no MEAS_DONE by experiment: %s",
            format_experiment_counts(start_without_done_counts),
        )
    if done_without_start_counts:
        logger.warning(
            "Acoustic log cycles with MEAS_DONE but no START_MEAS by experiment: %s",
            format_experiment_counts(done_without_start_counts),
        )
    if inverted_counts:
        logger.warning(
            "Acoustic log cycles skipped due to inverted timestamps by experiment: %s",
            format_experiment_counts(inverted_counts),
        )
    if filtered_context_counts:
        logger.info(
            "Ignored acoustic log context lines for experiments outside the active filter: %s",
            format_experiment_counts(filtered_context_counts),
        )
    if selected_experiment_ids:
        missing_from_log = sorted(
            experiment_id
            for experiment_id in selected_experiment_ids
            if observed_context_counts.get(experiment_id, 0) == 0
        )
        if missing_from_log:
            logger.warning(
                "Configured experiments not present in acoustic log context lines: %s",
                ", ".join(missing_from_log),
            )
        missing_completed = sorted(
            experiment_id
            for experiment_id in selected_experiment_ids
            if completed_counts.get(experiment_id, 0) == 0
        )
        if missing_completed:
            logger.warning(
                "Configured experiments with no completed acoustic cycles after log parsing: %s",
                ", ".join(missing_completed),
            )
    if logger.isEnabledFor(logging.DEBUG):
        start_without_done_keys = sorted(key for key in starts if key not in ends)[:10]
        for experiment_id, cycle_id in start_without_done_keys:
            start_meas_id, t_start = starts[(experiment_id, cycle_id)]
            logger.debug(
                "Missing MEAS_DONE for %s/cycle %d/meas %s start=%s",
                experiment_id,
                cycle_id,
                start_meas_id,
                t_start.isoformat(sep=" "),
            )
        done_without_start_keys = sorted(key for key in ends if key not in starts)[:10]
        for experiment_id, cycle_id in done_without_start_keys:
            end_meas_id, t_done = ends[(experiment_id, cycle_id)]
            logger.debug(
                "Missing START_MEAS for %s/cycle %d/meas %s done=%s",
                experiment_id,
                cycle_id,
                end_meas_id,
                t_done.isoformat(sep=" "),
            )
    return cycles


def load_positions(
    positions_root: Path,
    experiment_ids: Iterable[str] | None = None,
) -> dict[tuple[str, int], PositionRow]:
    positions_root = positions_root.expanduser().resolve()
    if not positions_root.exists():
        raise FileNotFoundError(f"Positions directory does not exist: {positions_root}")

    selected_experiment_ids = set(str(experiment_id) for experiment_id in (experiment_ids or []))
    position_files = sorted(positions_root.glob("exp-*-positions.csv"))
    logger.info(
        "Reading positions from %s (%d matching files)",
        positions_root,
        len(position_files),
    )

    position_rows: dict[tuple[str, int], PositionRow] = {}
    for path in position_files:
        match = POSITION_FILE_PATTERN.match(path.name)
        if match is None:
            continue

        fallback_experiment_id = match.group("experiment_id")
        if selected_experiment_ids and fallback_experiment_id not in selected_experiment_ids:
            continue

        with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                experiment_id = str(row.get("experiment_id") or fallback_experiment_id).strip()
                if not experiment_id:
                    continue
                if selected_experiment_ids and experiment_id not in selected_experiment_ids:
                    continue

                cycle_raw = row.get("cycle_id")
                if cycle_raw in ("", None):
                    continue

                try:
                    cycle_id = parse_cycle_id(cycle_raw)
                except ValueError:
                    logger.warning("Skipping position row with invalid cycle_id in %s", path)
                    continue

                position_rows[(experiment_id, cycle_id)] = PositionRow(
                    experiment_id=experiment_id,
                    cycle_id=cycle_id,
                    x=parse_optional_float(row.get("x")),
                    y=parse_optional_float(row.get("y")),
                    z=parse_optional_float(row.get("z")),
                    position_available=(
                        1.0
                        if str(row.get("position_status", "")).strip().lower() == "ok"
                        else 0.0
                    ),
                )

    logger.info("Loaded %d unique position rows", len(position_rows))
    return position_rows


def index_signal_files(signal_dir: Path, recursive: bool) -> list[IndexedSignalFile]:
    signal_dir = signal_dir.expanduser().resolve()
    if not signal_dir.exists():
        raise FileNotFoundError(f"Signal directory does not exist: {signal_dir}")

    iterator = signal_dir.rglob("Measured_Signal_*.csv") if recursive else signal_dir.glob("Measured_Signal_*.csv")
    indexed_files: list[IndexedSignalFile] = []
    for path in iterator:
        match = SIGNAL_RE.search(path.stem)
        if match is None:
            continue
        indexed_files.append(
            IndexedSignalFile(
                timestamp=datetime.strptime(match.group("timestamp"), "%Y%m%d_%H%M%S"),
                path=path,
            )
        )

    indexed_files.sort(key=lambda item: (item.timestamp, str(item.path)))
    if indexed_files:
        logger.info(
            "Indexed %d acoustic signal files under %s spanning %s .. %s",
            len(indexed_files),
            signal_dir,
            indexed_files[0].timestamp.isoformat(sep=" "),
            indexed_files[-1].timestamp.isoformat(sep=" "),
        )
    else:
        logger.warning("Indexed 0 acoustic signal files under %s", signal_dir)
    if logger.isEnabledFor(logging.DEBUG):
        for indexed_file in indexed_files[:10]:
            logger.debug(
                "Indexed signal file: %s @ %s",
                indexed_file.path,
                indexed_file.timestamp.isoformat(sep=" "),
            )
    return indexed_files


def signal_relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def candidate_score(
    candidate: IndexedSignalFile,
    cycle: AcousticCycle,
) -> tuple[int, float, str]:
    experiment_hint = cycle.experiment_id.upper()
    path_parts = {part.upper() for part in candidate.path.parts}
    path_text = str(candidate.path).upper()
    experiment_match_penalty = 0 if experiment_hint in path_parts or experiment_hint in path_text else 1
    distance_to_done_s = abs((candidate.timestamp - cycle.t_done).total_seconds())
    return (experiment_match_penalty, distance_to_done_s, str(candidate.path))


def match_cycles_to_signal_files(
    cycles: list[AcousticCycle],
    signal_files: list[IndexedSignalFile],
    signal_dir: Path,
    tolerance: timedelta,
) -> tuple[list[MatchedCycle], list[AcousticCycle]]:
    if not signal_files:
        return [], cycles

    timestamps = [item.timestamp for item in signal_files]
    used_indices: set[int] = set()
    matched_cycles: list[MatchedCycle] = []
    missing_cycles: list[AcousticCycle] = []
    matched_counts: Counter[str] = Counter()
    missing_counts: Counter[str] = Counter()
    missing_no_window_counts: Counter[str] = Counter()
    missing_all_used_counts: Counter[str] = Counter()
    ambiguous_candidate_counts: Counter[str] = Counter()

    for cycle in cycles:
        window_start = cycle.t_start - tolerance
        window_end = cycle.t_done + tolerance
        start_idx = bisect_left(timestamps, window_start)
        end_idx = bisect_right(timestamps, window_end)
        candidate_count_in_window = end_idx - start_idx
        candidate_indices = [
            index for index in range(start_idx, end_idx) if index not in used_indices
        ]

        if not candidate_indices:
            missing_cycles.append(cycle)
            missing_counts[cycle.experiment_id] += 1
            if candidate_count_in_window == 0:
                missing_no_window_counts[cycle.experiment_id] += 1
            else:
                missing_all_used_counts[cycle.experiment_id] += 1
            if logger.isEnabledFor(logging.DEBUG):
                nearest_indices: list[int] = []
                if 0 <= start_idx < len(signal_files):
                    nearest_indices.append(start_idx)
                if start_idx - 1 >= 0:
                    nearest_indices.append(start_idx - 1)
                nearest_indices = list(dict.fromkeys(nearest_indices))
                nearest_text = "none"
                if nearest_indices:
                    nearest_index = min(
                        nearest_indices,
                        key=lambda index: abs((signal_files[index].timestamp - cycle.t_done).total_seconds()),
                    )
                    nearest_signal = signal_files[nearest_index]
                    nearest_text = (
                        f"{signal_relative_path(nearest_signal.path, signal_dir)} "
                        f"ts={nearest_signal.timestamp.isoformat(sep=' ')} "
                        f"delta_to_done_s={abs((nearest_signal.timestamp - cycle.t_done).total_seconds()):.3f}"
                    )
                reason = (
                    "no signal file timestamp in tolerance window"
                    if candidate_count_in_window == 0
                    else f"{candidate_count_in_window} signal file(s) in tolerance window but already used"
                )
                logger.debug(
                    "Unmatched acoustic cycle %s window=[%s, %s] reason=%s nearest_signal=%s",
                    format_cycle_debug(cycle),
                    window_start.isoformat(sep=" "),
                    window_end.isoformat(sep=" "),
                    reason,
                    nearest_text,
                )
            continue

        if len(candidate_indices) > 1:
            ambiguous_candidate_counts[cycle.experiment_id] += 1
        best_index = min(
            candidate_indices,
            key=lambda index: candidate_score(signal_files[index], cycle),
        )
        used_indices.add(best_index)
        signal_file = signal_files[best_index]
        matched_counts[cycle.experiment_id] += 1
        matched_cycles.append(
            MatchedCycle(
                cycle=cycle,
                position=None,
                signal_file=signal_file,
                signal_path=signal_relative_path(signal_file.path, signal_dir),
            )
        )

    logger.info(
        "Matched %d acoustic cycles to signal files (%d unmatched cycles, %d unused signal files)",
        len(matched_cycles),
        len(missing_cycles),
        len(signal_files) - len(used_indices),
    )
    logger.info("Matched acoustic cycles by experiment: %s", format_experiment_counts(matched_counts))
    if ambiguous_candidate_counts:
        logger.info(
            "Cycles with multiple candidate signal files in the tolerance window by experiment: %s",
            format_experiment_counts(ambiguous_candidate_counts),
        )
    if missing_counts:
        logger.warning(
            "Unmatched acoustic cycles by experiment: %s",
            format_experiment_counts(missing_counts),
        )
    if missing_no_window_counts:
        logger.warning(
            "Unmatched acoustic cycles with no signal timestamp inside the tolerance window by experiment: %s",
            format_experiment_counts(missing_no_window_counts),
        )
    if missing_all_used_counts:
        logger.warning(
            "Unmatched acoustic cycles where all candidate signal files were already assigned by experiment: %s",
            format_experiment_counts(missing_all_used_counts),
        )
    return matched_cycles, missing_cycles


def parse_signal_file(path: Path, signal_dtype: np.dtype[Any]) -> ParsedSignalFile:
    sweep_duration = math.nan
    f_start = math.nan
    f_stop = math.nan
    chirp_amp = math.nan
    chirp_excitation: np.ndarray | None = None
    microphones: list[dict[str, Any]] = []

    with path.open(
        "r",
        encoding="utf-8",
        errors="replace",
        newline="",
        buffering=SIGNAL_FILE_BUFFER_SIZE,
    ) as handle:
        header = next(handle, None)
        if header is None:
            raise ValueError(f"Signal CSV is empty: {path}")

        for line_number, raw_line in enumerate(handle, start=2):
            line = raw_line.rstrip("\r\n")
            if not line:
                continue

            try:
                (
                    duration_raw,
                    f_start_raw,
                    f_stop_raw,
                    chirp_amp_raw,
                    microphone_coordinates,
                    label,
                    values_raw,
                ) = split_signal_csv_line(line)
            except ValueError as exc:
                raise ValueError(f"Malformed signal CSV row {line_number} in {path}: {exc}") from exc

            if (
                "unused" in microphone_coordinates.casefold()
                or "unused" in label.casefold()
                or values_raw == "unused"
            ):
                continue

            if math.isnan(sweep_duration):
                sweep_duration = parse_optional_float(duration_raw)
                f_start = parse_optional_float(f_start_raw)
                f_stop = parse_optional_float(f_stop_raw)
                chirp_amp = parse_optional_float(chirp_amp_raw)

            values = parse_signal_vector(values_raw, signal_dtype)

            if label == "chirp_excitation":
                chirp_excitation = values
                continue

            if values is None:
                continue

            microphones.append(
                {
                    "microphone_label": label,
                    "microphone_coordinates": parse_microphone_coordinates(microphone_coordinates),
                    "values": values,
                }
            )

    if math.isnan(sweep_duration):
        raise ValueError(f"Signal CSV has no readable rows: {path}")

    return ParsedSignalFile(
        sweep_duration=float(sweep_duration),
        f_start=float(f_start),
        f_stop=float(f_stop),
        chirp_amp=float(chirp_amp),
        chirp_excitation=chirp_excitation,
        microphones=microphones,
    )


def parse_matched_cycle(
    matched_cycle: MatchedCycle,
    signal_dtype_name: str,
) -> tuple[dict[str, Any], dict[str, Any] | None, list[dict[str, Any]]]:
    signal_dtype = np.dtype(signal_dtype_name)
    parsed_signal = parse_signal_file(matched_cycle.signal_file.path, signal_dtype)
    cycle = matched_cycle.cycle

    cycle_row = {
        "experiment_id": cycle.experiment_id,
        "cycle_id": cycle.cycle_id,
    }

    chirp_row: dict[str, Any] | None = None
    if parsed_signal.chirp_excitation is not None:
        chirp_row = {
            "duration": float(parsed_signal.sweep_duration),
            "f_start": float(parsed_signal.f_start),
            "f_stop": float(parsed_signal.f_stop),
            "chirp_amp": float(parsed_signal.chirp_amp),
            "chirp_sample_count": int(parsed_signal.chirp_excitation.size),
            "chirp_excitation": parsed_signal.chirp_excitation.astype(signal_dtype, copy=False).tolist(),
        }

    response_rows = [
        {
            "experiment_id": cycle.experiment_id,
            "cycle_id": cycle.cycle_id,
            "microphone_label": microphone["microphone_label"],
            "microphone_coordinates": microphone.get("microphone_coordinates"),
            "values": microphone["values"].astype(signal_dtype, copy=False),
        }
        for microphone in parsed_signal.microphones
    ]

    return cycle_row, chirp_row, response_rows


def build_final_output_paths(output_dir: Path) -> dict[str, Path]:
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    return {
        "cycles": output_dir / "acoustic_cycles.parquet",
        "responses": output_dir / "acoustic_responses.parquet",
        "chirps": output_dir / "acoustic_chirps.parquet",
    }


def build_netcdf_output_path(output_dir: Path, experiment_ids: Iterable[str] | None) -> Path:
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    slug = experiment_slug(experiment_ids)
    stem = f"acoustic_{slug}" if slug else "acoustic"
    return output_dir / f"{stem}.nc"


def build_batch_workspace(output_dir: Path) -> dict[str, Path]:
    output_dir = output_dir.expanduser().resolve()
    batch_root = output_dir / BATCH_ROOT_NAME
    return {
        "output_dir": output_dir,
        "root": batch_root,
        "manifests": batch_root / BATCH_MANIFESTS_NAME,
        "cycles": batch_root / "cycles",
        "responses": batch_root / "responses",
        "chirps": batch_root / "chirps",
    }


def ensure_batch_workspace(workspace: dict[str, Path]) -> None:
    workspace["output_dir"].mkdir(parents=True, exist_ok=True)
    workspace["root"].mkdir(parents=True, exist_ok=True)
    workspace["manifests"].mkdir(parents=True, exist_ok=True)
    for table_name in TABLE_NAMES:
        workspace[table_name].mkdir(parents=True, exist_ok=True)


def batch_file_paths(workspace: dict[str, Path], batch_id: int) -> dict[str, Path]:
    stem = f"batch_{batch_id:06d}"
    return {
        "cycles": workspace["cycles"] / f"{stem}_cycles.parquet",
        "responses": workspace["responses"] / f"{stem}_responses.parquet",
        "chirps": workspace["chirps"] / f"{stem}_chirps.parquet",
        "manifest": workspace["manifests"] / f"{stem}.json",
    }


def make_temp_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.tmp")


def relative_to_output_dir(path: Path, output_dir: Path) -> str:
    return str(path.resolve().relative_to(output_dir.resolve()))


def load_completed_batches(output_dir: Path) -> tuple[list[CompletedBatch], set[tuple[str, int]], int]:
    output_dir = output_dir.expanduser().resolve()
    workspace = build_batch_workspace(output_dir)
    completed_batches: list[CompletedBatch] = []
    processed_cycle_keys: set[tuple[str, int]] = set()
    duplicate_cycle_count = 0
    max_batch_id = 0

    if not workspace["manifests"].exists():
        return completed_batches, processed_cycle_keys, 1

    for manifest_path in sorted(workspace["manifests"].glob("batch_*.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Skipping unreadable batch manifest %s: %s", manifest_path, exc)
            continue

        try:
            batch_id = int(manifest["batch_id"])
        except (KeyError, TypeError, ValueError):
            logger.warning("Skipping invalid batch manifest %s", manifest_path)
            continue

        try:
            file_paths = {
                table_name: output_dir / str(manifest["table_files"][table_name])
                for table_name in TABLE_NAMES
            }
        except KeyError:
            logger.warning("Skipping batch manifest with missing table file entries: %s", manifest_path)
            continue
        if any(not path.exists() for path in file_paths.values()):
            logger.warning(
                "Skipping batch %s because one or more parquet files are missing",
                manifest_path,
            )
            continue

        cycle_keys: list[tuple[str, int]] = []
        for entry in manifest.get("cycle_entries", []):
            try:
                key = (str(entry["experiment_id"]), int(entry["cycle_id"]))
            except (KeyError, TypeError, ValueError):
                logger.warning("Skipping invalid cycle entry in %s", manifest_path)
                cycle_keys = []
                break
            cycle_keys.append(key)

        if not cycle_keys and int(manifest.get("cycle_count", 0)) > 0:
            logger.warning("Skipping batch manifest without valid cycle entries: %s", manifest_path)
            continue

        for key in cycle_keys:
            if key in processed_cycle_keys:
                duplicate_cycle_count += 1
            processed_cycle_keys.add(key)

        completed_batches.append(
            CompletedBatch(
                batch_id=batch_id,
                manifest_path=manifest_path,
                files=file_paths,
                cycle_keys=cycle_keys,
                cycle_count=int(manifest.get("cycle_count", len(cycle_keys))),
                response_count=int(manifest.get("response_count", 0)),
                chirp_count=int(manifest.get("chirp_count", 0)),
            )
        )
        max_batch_id = max(max_batch_id, batch_id)

    completed_batches.sort(key=lambda batch: batch.batch_id)
    if duplicate_cycle_count:
        logger.warning(
            "Detected %d duplicate cycle keys across completed batches; merged outputs may contain duplicates",
            duplicate_cycle_count,
        )
    return completed_batches, processed_cycle_keys, max_batch_id + 1


class AcousticParquetLayout:
    def __init__(self, signal_dtype_name: str):
        pa, pq = require_pyarrow()
        self._pa = pa
        self._pq = pq
        self.signal_dtype_name = signal_dtype_name
        self.schemas = {
            "cycles": self.cycle_schema(pa),
            "responses": self.response_schema(pa, signal_dtype_name),
            "chirps": self.chirp_schema(pa, signal_dtype_name),
        }

    @staticmethod
    def cycle_schema(pa):
        return pa.schema(
            [
                ("experiment_id", pa.string()),
                ("cycle_id", pa.int64()),
            ]
        )

    @staticmethod
    def response_schema(pa, signal_dtype_name: str):
        value_type = pa.float64() if signal_dtype_name == "float64" else pa.float32()
        return pa.schema(
            [
                ("experiment_id", pa.string()),
                ("cycle_id", pa.int64()),
                ("microphone_label", pa.string()),
                ("values", pa.list_(value_type)),
            ]
        )

    @staticmethod
    def chirp_schema(pa, signal_dtype_name: str):
        value_type = pa.float64() if signal_dtype_name == "float64" else pa.float32()
        return pa.schema(
            [
                ("duration", pa.float32()),
                ("f_start", pa.float32()),
                ("f_stop", pa.float32()),
                ("chirp_amp", pa.float32()),
                ("chirp_sample_count", pa.int32()),
                ("chirp_excitation", pa.list_(value_type)),
            ]
        )

    def write_rows(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        path: Path,
        *,
        compression: str | None,
    ) -> None:
        table = self._pa.Table.from_pylist(rows, schema=self.schemas[table_name])
        self._pq.write_table(table, path, compression=compression)


def fixed_chirp_rows_equal(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    atol: float = 1e-6,
) -> bool:
    scalar_fields = ("duration", "f_start", "f_stop", "chirp_amp")
    for field_name in scalar_fields:
        left_value = parse_optional_float(left.get(field_name), default=math.nan)
        right_value = parse_optional_float(right.get(field_name), default=math.nan)
        if math.isnan(left_value) and math.isnan(right_value):
            continue
        if not math.isclose(left_value, right_value, abs_tol=atol, rel_tol=0.0):
            return False

    if int(left.get("chirp_sample_count") or 0) != int(right.get("chirp_sample_count") or 0):
        return False

    left_excitation = np.asarray(left.get("chirp_excitation") or [], dtype=np.float64)
    right_excitation = np.asarray(right.get("chirp_excitation") or [], dtype=np.float64)
    if left_excitation.shape != right_excitation.shape:
        return False
    if left_excitation.size == 0 and right_excitation.size == 0:
        return True
    return bool(np.allclose(left_excitation, right_excitation, atol=atol, rtol=0.0))


def canonicalize_fixed_chirp_rows(chirp_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical_row: dict[str, Any] | None = None
    for row in chirp_rows:
        if canonical_row is None:
            canonical_row = {
                "duration": parse_optional_float(row.get("duration"), default=math.nan),
                "f_start": parse_optional_float(row.get("f_start"), default=math.nan),
                "f_stop": parse_optional_float(row.get("f_stop"), default=math.nan),
                "chirp_amp": parse_optional_float(row.get("chirp_amp"), default=math.nan),
                "chirp_sample_count": int(row.get("chirp_sample_count") or 0),
                "chirp_excitation": np.asarray(
                    row.get("chirp_excitation") or [],
                    dtype=np.float64,
                ).reshape(-1).tolist(),
            }
            continue

        if not fixed_chirp_rows_equal(canonical_row, row):
            raise RuntimeError("Detected inconsistent fixed chirp metadata across acoustic files.")

    return [] if canonical_row is None else [canonical_row]


def write_completed_batch(
    layout: AcousticParquetLayout,
    workspace: dict[str, Path],
    batch_id: int,
    cycle_rows: list[dict[str, Any]],
    response_rows: list[dict[str, Any]],
    chirp_rows: list[dict[str, Any]],
    *,
    compression: str | None,
) -> CompletedBatch:
    chirp_rows = canonicalize_fixed_chirp_rows(chirp_rows)
    paths = batch_file_paths(workspace, batch_id)
    temp_paths = {
        name: make_temp_path(path)
        for name, path in paths.items()
        if name != "manifest"
    }
    manifest_temp_path = make_temp_path(paths["manifest"])

    for path in list(temp_paths.values()) + [manifest_temp_path]:
        if path.exists():
            path.unlink()

    try:
        layout.write_rows("cycles", cycle_rows, temp_paths["cycles"], compression=compression)
        layout.write_rows("responses", response_rows, temp_paths["responses"], compression=compression)
        layout.write_rows("chirps", chirp_rows, temp_paths["chirps"], compression=compression)

        for table_name in TABLE_NAMES:
            temp_paths[table_name].replace(paths[table_name])

        manifest = {
            "batch_id": batch_id,
            "created_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "cycle_count": len(cycle_rows),
            "response_count": len(response_rows),
            "chirp_count": len(chirp_rows),
            "table_files": {
                table_name: relative_to_output_dir(paths[table_name], workspace["output_dir"])
                for table_name in TABLE_NAMES
            },
            "cycle_entries": [
                {
                    "experiment_id": str(row["experiment_id"]),
                    "cycle_id": int(row["cycle_id"]),
                }
                for row in cycle_rows
            ],
        }
        manifest_temp_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        manifest_temp_path.replace(paths["manifest"])
    finally:
        for path in list(temp_paths.values()) + [manifest_temp_path]:
            if path.exists():
                path.unlink()

    return CompletedBatch(
        batch_id=batch_id,
        manifest_path=paths["manifest"],
        files={table_name: paths[table_name] for table_name in TABLE_NAMES},
        cycle_keys=[
            (str(row["experiment_id"]), int(row["cycle_id"]))
            for row in cycle_rows
        ],
        cycle_count=len(cycle_rows),
        response_count=len(response_rows),
        chirp_count=len(chirp_rows),
    )


def merge_completed_batches(
    layout: AcousticParquetLayout,
    completed_batches: list[CompletedBatch],
    output_dir: Path,
    *,
    compression: str | None,
) -> dict[str, Path]:
    if not completed_batches:
        raise RuntimeError("No completed acoustic batches are available to merge.")

    output_paths = build_final_output_paths(output_dir)
    temp_paths = {
        table_name: make_temp_path(path)
        for table_name, path in output_paths.items()
    }
    writers: dict[str, Any] = {}
    merged_chirp_rows: list[dict[str, Any]] = []

    for path in temp_paths.values():
        if path.exists():
            path.unlink()

    try:
        for table_name in ("cycles", "responses"):
            writers[table_name] = layout._pq.ParquetWriter(
                temp_paths[table_name],
                layout.schemas[table_name],
                compression=compression,
            )

        for batch in completed_batches:
            for table_name in ("cycles", "responses"):
                table = layout._pq.read_table(batch.files[table_name])
                writers[table_name].write_table(table)
            merged_chirp_rows.extend(layout._pq.read_table(batch.files["chirps"]).to_pylist())

        layout.write_rows(
            "chirps",
            canonicalize_fixed_chirp_rows(merged_chirp_rows),
            temp_paths["chirps"],
            compression=compression,
        )

        for writer in writers.values():
            writer.close()
        writers.clear()

        for table_name in TABLE_NAMES:
            temp_paths[table_name].replace(output_paths[table_name])
    finally:
        for writer in writers.values():
            writer.close()
        for path in temp_paths.values():
            if path.exists():
                path.unlink()

    return output_paths


def write_dataset(
    dataset: Any,
    output_file: Path,
    *,
    hdf5_compression_level: int = DEFAULT_HDF5_COMPRESSION_LEVEL,
) -> Path:
    output_file = output_file.expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_target = output_file
    if write_target.exists():
        logger.info("Acoustic NetCDF already exists, skipping write: %s", write_target)
        return write_target

    engine = resolve_netcdf_engine()
    encoding = build_netcdf_encoding(
        dataset,
        engine=engine,
        hdf5_compression_level=hdf5_compression_level,
    )
    if engine == "scipy":
        oversized_variables: list[tuple[str, int]] = []
        for variable_name, variable in dataset.data_vars.items():
            data = getattr(variable, "data", None)
            if data is None:
                continue
            nbytes = int(getattr(data, "nbytes", 0) or 0)
            if nbytes > NETCDF3_MAX_VARIABLE_BYTES:
                oversized_variables.append((str(variable_name), nbytes))

        if oversized_variables:
            largest_name, largest_nbytes = max(oversized_variables, key=lambda item: item[1])
            install_hint = (
                f"`{sys.executable} -m pip install h5netcdf h5py` "
                f"or `{sys.executable} -m pip install netCDF4`"
            )
            raise RuntimeError(
                "The available NetCDF backend is `scipy`, which writes NetCDF3 files. "
                f"Variable `{largest_name}` is {format_gibibytes(largest_nbytes)}, exceeding the "
                f"NetCDF3 per-variable limit of {format_gibibytes(NETCDF3_MAX_VARIABLE_BYTES)}. "
                f"Install an HDF5-capable backend in the same interpreter, for example {install_hint}."
            )
        if hdf5_compression_level > 0:
            logger.warning(
                "HDF5 compression level %d requested, but engine=scipy does not support compressed NetCDF output.",
                hdf5_compression_level,
            )

    compression_text = (
        f"gzip level {hdf5_compression_level}"
        if encoding is not None
        else "none"
    )
    logger.info(
        "Writing acoustic NetCDF dataset to %s (engine=%s, compression=%s)",
        write_target,
        engine,
        compression_text,
    )
    dataset.to_netcdf(str(write_target), engine=engine, mode="w", encoding=encoding)
    return write_target


def ordered_present_experiment_ids(
    cycle_rows: Iterable[dict[str, Any]],
    preferred_experiment_ids: Iterable[str] | None = None,
) -> list[str]:
    present_ids = list(
        dict.fromkeys(
            str(row["experiment_id"]).strip()
            for row in cycle_rows
            if str(row.get("experiment_id", "")).strip()
        )
    )
    if preferred_experiment_ids is None:
        return present_ids

    preferred = [str(experiment_id).strip() for experiment_id in preferred_experiment_ids if str(experiment_id).strip()]
    ordered = [experiment_id for experiment_id in preferred if experiment_id in present_ids]
    ordered.extend(experiment_id for experiment_id in present_ids if experiment_id not in ordered)
    return ordered


def build_acoustic_xarray_dataset(
    cycle_rows: list[dict[str, Any]],
    response_rows: list[dict[str, Any]],
    chirp_rows: list[dict[str, Any]],
    *,
    signal_dtype_name: str,
    preferred_experiment_ids: Iterable[str] | None = None,
):
    if not cycle_rows:
        raise RuntimeError("Cannot build an acoustic xarray dataset without cycle rows.")

    xr = require_xarray()
    signal_dtype = np.float64 if signal_dtype_name == "float64" else np.float32
    experiment_id_rows = chain(cycle_rows, response_rows)
    experiment_ids = ordered_present_experiment_ids(experiment_id_rows, preferred_experiment_ids)
    cycle_ids = sorted(
        {
            int(row["cycle_id"])
            for row in chain(cycle_rows, response_rows)
            if row.get("cycle_id") is not None
        }
    )
    microphone_labels = list(
        dict.fromkeys(
            str(row.get("microphone_label") or "").strip()
            for row in response_rows
            if str(row.get("microphone_label") or "").strip()
        )
    )

    exp_index = {experiment_id: index for index, experiment_id in enumerate(experiment_ids)}
    cycle_index = {cycle_id: index for index, cycle_id in enumerate(cycle_ids)}
    microphone_index = {
        microphone_label: index
        for index, microphone_label in enumerate(microphone_labels)
    }
    microphone_positions = np.full((len(microphone_labels), 3), np.nan, dtype=np.float64)
    microphone_position_written = np.zeros(len(microphone_labels), dtype=bool)

    max_sample_count = max(
        (value_sequence_length(row.get("values")) for row in response_rows),
        default=0,
    )

    cycle_shape = (len(experiment_ids), len(cycle_ids))
    response_shape = cycle_shape + (len(microphone_labels),)
    waveform_shape = response_shape + (max_sample_count,)
    response_written = np.zeros(response_shape, dtype=bool)
    values = np.full(waveform_shape, np.nan, dtype=signal_dtype)

    duplicate_response_count = 0
    microphone_position_conflict_count = 0
    for row in response_rows:
        experiment_id = str(row["experiment_id"])
        cycle_id = int(row["cycle_id"])
        microphone_label = str(row.get("microphone_label") or "").strip()
        if microphone_label not in microphone_index:
            continue

        exp_idx = exp_index[experiment_id]
        cyc_idx = cycle_index[cycle_id]
        mic_idx = microphone_index[microphone_label]
        microphone_coordinates = row.get("microphone_coordinates")
        if microphone_coordinates is not None:
            coordinate_vector = value_sequence_as_array(microphone_coordinates, np.dtype(np.float64))
            if coordinate_vector.size == 3 and np.all(np.isfinite(coordinate_vector)):
                if microphone_position_written[mic_idx]:
                    if not np.allclose(
                        microphone_positions[mic_idx],
                        coordinate_vector,
                        rtol=0.0,
                        atol=1e-9,
                        equal_nan=True,
                    ):
                        microphone_position_conflict_count += 1
                else:
                    microphone_positions[mic_idx] = coordinate_vector
                    microphone_position_written[mic_idx] = True
        if response_written[exp_idx, cyc_idx, mic_idx]:
            duplicate_response_count += 1

        waveform = value_sequence_as_array(row.get("values"), signal_dtype)
        actual_sample_count = min(waveform.size, max_sample_count)
        response_written[exp_idx, cyc_idx, mic_idx] = True
        if actual_sample_count > 0:
            values[exp_idx, cyc_idx, mic_idx, :actual_sample_count] = waveform[:actual_sample_count]

    logger.info(
        "Building acoustic xarray dataset: experiments=%d cycles=%d microphones=%d sample_index=%d",
        len(experiment_ids),
        len(cycle_ids),
        len(microphone_labels),
        max_sample_count,
    )

    dataset_attrs = {
        "description": "Acoustic response waveforms indexed by experiment_id, cycle_id, and microphone_label.",
        "values_definition": (
            "Microphone response waveforms stored as `values`; trailing samples are NaN padded "
            "for missing samples or missing microphone responses."
        ),
        "signal_dtype": signal_dtype_name,
        "cycle_row_count": len(cycle_rows),
        "response_row_count": len(response_rows),
        "chirp_row_count": len(chirp_rows),
        "duplicate_response_count": duplicate_response_count,
        "microphone_position_count": int(microphone_position_written.sum()),
        "microphone_position_conflict_count": microphone_position_conflict_count,
        "microphone_position_definition": (
            "Fixed microphone coordinates stored once per microphone_label as "
            "microphone_x, microphone_y, and microphone_z."
        ),
        "sample_dimension_name": "sample_index",
    }

    coords = {
        "experiment_id": np.asarray(experiment_ids, dtype=str),
        "cycle_id": np.asarray(cycle_ids, dtype=np.int64),
        "microphone_label": np.asarray(microphone_labels, dtype=str),
    }
    data_vars = {
        "values": (
            ("experiment_id", "cycle_id", "microphone_label", "sample_index"),
            values,
        ),
        "microphone_x": (("microphone_label",), microphone_positions[:, 0]),
        "microphone_y": (("microphone_label",), microphone_positions[:, 1]),
        "microphone_z": (("microphone_label",), microphone_positions[:, 2]),
    }

    dataset = xr.Dataset(data_vars=data_vars, coords=coords, attrs=dataset_attrs)
    return dataset, experiment_ids


def write_acoustic_datasets_per_experiment(
    cycle_rows: list[dict[str, Any]],
    response_rows: list[dict[str, Any]],
    chirp_rows: list[dict[str, Any]],
    output_dir: Path,
    *,
    signal_dtype_name: str,
    hdf5_compression_level: int = DEFAULT_HDF5_COMPRESSION_LEVEL,
    preferred_experiment_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    experiment_ids = ordered_present_experiment_ids(
        chain(cycle_rows, response_rows),
        preferred_experiment_ids,
    )
    if not experiment_ids:
        raise RuntimeError("No experiment_id values are available for acoustic NetCDF export.")

    cycle_rows_by_experiment: dict[str, list[dict[str, Any]]] = {
        experiment_id: []
        for experiment_id in experiment_ids
    }
    response_rows_by_experiment: dict[str, list[dict[str, Any]]] = {
        experiment_id: []
        for experiment_id in experiment_ids
    }

    for row in cycle_rows:
        experiment_id = str(row.get("experiment_id") or "").strip()
        if experiment_id in cycle_rows_by_experiment:
            cycle_rows_by_experiment[experiment_id].append(row)

    for row in response_rows:
        experiment_id = str(row.get("experiment_id") or "").strip()
        if experiment_id in response_rows_by_experiment:
            response_rows_by_experiment[experiment_id].append(row)

    dataset_paths: list[Path] = []
    written_dataset_paths: list[Path] = []
    skipped_dataset_paths: list[Path] = []

    for experiment_id in experiment_ids:
        experiment_cycle_rows = cycle_rows_by_experiment.get(experiment_id, [])
        if not experiment_cycle_rows:
            logger.warning(
                "Skipping acoustic dataset build for %s because no cycle rows were collected for this experiment.",
                experiment_id,
            )
            continue

        output_path = build_netcdf_output_path(output_dir, [experiment_id])
        if output_path.exists():
            logger.info(
                "Acoustic export for %s already exists, skipping dataset build: %s",
                experiment_id,
                output_path,
            )
            dataset_paths.append(output_path)
            skipped_dataset_paths.append(output_path)
            continue

        experiment_response_rows = response_rows_by_experiment.get(experiment_id, [])
        microphone_count = len(
            {
                str(row.get("microphone_label") or "").strip()
                for row in experiment_response_rows
                if str(row.get("microphone_label") or "").strip()
            }
        )
        logger.info(
            "Preparing acoustic dataset for %s: cycles=%d response_rows=%d microphones=%d output=%s",
            experiment_id,
            len(experiment_cycle_rows),
            len(experiment_response_rows),
            microphone_count,
            output_path,
        )
        if not experiment_response_rows:
            logger.warning("Experiment %s has cycle rows but no parsed response rows.", experiment_id)
        dataset, _ = build_acoustic_xarray_dataset(
            experiment_cycle_rows,
            experiment_response_rows,
            chirp_rows,
            signal_dtype_name=signal_dtype_name,
            preferred_experiment_ids=[experiment_id],
        )
        written_output_path = write_dataset(
            dataset,
            output_path,
            hdf5_compression_level=hdf5_compression_level,
        )
        logger.info("Wrote acoustic dataset for %s: %s", experiment_id, written_output_path)
        dataset_paths.append(written_output_path)
        written_dataset_paths.append(written_output_path)

    return {
        "dataset_paths": dataset_paths,
        "written_dataset_paths": written_dataset_paths,
        "skipped_dataset_paths": skipped_dataset_paths,
        "exported_experiment_ids": experiment_ids,
    }


def export_acoustic_xarray(
    merged_output_paths: dict[str, Path],
    output_dir: Path,
    *,
    signal_dtype_name: str,
    hdf5_compression_level: int = DEFAULT_HDF5_COMPRESSION_LEVEL,
    preferred_experiment_ids: Iterable[str] | None = None,
) -> list[Path]:
    _, pq = require_pyarrow()
    cycle_rows = pq.read_table(merged_output_paths["cycles"]).to_pylist()
    response_rows = pq.read_table(merged_output_paths["responses"]).to_pylist()
    chirp_rows = pq.read_table(merged_output_paths["chirps"]).to_pylist()
    export_summary = write_acoustic_datasets_per_experiment(
        cycle_rows,
        response_rows,
        chirp_rows,
        output_dir,
        signal_dtype_name=signal_dtype_name,
        hdf5_compression_level=hdf5_compression_level,
        preferred_experiment_ids=preferred_experiment_ids,
    )
    return export_summary["dataset_paths"]


def export_acoustic_tables(args: argparse.Namespace) -> dict[str, Any]:
    ensure_csv_field_limit()
    experiment_ids = get_configured_experiment_ids()
    log_path, signal_dir = resolve_runtime_inputs(
        args.log_path,
        args.signal_dir,
        recursive_signal_search=args.recursive_signal_search,
    )
    logger.info("Resolved acoustic log path: %s", log_path)
    logger.info("Resolved signal directory: %s", signal_dir)
    output_dir = args.output_dir.expanduser().resolve()
    pending_experiment_ids = experiment_ids
    skipped_dataset_paths: list[Path] = []
    skipped_experiment_ids: list[str] = []
    if experiment_ids is not None:
        pending_experiment_ids = []
        for experiment_id in experiment_ids:
            expected_output_path = build_netcdf_output_path(output_dir, [experiment_id])
            if expected_output_path.exists():
                logger.info(
                    "Acoustic export for %s already exists, skipping parse: %s",
                    experiment_id,
                    expected_output_path,
                )
                skipped_dataset_paths.append(expected_output_path)
                skipped_experiment_ids.append(experiment_id)
            else:
                pending_experiment_ids.append(experiment_id)

        logger.info(
            "Experiments pending acoustic parse after existing-output check: %s",
            ", ".join(pending_experiment_ids) if pending_experiment_ids else "(none)",
        )
        if skipped_experiment_ids:
            logger.info(
                "Experiments skipped because acoustic output already exists: %s",
                ", ".join(skipped_experiment_ids),
            )

        if not pending_experiment_ids:
            return {
                "cycle_count": 0,
                "response_count": 0,
                "chirp_count": 0,
                "matched_cycle_count": 0,
                "pending_cycle_count": 0,
                "already_processed_cycle_count": 0,
                "missing_cycle_count": 0,
                "missing_position_count": 0,
                "new_batch_count": 0,
                "completed_batch_count": 0,
                "batch_workspace": None,
                "merged_output_paths": {"datasets": skipped_dataset_paths},
                "dataset_path": skipped_dataset_paths[0] if len(skipped_dataset_paths) == 1 else None,
                "dataset_paths": skipped_dataset_paths,
                "written_dataset_paths": [],
                "skipped_dataset_paths": skipped_dataset_paths,
                "skipped_existing_output": True,
            }

    cycles = parse_acoustic_log(log_path, experiment_ids=pending_experiment_ids)
    signal_files = index_signal_files(signal_dir, recursive=args.recursive_signal_search)
    tolerance = timedelta(seconds=float(args.timestamp_tolerance_s))
    matched_cycles, missing_cycles = match_cycles_to_signal_files(
        cycles,
        signal_files,
        signal_dir,
        tolerance,
    )

    if not matched_cycles:
        raise RuntimeError("No acoustic cycles could be matched to signal files.")

    if args.max_cycles is not None:
        if len(matched_cycles) > args.max_cycles:
            logger.warning(
                "--max-cycles=%d truncates the matched acoustic cycle queue from %d to %d. "
                "Matched cycles before truncation by experiment: %s",
                args.max_cycles,
                len(matched_cycles),
                args.max_cycles,
                format_experiment_counts(
                    count_experiment_ids(matched_cycle.cycle.experiment_id for matched_cycle in matched_cycles)
                ),
            )
        matched_cycles = matched_cycles[: args.max_cycles]
        logger.info(
            "Matched acoustic cycles after truncation by experiment: %s",
            format_experiment_counts(
                count_experiment_ids(matched_cycle.cycle.experiment_id for matched_cycle in matched_cycles)
            ),
        )

    logger.info("Queued %d acoustic files for parsing", len(matched_cycles))

    cycle_rows: list[dict[str, Any]] = []
    response_rows: list[dict[str, Any]] = []
    chirp_rows: list[dict[str, Any]] = []
    cycle_count = 0
    response_count = 0
    chirp_count = 0
    executor: ThreadPoolExecutor | ProcessPoolExecutor | None = None
    progress = ProgressTracker(
        len(matched_cycles),
        enabled=args.show_progress,
        description="Processing acoustic files",
        unit="file",
    )

    worker = partial(parse_matched_cycle, signal_dtype_name=args.signal_dtype)
    iterator: Iterable[tuple[dict[str, Any], dict[str, Any] | None, list[dict[str, Any]]]]
    if args.workers == 1:
        iterator = map(worker, matched_cycles)
    elif args.parallelism == "processes":
        executor = ProcessPoolExecutor(max_workers=args.workers)
        iterator = executor.map(worker, matched_cycles, chunksize=1)
    else:
        executor = ThreadPoolExecutor(max_workers=args.workers)
        iterator = executor.map(worker, matched_cycles)

    try:
        for cycle_count, (cycle_row, chirp_row, parsed_response_rows) in enumerate(iterator, start=1):
            cycle_rows.append(cycle_row)
            response_rows.extend(parsed_response_rows)
            response_count += len(parsed_response_rows)
            if chirp_row is not None:
                chirp_rows.append(chirp_row)
                chirp_count += 1
            progress.update(1)
    finally:
        if executor is not None:
            executor.shutdown(wait=True)
        progress.close()

    logger.info(
        "Parsed acoustic cycle rows by experiment: %s",
        format_experiment_counts(count_experiment_ids(row["experiment_id"] for row in cycle_rows)),
    )
    logger.info(
        "Parsed acoustic response rows by experiment: %s",
        format_experiment_counts(count_experiment_ids(row["experiment_id"] for row in response_rows)),
    )
    chirp_rows = canonicalize_fixed_chirp_rows(chirp_rows)
    export_summary = write_acoustic_datasets_per_experiment(
        cycle_rows,
        response_rows,
        chirp_rows,
        output_dir,
        signal_dtype_name=args.signal_dtype,
        hdf5_compression_level=args.hdf5_compression_level,
        preferred_experiment_ids=pending_experiment_ids,
    )
    dataset_paths = skipped_dataset_paths + export_summary["dataset_paths"]
    written_dataset_paths = export_summary["written_dataset_paths"]
    skipped_dataset_paths = skipped_dataset_paths + export_summary["skipped_dataset_paths"]

    return {
        "cycle_count": cycle_count,
        "response_count": response_count,
        "chirp_count": chirp_count,
        "matched_cycle_count": len(matched_cycles),
        "pending_cycle_count": len(matched_cycles),
        "already_processed_cycle_count": 0,
        "missing_cycle_count": len(missing_cycles),
        "missing_position_count": 0,
        "new_batch_count": 0,
        "completed_batch_count": 0,
        "batch_workspace": None,
        "merged_output_paths": {"datasets": dataset_paths},
        "dataset_path": dataset_paths[0] if len(dataset_paths) == 1 else None,
        "dataset_paths": dataset_paths,
        "written_dataset_paths": written_dataset_paths,
        "skipped_dataset_paths": skipped_dataset_paths,
        "skipped_existing_output": bool(skipped_dataset_paths),
    }


def format_size_mebibytes(path: Path) -> str:
    return f"{path.stat().st_size / (1024 * 1024):.2f} MiB"


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)
    experiment_ids = get_configured_experiment_ids()

    logger.info("Signal dtype: %s", args.signal_dtype)
    logger.info("HDF5 compression level: %d", args.hdf5_compression_level)
    logger.info(
        "Parallel parsing: backend=%s workers=%d",
        args.parallelism,
        args.workers,
    )
    if experiment_ids:
        logger.info("Experiment filter from EXPERIMENTS: %s", ", ".join(experiment_ids))
    else:
        logger.info("Experiment filter from EXPERIMENTS: all experiments")

    if args.merge_only:
        logger.warning(
            "--merge-only is ignored; the parser now writes NetCDF directly from source files."
        )
    if not args.merge_batches:
        logger.warning(
            "--skip-merge is ignored; the parser now writes NetCDF directly."
        )
    if (
        args.batch_cycles != DEFAULT_BATCH_CYCLES
        or args.batch_compression != "none"
        or args.final_compression != "snappy"
    ):
        logger.warning(
            "Parquet batch/compression options are ignored; no parquet files are written."
        )

    summary = export_acoustic_tables(args)
    skipped_dataset_paths = summary.get("skipped_dataset_paths") or []
    if skipped_dataset_paths:
        logger.info(
            "Skipped %d existing acoustic dataset(s).",
            len(skipped_dataset_paths),
        )
    logger.info(
        "Acoustic export complete: cycles=%d responses=%d chirps=%d "
        "(matched=%d unmatched=%d)",
        summary["cycle_count"],
        summary["response_count"],
        summary["chirp_count"],
        summary["matched_cycle_count"],
        summary["missing_cycle_count"],
    )
    dataset_paths = summary.get("dataset_paths")
    if not dataset_paths:
        dataset_path = summary.get("dataset_path")
        dataset_paths = [dataset_path] if dataset_path is not None else []
    for dataset_path in dataset_paths:
        if dataset_path is not None and dataset_path.exists():
            logger.info("dataset: %s (%s)", dataset_path, format_size_mebibytes(dataset_path))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
