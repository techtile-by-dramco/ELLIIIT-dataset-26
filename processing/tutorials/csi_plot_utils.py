"""Utility helpers for tutorial notebooks that inspect RF xarray datasets."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
import sys
from typing import Iterable, Sequence

import matplotlib.pyplot as plt
from matplotlib.animation import FFMpegWriter
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize
from matplotlib.patches import Rectangle
import numpy as np
import requests
import xarray as xr
import yaml

POSITIONS_URL = (
    "https://raw.githubusercontent.com/techtile-by-dramco/"
    "techtile-description/refs/heads/main/geometry/"
    "techtile_antenna_locations.yml"
)
PROCESSING_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROCESSING_ROOT.parent
DEFAULT_RESULTS_DIR = REPO_ROOT / "results"
DEFAULT_HEATMAP_MAX_CYCLE_VALUES = 100
DEFAULT_MOVIE_MAX_FRAMES = 250
DEFAULT_MOVIE_FPS = 10
DEFAULT_MOVIE_DPI = 120
POWER_DB_FLOOR = -120.0
ANTENNA_TILE_SIZE_M = 0.14
DATASET_TIMESTAMP_PATTERN = re.compile(r".*_(?P<timestamp>\d{8}_\d{6})(?:_\d{2})?\.nc$")

DIMENSION_DESCRIPTIONS = {
    "experiment_id": "One logical measurement run such as EXP003 or EXP005.",
    "cycle_id": "Shared orchestrator cycle axis across the dataset. Not every experiment uses every listed cycle.",
    "hostname": "One RF receiver host or tile.",
    "measurement_index": "A flat helper axis used only in tutorial tables after valid rows are merged.",
}

VARIABLE_DESCRIPTIONS = {
    "rover_x": "Rover X coordinate in meters for one experiment/cycle pair.",
    "rover_y": "Rover Y coordinate in meters for one experiment/cycle pair.",
    "rover_z": "Rover Z coordinate in meters for one experiment/cycle pair.",
    "position_available": "Boolean-like mask that marks whether the rover position is valid for that cycle.",
    "csi_real": "Real part of the cable-corrected complex CSI value.",
    "csi_imag": "Imaginary part of the cable-corrected complex CSI value.",
    "csi_available": "Boolean-like mask that marks whether a host contributed CSI for that experiment/cycle.",
    "csi_host_count": "Number of hosts with CSI in a selected cycle.",
    "experiment_id": "Experiment label carried into flattened tutorial tables.",
    "cycle_id": "Cycle label carried into flattened tutorial tables.",
}

plt.style.use("seaborn-v0_8-whitegrid")


def normalize_experiment_ids(experiment_id: str | Sequence[str]) -> list[str]:
    if isinstance(experiment_id, str):
        experiment_ids = [experiment_id]
    elif np.isscalar(experiment_id):
        experiment_ids = [str(experiment_id)]
    else:
        experiment_ids = [str(value) for value in experiment_id]
    if not experiment_ids:
        raise ValueError("At least one experiment ID is required.")
    return experiment_ids


def experiment_label(experiment_id: str | Sequence[str]) -> str:
    return ", ".join(normalize_experiment_ids(experiment_id))


def experiment_phrase(experiment_id: str | Sequence[str]) -> str:
    experiment_ids = normalize_experiment_ids(experiment_id)
    if len(experiment_ids) == 1:
        return f"experiment {experiment_ids[0]}"
    return f"experiments {', '.join(experiment_ids)}"


def experiment_slug(experiment_id: str | Sequence[str]) -> str:
    return "__".join(normalize_experiment_ids(experiment_id))


def default_dataset_search_dirs(search_dirs: Iterable[str | Path] | None = None) -> list[Path]:
    if search_dirs is None:
        search_dirs = [
            DEFAULT_RESULTS_DIR,
            Path.cwd() / "results",
            Path.cwd(),
            PROCESSING_ROOT,
            PROCESSING_ROOT / "tutorials",
            Path.cwd() / "tutorials",
            Path.cwd() / "processing",
            Path.cwd() / "processing" / "tutorials",
        ]

    resolved_dirs: list[Path] = []
    seen_dirs: set[Path] = set()
    for candidate in search_dirs:
        resolved_candidate = Path(candidate).resolve()
        if resolved_candidate in seen_dirs or not resolved_candidate.exists():
            continue
        resolved_dirs.append(resolved_candidate)
        seen_dirs.add(resolved_candidate)
    return resolved_dirs


def dataset_sort_key(path: str | Path) -> tuple[datetime, float, str]:
    path = Path(path)
    match = DATASET_TIMESTAMP_PATTERN.match(path.name)
    if match is not None:
        dataset_time = datetime.strptime(match.group("timestamp"), "%Y%m%d_%H%M%S")
    else:
        dataset_time = datetime.fromtimestamp(path.stat().st_mtime)
    return dataset_time, path.stat().st_mtime, path.name


def find_dataset_paths(
    experiment_id: str | Sequence[str] | None = None,
    dataset_glob: str | None = None,
    search_dirs: Iterable[str | Path] | None = None,
) -> list[Path]:
    search_dirs = default_dataset_search_dirs(search_dirs)
    if dataset_glob is not None:
        patterns = [dataset_glob]
    elif experiment_id is None:
        patterns = ["csi*.nc"]
    else:
        patterns = [f"csi_{experiment_slug(experiment_id)}*.nc"]

    candidate_paths: list[Path] = []
    seen_paths: set[Path] = set()
    for search_dir in search_dirs:
        for pattern in patterns:
            for candidate in search_dir.glob(pattern):
                resolved_candidate = candidate.resolve()
                if resolved_candidate in seen_paths:
                    continue
                candidate_paths.append(resolved_candidate)
                seen_paths.add(resolved_candidate)

    candidate_paths.sort(key=dataset_sort_key, reverse=True)
    return candidate_paths


def open_dataset(
    experiment_id: str | Sequence[str] | None = None,
    dataset_path: str | Path | None = None,
    dataset_glob: str | None = None,
    search_dirs: Iterable[str | Path] | None = None,
) -> tuple[xr.Dataset, Path]:
    if dataset_path is None:
        candidates = find_dataset_paths(
            experiment_id=experiment_id,
            dataset_glob=dataset_glob,
            search_dirs=search_dirs,
        )
        if not candidates:
            searched_locations = ", ".join(str(path) for path in default_dataset_search_dirs(search_dirs))
            if dataset_glob is not None:
                raise FileNotFoundError(
                    f"Could not find a dataset matching {dataset_glob!r} in: {searched_locations}"
                )
            if experiment_id is None:
                raise FileNotFoundError(f"Could not find a dataset matching csi*.nc in: {searched_locations}")
            raise FileNotFoundError(
                f"Could not find a dataset matching csi_{experiment_slug(experiment_id)}*.nc in: "
                f"{searched_locations}"
            )
        dataset_path = candidates[0]

    dataset_path = Path(dataset_path).resolve()
    ds = open_netcdf_dataset(dataset_path, label="RF dataset")

    if experiment_id is not None:
        available_experiments = set(ds["experiment_id"].values.astype(str).tolist())
        missing_experiments = [
            selected_experiment_id
            for selected_experiment_id in normalize_experiment_ids(experiment_id)
            if selected_experiment_id not in available_experiments
        ]
        if missing_experiments:
            ds.close()
            raise ValueError(
                f"Dataset {dataset_path} does not contain experiment IDs: {', '.join(missing_experiments)}"
            )

    return ds, dataset_path


def open_netcdf_dataset(dataset_path: str | Path, *, label: str = "NetCDF dataset") -> xr.Dataset:
    dataset_path = Path(dataset_path).resolve()
    try:
        return xr.open_dataset(dataset_path)
    except ValueError as exc:
        message = str(exc)
        if "xarray's IO backends" not in message:
            raise

        install_hint = (
            f"`{sys.executable} -m pip install h5netcdf h5py` "
            f"or `{sys.executable} -m pip install netCDF4`"
        )
        raise RuntimeError(
            f"Could not open {label} at {dataset_path} because this Jupyter/Python environment is missing "
            "an HDF5 NetCDF backend for xarray. Install one into the same interpreter, for example "
            f"{install_hint}."
        ) from exc


def load_antenna_positions(positions_url: str = POSITIONS_URL) -> dict[str, np.ndarray]:
    response = requests.get(positions_url, timeout=20)
    response.raise_for_status()
    config = yaml.safe_load(response.text)

    positions: dict[str, np.ndarray] = {}
    for entry in config["antennes"]:
        tile = str(entry["tile"]).upper()
        channel = next(
            (channel for channel in entry["channels"] if int(channel["ch"]) == 1),
            None,
        )
        if channel is None:
            continue
        positions[tile] = np.array([channel["x"], channel["y"], channel["z"]], dtype=float)
    return positions


def antenna_position_table(
    antenna_positions: dict[str, np.ndarray] | None = None,
    hostnames: Iterable[str] | None = None,
    positions_url: str = POSITIONS_URL,
) -> xr.Dataset:
    if antenna_positions is None:
        antenna_positions = load_antenna_positions(positions_url)

    if hostnames is None:
        selected_hostnames = ordered_hostnames(antenna_positions.keys(), antenna_positions)
    else:
        selected_hostnames = ordered_hostnames(hostnames, antenna_positions)
        selected_hostnames = [
            hostname for hostname in selected_hostnames if hostname.upper() in antenna_positions
        ]
    if selected_hostnames:
        xyz = np.asarray(
            [antenna_positions[hostname.upper()] for hostname in selected_hostnames],
            dtype=float,
        )
    else:
        xyz = np.empty((0, 3), dtype=float)
    return xr.Dataset(
        data_vars={
            "antenna_x": ("hostname", xyz[:, 0]),
            "antenna_y": ("hostname", xyz[:, 1]),
            "antenna_z": ("hostname", xyz[:, 2]),
        },
        coords={"hostname": np.asarray(selected_hostnames, dtype=str)},
        attrs={
            "positions_url": positions_url,
            "antenna_tile_size_m": ANTENNA_TILE_SIZE_M,
        },
    )


def hostname_sort_key(hostname: str, antenna_positions: dict[str, np.ndarray] | None = None) -> tuple:
    antenna_positions = antenna_positions or {}
    position = antenna_positions.get(str(hostname).upper())
    if position is None:
        return (1, np.inf, np.inf, np.inf, str(hostname))
    x, y, z = [float(value) for value in position]
    return (0, y, x, z, str(hostname))


def ordered_hostnames(
    hostnames: Iterable[str],
    antenna_positions: dict[str, np.ndarray] | None = None,
) -> list[str]:
    return sorted(
        [str(hostname) for hostname in hostnames],
        key=lambda hostname: hostname_sort_key(hostname, antenna_positions),
    )


def tick_positions(values: Sequence[object], max_ticks: int = 20) -> np.ndarray:
    values = np.asarray(values)
    if values.size <= max_ticks:
        return np.arange(values.size)
    return np.linspace(0, values.size - 1, max_ticks, dtype=int)


def preview_coord_values(values: Sequence[object], max_items: int = 6) -> str:
    values = np.asarray(values)
    if values.size == 0:
        return "(empty)"
    preview = ", ".join(str(value) for value in values[:max_items])
    if values.size <= max_items:
        return preview
    return f"{preview}, ... ({values.size} total)"


def markdown_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join("---" for _ in headers) + " |"
    body_rows = [
        "| " + " | ".join(str(value) for value in row) + " |"
        for row in rows
    ]
    return "\n".join([header_row, separator_row, *body_rows])


def available_experiment_ids(ds: xr.Dataset) -> list[str]:
    return ds["experiment_id"].values.astype(str).tolist()


def available_cycle_ids(ds: xr.Dataset, experiment_id: str) -> np.ndarray:
    experiment = ds.sel(experiment_id=experiment_id)
    cycle_mask = (experiment["csi_available"].values > 0).any(axis=1)
    return experiment["cycle_id"].values[cycle_mask].astype(int)


def active_hostnames(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None = None,
) -> list[str]:
    experiment_ids = available_experiment_ids(ds) if experiment_id is None else normalize_experiment_ids(experiment_id)
    selected_hostnames: set[str] = set()
    for selected_experiment_id in experiment_ids:
        experiment = ds.sel(experiment_id=selected_experiment_id)
        host_mask = (experiment["csi_available"].values > 0).any(axis=0)
        selected_hostnames.update(experiment["hostname"].values[host_mask].astype(str).tolist())
    return sorted(selected_hostnames)


def first_available_cycle_ids(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None = None,
) -> dict[str, int]:
    experiment_ids = available_experiment_ids(ds) if experiment_id is None else normalize_experiment_ids(experiment_id)
    first_cycles: dict[str, int] = {}
    for selected_experiment_id in experiment_ids:
        cycle_ids = available_cycle_ids(ds, selected_experiment_id)
        if cycle_ids.size > 0:
            first_cycles[selected_experiment_id] = int(cycle_ids[0])
    return first_cycles


def dataset_overview(ds: xr.Dataset) -> dict[str, object]:
    cycle_ids = ds["cycle_id"].values.astype(int)
    last_measurement_timestamp = ds.attrs.get("last_measurement_timestamp")
    return {
        "experiment_ids": available_experiment_ids(ds),
        "experiment_count": int(ds.sizes.get("experiment_id", 0)),
        "cycle_count": int(ds.sizes.get("cycle_id", 0)),
        "hostname_count": int(ds.sizes.get("hostname", 0)),
        "cycle_id_min": int(cycle_ids.min()) if cycle_ids.size else None,
        "cycle_id_max": int(cycle_ids.max()) if cycle_ids.size else None,
        "last_measurement_timestamp": last_measurement_timestamp,
        "last_measurement_timestamp_source": ds.attrs.get("last_measurement_timestamp_source"),
    }


def xarray_structure_markdown(ds: xr.Dataset, max_coord_preview: int = 6) -> str:
    dimension_rows = [
        (
            dimension,
            int(size),
            DIMENSION_DESCRIPTIONS.get(dimension, "No description recorded."),
        )
        for dimension, size in ds.sizes.items()
    ]
    coordinate_rows = [
        (
            coordinate_name,
            type(ds.indexes[coordinate_name]).__name__ if coordinate_name in ds.indexes else "(none)",
            preview_coord_values(ds[coordinate_name].values, max_items=max_coord_preview),
        )
        for coordinate_name in ds.coords
    ]
    variable_rows = [
        (
            variable_name,
            ", ".join(ds[variable_name].dims),
            tuple(int(length) for length in ds[variable_name].shape),
            VARIABLE_DESCRIPTIONS.get(variable_name, "No description recorded."),
        )
        for variable_name in ds.data_vars
    ]

    sections = [
        "## Dataset Axes",
        markdown_table(
            ["Dimension", "Size", "Meaning"],
            dimension_rows,
        ),
        "",
        "## Coordinate Indexes",
        markdown_table(
            ["Coordinate", "Index type", "Preview"],
            coordinate_rows,
        ),
        "",
        "## Data Variables",
        markdown_table(
            ["Variable", "Dims", "Shape", "Meaning"],
            variable_rows,
        ),
        "",
        "Think of the dataset as one stack of experiment slices.",
        "",
        "- A full dataset uses `(experiment_id, cycle_id, hostname)` as its named axes.",
        "- Selecting one `experiment_id` removes the outer axis and leaves a `cycle_id x hostname` slice.",
        "- Rover variables live on the `cycle_id` axis only, because one rover pose belongs to one cycle.",
        "- CSI variables live on `cycle_id x hostname`, because one cycle can contain many host measurements.",
    ]
    return "\n".join(sections)


def selection_walkthrough_markdown(
    ds: xr.Dataset,
    experiment_id: str,
    cycle_id: int,
    hostname: str,
) -> str:
    full_dataset_sizes = ", ".join(f"{name}={size}" for name, size in ds.sizes.items())
    experiment_slice = ds.sel(experiment_id=experiment_id)
    cycle_slice = experiment_slice.sel(cycle_id=int(cycle_id))
    host_slice = cycle_slice.sel(hostname=str(hostname))

    rows = [
        (
            "`ds`",
            full_dataset_sizes,
            "The complete dataset.",
        ),
        (
            f"`ds.sel(experiment_id=\"{experiment_id}\")`",
            ", ".join(f"{name}={size}" for name, size in experiment_slice.sizes.items()),
            "One experiment slice. Rover variables are vectors over `cycle_id`; CSI variables are a `cycle_id x hostname` matrix.",
        ),
        (
            f"`ds.sel(experiment_id=\"{experiment_id}\", cycle_id={int(cycle_id)})`",
            ", ".join(f"{name}={size}" for name, size in cycle_slice.sizes.items()) or "(scalar)",
            "One physical rover stop. Rover variables become scalars, CSI becomes a vector over hostnames.",
        ),
        (
            f"`...sel(hostname=\"{hostname}\")`",
            ", ".join(f"{name}={size}" for name, size in host_slice.sizes.items()) or "(scalar)",
            "One host in one cycle. CSI variables become scalars.",
        ),
    ]

    sections = [
        "## Selection Walkthrough",
        markdown_table(["Selection", "Remaining dims", "Meaning"], rows),
        "",
        "Use `.sel(...)` for named coordinates such as `experiment_id`, `cycle_id`, and `hostname`.",
        "Use `.isel(...)` only when you intentionally want integer positions instead of coordinate labels.",
    ]
    return "\n".join(sections)


def experiment_overview(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None = None,
) -> list[dict[str, object]]:
    experiment_ids = available_experiment_ids(ds) if experiment_id is None else normalize_experiment_ids(experiment_id)
    rows: list[dict[str, object]] = []
    for selected_experiment_id in experiment_ids:
        experiment = ds.sel(experiment_id=selected_experiment_id)
        csi_host_count = experiment["csi_available"].sum(dim="hostname").values.astype(int)
        positions = positions_for_experiment(ds, selected_experiment_id)
        cycle_ids = experiment["cycle_id"].values.astype(int)
        available_cycles = cycle_ids[csi_host_count > 0]
        rows.append(
            {
                "experiment_id": selected_experiment_id,
                "cycle_count": int(experiment.sizes.get("cycle_id", 0)),
                "cycles_with_csi": int(np.count_nonzero(csi_host_count > 0)),
                "valid_position_count": int(positions.sizes.get("cycle_id", 0)),
                "first_csi_cycle": int(available_cycles[0]) if available_cycles.size else None,
                "last_csi_cycle": int(available_cycles[-1]) if available_cycles.size else None,
            }
        )
    return rows


def print_dataset_overview(ds: xr.Dataset) -> None:
    overview = dataset_overview(ds)
    print(f"Experiments: {overview['experiment_ids']}")
    print(
        "Dataset shape:"
        f" experiment_id={overview['experiment_count']},"
        f" cycle_id={overview['cycle_count']},"
        f" hostname={overview['hostname_count']}"
    )
    print(f"Cycle ID range: {overview['cycle_id_min']} .. {overview['cycle_id_max']}")
    if overview["last_measurement_timestamp"] is not None:
        print(
            "Last measurement timestamp:"
            f" {overview['last_measurement_timestamp']}"
            f" ({overview['last_measurement_timestamp_source']})"
        )


def print_experiment_overview(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None = None,
) -> None:
    for row in experiment_overview(ds, experiment_id):
        print(
            f"{row['experiment_id']}:"
            f" cycles_with_csi={row['cycles_with_csi']},"
            f" valid_positions={row['valid_position_count']},"
            f" first_csi_cycle={row['first_csi_cycle']},"
            f" last_csi_cycle={row['last_csi_cycle']}"
        )


def experiment_cycle_table(
    ds: xr.Dataset,
    experiment_id: str,
    max_rows: int | None = 12,
    only_cycles_with_csi: bool = True,
) -> xr.Dataset:
    experiment = ds.sel(experiment_id=experiment_id)
    csi_host_count = experiment["csi_available"].sum(dim="hostname").astype(int)
    position_valid = (
        (experiment["position_available"] > 0)
        & np.isfinite(experiment["rover_x"])
        & np.isfinite(experiment["rover_y"])
        & np.isfinite(experiment["rover_z"])
    )
    table = xr.Dataset(
        data_vars={
            "has_any_csi": csi_host_count > 0,
            "csi_host_count": csi_host_count,
            "position_valid": position_valid,
            "rover_x": experiment["rover_x"],
            "rover_y": experiment["rover_y"],
            "rover_z": experiment["rover_z"],
        },
        coords={"cycle_id": experiment["cycle_id"]},
        attrs={"experiment_id": str(experiment_id)},
    )
    if only_cycles_with_csi:
        table = table.where(table["has_any_csi"], drop=True)
    if max_rows is not None:
        table = table.isel(cycle_id=slice(0, int(max_rows)))
    return table


def positions_for_experiment(ds: xr.Dataset, experiment_id: str) -> xr.Dataset:
    experiment = ds.sel(experiment_id=experiment_id)
    valid_mask = (
        (experiment["position_available"] > 0)
        & np.isfinite(experiment["rover_x"])
        & np.isfinite(experiment["rover_y"])
        & np.isfinite(experiment["rover_z"])
    )

    positions = xr.Dataset(
        data_vars={
            "rover_x": experiment["rover_x"],
            "rover_y": experiment["rover_y"],
            "rover_z": experiment["rover_z"],
            "position_available": experiment["position_available"],
            "csi_host_count": experiment["csi_available"].sum(dim="hostname"),
        },
        coords={"cycle_id": experiment["cycle_id"]},
        attrs={"experiment_id": str(experiment_id)},
    )
    return positions.where(valid_mask, drop=True)


def positions_for_experiments(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None = None,
) -> xr.Dataset:
    experiment_ids = available_experiment_ids(ds) if experiment_id is None else normalize_experiment_ids(experiment_id)

    experiment_values: list[np.ndarray] = []
    cycle_values: list[np.ndarray] = []
    rover_x_values: list[np.ndarray] = []
    rover_y_values: list[np.ndarray] = []
    rover_z_values: list[np.ndarray] = []
    csi_host_count_values: list[np.ndarray] = []

    for selected_experiment_id in experiment_ids:
        positions = positions_for_experiment(ds, selected_experiment_id)
        count = int(positions.sizes.get("cycle_id", 0))
        if count == 0:
            continue

        experiment_values.append(np.repeat(selected_experiment_id, count).astype(str))
        cycle_values.append(positions["cycle_id"].values.astype(int))
        rover_x_values.append(positions["rover_x"].values.astype(float))
        rover_y_values.append(positions["rover_y"].values.astype(float))
        rover_z_values.append(positions["rover_z"].values.astype(float))
        csi_host_count_values.append(positions["csi_host_count"].values.astype(int))

    if not cycle_values:
        return xr.Dataset(
            data_vars={
                "experiment_id": ("measurement_index", np.asarray([], dtype=str)),
                "cycle_id": ("measurement_index", np.asarray([], dtype=int)),
                "rover_x": ("measurement_index", np.asarray([], dtype=float)),
                "rover_y": ("measurement_index", np.asarray([], dtype=float)),
                "rover_z": ("measurement_index", np.asarray([], dtype=float)),
                "csi_host_count": ("measurement_index", np.asarray([], dtype=int)),
            },
            coords={"measurement_index": np.asarray([], dtype=int)},
            attrs={"experiment_ids": experiment_ids},
        )

    measurement_count = sum(values.size for values in cycle_values)
    return xr.Dataset(
        data_vars={
            "experiment_id": ("measurement_index", np.concatenate(experiment_values)),
            "cycle_id": ("measurement_index", np.concatenate(cycle_values)),
            "rover_x": ("measurement_index", np.concatenate(rover_x_values)),
            "rover_y": ("measurement_index", np.concatenate(rover_y_values)),
            "rover_z": ("measurement_index", np.concatenate(rover_z_values)),
            "csi_host_count": ("measurement_index", np.concatenate(csi_host_count_values)),
        },
        coords={"measurement_index": np.arange(measurement_count, dtype=int)},
        attrs={"experiment_ids": experiment_ids},
    )


def movie_frame_table(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None = None,
    max_frames: int | None = DEFAULT_MOVIE_MAX_FRAMES,
) -> xr.Dataset:
    positions = positions_for_experiments(ds, experiment_id)
    total_valid_positions = int(positions.sizes.get("measurement_index", 0))
    if total_valid_positions == 0:
        if experiment_id is None:
            raise ValueError("No valid rover positions available in the dataset.")
        raise ValueError(f"No valid rover positions for {experiment_phrase(experiment_id)}.")

    if max_frames is not None:
        max_frames = int(max_frames)
        if max_frames <= 0:
            raise ValueError("max_frames must be positive or None.")

    if max_frames is None or total_valid_positions <= max_frames:
        source_indices = np.arange(total_valid_positions, dtype=int)
    else:
        source_indices = np.unique(
            np.linspace(0, total_valid_positions - 1, num=max_frames, dtype=int)
        )

    frame_table = positions.isel(measurement_index=source_indices).copy()
    frame_table = frame_table.assign_coords(
        measurement_index=np.arange(frame_table.sizes["measurement_index"], dtype=int)
    )
    frame_table["source_measurement_index"] = ("measurement_index", source_indices.astype(int))
    frame_table.attrs.update(
        {
            "experiment_ids": positions.attrs.get("experiment_ids", []),
            "total_valid_positions": total_valid_positions,
            "frame_count": int(frame_table.sizes.get("measurement_index", 0)),
            "requested_max_frames": max_frames,
            "sampled": bool(source_indices.size != total_valid_positions),
        }
    )
    return frame_table


def rover_track_for_experiment(ds: xr.Dataset, experiment_id: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    positions = positions_for_experiment(ds, experiment_id)
    return (
        positions["rover_x"].values.astype(float),
        positions["rover_y"].values.astype(float),
        positions["cycle_id"].values.astype(int),
    )


def cycle_position(ds: xr.Dataset, experiment_id: str, cycle_id: int) -> dict[str, object]:
    experiment = ds.sel(experiment_id=experiment_id, cycle_id=int(cycle_id))
    rover_x = float(experiment["rover_x"].item())
    rover_y = float(experiment["rover_y"].item())
    rover_z = float(experiment["rover_z"].item())
    position_available = bool(experiment["position_available"].item() > 0)
    finite_position = np.isfinite([rover_x, rover_y, rover_z]).all()

    return {
        "experiment_id": str(experiment_id),
        "cycle_id": int(cycle_id),
        "position_available": bool(position_available and finite_position),
        "rover_x": rover_x if position_available and finite_position else None,
        "rover_y": rover_y if position_available and finite_position else None,
        "rover_z": rover_z if position_available and finite_position else None,
        "csi_host_count": int((experiment["csi_available"].values > 0).sum()),
    }


def find_nearest_position_cycle(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None,
    x: float,
    y: float,
    z: float | None = None,
) -> dict[str, object]:
    positions = positions_for_experiments(ds, experiment_id)
    if positions.sizes.get("measurement_index", 0) == 0:
        if experiment_id is None:
            raise ValueError("No valid rover positions available in the dataset.")
        raise ValueError(f"No valid rover positions for {experiment_phrase(experiment_id)}.")

    dx = positions["rover_x"].values.astype(float) - float(x)
    dy = positions["rover_y"].values.astype(float) - float(y)
    if z is None:
        distance = np.sqrt(dx**2 + dy**2)
    else:
        dz = positions["rover_z"].values.astype(float) - float(z)
        distance = np.sqrt(dx**2 + dy**2 + dz**2)

    best_index = int(np.argmin(distance))
    return {
        "experiment_id": str(positions["experiment_id"].values[best_index]),
        "target_x": float(x),
        "target_y": float(y),
        "target_z": None if z is None else float(z),
        "cycle_id": int(positions["cycle_id"].values[best_index]),
        "rover_x": float(positions["rover_x"].values[best_index]),
        "rover_y": float(positions["rover_y"].values[best_index]),
        "rover_z": float(positions["rover_z"].values[best_index]),
        "distance_m": float(distance[best_index]),
        "csi_host_count": int(positions["csi_host_count"].values[best_index]),
    }


def add_experiment_group_markers(ax: plt.Axes, plot_data: dict[str, object]) -> None:
    group_boundaries = plot_data["group_boundaries"]
    if len(group_boundaries) <= 1:
        return
    for group_index, (selected_experiment_id, start, end) in enumerate(group_boundaries):
        if group_index > 0:
            ax.axvline(start - 0.5, color="white", linewidth=1.0, alpha=0.9)
        center = (start + end - 1) / 2.0
        ax.text(
            center,
            1.02,
            selected_experiment_id,
            transform=ax.get_xaxis_transform(),
            ha="center",
            va="bottom",
            fontsize=9,
        )


def prepare_heatmap_csi(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str],
    antenna_positions: dict[str, np.ndarray] | None = None,
    max_cycle_values: int | None = DEFAULT_HEATMAP_MAX_CYCLE_VALUES,
) -> dict[str, object]:
    if max_cycle_values is not None:
        max_cycle_values = int(max_cycle_values)
        if max_cycle_values <= 0:
            raise ValueError("max_cycle_values must be positive or None.")

    experiment_ids = normalize_experiment_ids(experiment_id)
    entries: list[dict[str, object]] = []
    combined_hostnames: list[str] = []
    seen_hostnames: set[str] = set()

    for selected_experiment_id in experiment_ids:
        experiment = ds.sel(experiment_id=selected_experiment_id)
        csi_available = experiment["csi_available"].values > 0
        available_host_mask = csi_available.any(axis=0)
        available_cycle_mask = csi_available.any(axis=1)
        present_hostnames = experiment["hostname"].values[available_host_mask].astype(str)
        present_cycle_ids = experiment["cycle_id"].values[available_cycle_mask].astype(int)
        if present_hostnames.size == 0:
            raise ValueError(f"No CSI data available for experiment {selected_experiment_id}.")
        if present_cycle_ids.size == 0:
            raise ValueError(f"No CSI cycle IDs available for experiment {selected_experiment_id}.")

        hostnames = ordered_hostnames(present_hostnames, antenna_positions)
        csi_complex = experiment["csi_real"] + 1j * experiment["csi_imag"]
        csi_complex = csi_complex.sel(hostname=hostnames, cycle_id=present_cycle_ids)
        entries.append(
            {
                "experiment_id": selected_experiment_id,
                "hostnames": hostnames,
                "cycle_ids": present_cycle_ids,
                "csi_complex": csi_complex.transpose("hostname", "cycle_id").values,
            }
        )
        for hostname in hostnames:
            if hostname not in seen_hostnames:
                seen_hostnames.add(hostname)
                combined_hostnames.append(hostname)

    hostnames = ordered_hostnames(combined_hostnames, antenna_positions)
    hostname_to_index = {hostname: index for index, hostname in enumerate(hostnames)}
    total_cycles = sum(int(entry["cycle_ids"].size) for entry in entries)
    csi_complex = np.full((len(hostnames), total_cycles), np.nan + 0j, dtype=np.complex128)
    cycle_labels: list[str] = []
    group_boundaries: list[tuple[str, int, int]] = []
    column = 0

    for entry in entries:
        cycle_ids = np.asarray(entry["cycle_ids"], dtype=int)
        cycle_count = int(cycle_ids.size)
        row_indices = [hostname_to_index[hostname] for hostname in entry["hostnames"]]
        csi_complex[np.ix_(row_indices, np.arange(column, column + cycle_count))] = entry["csi_complex"]
        if len(experiment_ids) == 1:
            cycle_labels.extend([str(int(csi_cycle_id)) for csi_cycle_id in cycle_ids])
        else:
            cycle_labels.extend(
                [
                    f"{entry['experiment_id']}:{int(csi_cycle_id)}"
                    for csi_cycle_id in cycle_ids
                ]
            )
        group_boundaries.append((entry["experiment_id"], column, column + cycle_count))
        column += cycle_count

    plot_data = {
        "experiment_ids": experiment_ids,
        "hostnames": hostnames,
        "cycle_labels": np.asarray(cycle_labels, dtype=object),
        "csi_complex": csi_complex,
        "group_boundaries": group_boundaries,
        "total_cycle_count": len(cycle_labels),
        "cycle_limit_applied": False,
        "max_cycle_values": max_cycle_values,
    }
    if max_cycle_values is None or len(cycle_labels) <= max_cycle_values:
        return plot_data

    start_column = len(cycle_labels) - max_cycle_values
    trimmed_group_boundaries: list[tuple[str, int, int]] = []
    for selected_experiment_id, start, end in group_boundaries:
        if end <= start_column:
            continue
        trimmed_group_boundaries.append(
            (
                selected_experiment_id,
                max(0, start - start_column),
                end - start_column,
            )
        )

    plot_data["cycle_labels"] = plot_data["cycle_labels"][start_column:]
    plot_data["csi_complex"] = plot_data["csi_complex"][:, start_column:]
    plot_data["group_boundaries"] = trimmed_group_boundaries
    plot_data["cycle_limit_applied"] = True
    return plot_data


def plot_phase_heatmap(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str],
    antenna_positions: dict[str, np.ndarray] | None = None,
    max_cycle_values: int | None = DEFAULT_HEATMAP_MAX_CYCLE_VALUES,
) -> tuple[plt.Figure, plt.Axes]:
    plot_data = prepare_heatmap_csi(
        ds,
        experiment_id,
        antenna_positions=antenna_positions,
        max_cycle_values=max_cycle_values,
    )
    phase_deg = np.rad2deg(np.angle(plot_data["csi_complex"]))
    values = np.ma.masked_invalid(phase_deg)
    cmap = plt.get_cmap("twilight").copy()
    cmap.set_bad(color="lightgray")

    fig_width = max(10, len(plot_data["cycle_labels"]) * 0.35)
    fig_height = max(6, len(plot_data["hostnames"]) * 0.35)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    image = ax.imshow(
        values,
        aspect="auto",
        interpolation="none",
        cmap=cmap,
        vmin=-180,
        vmax=180,
    )

    x_ticks = tick_positions(plot_data["cycle_labels"])
    y_ticks = np.arange(len(plot_data["hostnames"]))
    cycle_axis_label = "Cycle ID"
    if plot_data["cycle_limit_applied"]:
        cycle_axis_label += f" (last {plot_data['max_cycle_values']} values)"
    ax.set_xticks(x_ticks)
    ax.set_xticklabels(plot_data["cycle_labels"][x_ticks], rotation=45, ha="right")
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(plot_data["hostnames"])
    ax.set_xlabel(cycle_axis_label)
    ax.set_ylabel("Hostname")
    ax.set_title(
        f"CSI phase [deg] for {experiment_phrase(plot_data['experiment_ids'])} "
        "(ordered by antenna position)"
    )
    add_experiment_group_markers(ax, plot_data)

    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label("Phase [deg]")
    fig.tight_layout()
    return fig, ax


def plot_amplitude_heatmap(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str],
    antenna_positions: dict[str, np.ndarray] | None = None,
    max_cycle_values: int | None = DEFAULT_HEATMAP_MAX_CYCLE_VALUES,
) -> tuple[plt.Figure, plt.Axes]:
    plot_data = prepare_heatmap_csi(
        ds,
        experiment_id,
        antenna_positions=antenna_positions,
        max_cycle_values=max_cycle_values,
    )
    amplitude = np.abs(plot_data["csi_complex"])
    values = np.ma.masked_invalid(amplitude)
    cmap = plt.get_cmap("viridis").copy()
    cmap.set_bad(color="lightgray")

    fig_width = max(10, len(plot_data["cycle_labels"]) * 0.35)
    fig_height = max(6, len(plot_data["hostnames"]) * 0.35)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    image = ax.imshow(
        values,
        aspect="auto",
        interpolation="none",
        cmap=cmap,
    )

    x_ticks = tick_positions(plot_data["cycle_labels"])
    y_ticks = np.arange(len(plot_data["hostnames"]))
    cycle_axis_label = "Cycle ID"
    if plot_data["cycle_limit_applied"]:
        cycle_axis_label += f" (last {plot_data['max_cycle_values']} values)"
    ax.set_xticks(x_ticks)
    ax.set_xticklabels(plot_data["cycle_labels"][x_ticks], rotation=45, ha="right")
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(plot_data["hostnames"])
    ax.set_xlabel(cycle_axis_label)
    ax.set_ylabel("Hostname")
    ax.set_title(
        f"CSI amplitude for {experiment_phrase(plot_data['experiment_ids'])} "
        "(ordered by antenna position)"
    )
    add_experiment_group_markers(ax, plot_data)

    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label("Amplitude")
    fig.tight_layout()
    return fig, ax


def power_to_db(power: np.ndarray, floor_db: float = POWER_DB_FLOOR) -> np.ndarray:
    power = np.asarray(power, dtype=float)
    valid = np.isfinite(power) & (power > 0)
    safe_power = np.where(valid, power, np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        power_db = 10.0 * np.log10(safe_power)
    power_db = np.where(np.isfinite(power_db), power_db, floor_db)
    return np.maximum(power_db, floor_db)


def power_norm_from_values(power_db: np.ndarray, floor_db: float = POWER_DB_FLOOR) -> Normalize:
    power_db = np.asarray(power_db, dtype=float)
    valid_power_db = power_db[np.isfinite(power_db)]
    if valid_power_db.size == 0:
        vmin = floor_db
        vmax = floor_db + 1.0
    else:
        vmin = float(np.nanmin(valid_power_db))
        vmax = float(np.nanmax(valid_power_db))
        if vmax <= vmin:
            vmax = vmin + 1.0
    return Normalize(vmin=vmin, vmax=vmax)


def extract_csi_snapshot(
    ds: xr.Dataset,
    experiment_id: str,
    cycle_id: int,
    antenna_positions: dict[str, np.ndarray] | None = None,
) -> xr.Dataset:
    experiment = ds.sel(experiment_id=experiment_id)
    csi_available = experiment["csi_available"].sel(cycle_id=int(cycle_id)).values > 0
    present_hostnames = experiment["hostname"].values[csi_available].astype(str)
    if present_hostnames.size == 0:
        raise ValueError(f"No CSI data available for experiment {experiment_id}, cycle {cycle_id}.")

    hostnames = ordered_hostnames(present_hostnames, antenna_positions)
    csi_real = experiment["csi_real"].sel(cycle_id=int(cycle_id), hostname=hostnames).values.astype(float)
    csi_imag = experiment["csi_imag"].sel(cycle_id=int(cycle_id), hostname=hostnames).values.astype(float)
    csi_complex = csi_real + 1j * csi_imag
    amplitude = np.abs(csi_complex)
    power_db = power_to_db(np.square(amplitude))
    phase_deg = np.rad2deg(np.angle(csi_complex))

    antenna_xyz = np.full((len(hostnames), 3), np.nan, dtype=float)
    if antenna_positions is not None:
        for index, hostname in enumerate(hostnames):
            position = antenna_positions.get(hostname.upper())
            if position is None:
                continue
            antenna_xyz[index] = np.asarray(position, dtype=float)

    position = cycle_position(ds, experiment_id, int(cycle_id))
    snapshot = xr.Dataset(
        data_vars={
            "csi_real": ("hostname", csi_real),
            "csi_imag": ("hostname", csi_imag),
            "csi_amplitude": ("hostname", amplitude),
            "csi_power_db": ("hostname", power_db),
            "csi_phase_deg": ("hostname", phase_deg),
            "antenna_x": ("hostname", antenna_xyz[:, 0]),
            "antenna_y": ("hostname", antenna_xyz[:, 1]),
            "antenna_z": ("hostname", antenna_xyz[:, 2]),
        },
        coords={"hostname": hostnames},
        attrs={
            "experiment_id": str(experiment_id),
            "cycle_id": int(cycle_id),
            "position_available": position["position_available"],
            "rover_x": position["rover_x"],
            "rover_y": position["rover_y"],
            "rover_z": position["rover_z"],
            "csi_host_count": len(hostnames),
        },
    )
    return snapshot


def _snapshot_points(snapshot: xr.Dataset) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    x = snapshot["antenna_x"].values.astype(float)
    y = snapshot["antenna_y"].values.astype(float)
    valid = np.isfinite(x) & np.isfinite(y)
    if not valid.any():
        experiment_id = snapshot.attrs.get("experiment_id")
        cycle_id = snapshot.attrs.get("cycle_id")
        raise ValueError(
            f"No antenna positions mapped for experiment {experiment_id}, cycle {cycle_id}."
        )
    return x[valid], y[valid], valid


def _set_plane_axes(
    ax: plt.Axes,
    x: np.ndarray,
    y: np.ndarray,
    rover_x: float | None,
    rover_y: float | None,
    extra_x: np.ndarray | None = None,
    extra_y: np.ndarray | None = None,
) -> None:
    all_x = np.asarray(x, dtype=float)
    all_y = np.asarray(y, dtype=float)
    if rover_x is not None and rover_y is not None:
        all_x = np.concatenate([all_x, np.asarray([rover_x], dtype=float)])
        all_y = np.concatenate([all_y, np.asarray([rover_y], dtype=float)])
    if extra_x is not None and extra_y is not None and np.size(extra_x) > 0:
        all_x = np.concatenate([all_x, np.asarray(extra_x, dtype=float)])
        all_y = np.concatenate([all_y, np.asarray(extra_y, dtype=float)])

    x_pad = max(0.25, np.ptp(all_x) * 0.08 if all_x.size > 1 else 0.25)
    y_pad = max(0.25, np.ptp(all_y) * 0.08 if all_y.size > 1 else 0.25)
    ax.set_xlim(all_x.min() - x_pad, all_x.max() + x_pad)
    ax.set_ylim(all_y.min() - y_pad, all_y.max() + y_pad)
    ax.set_xlabel("x [m]")
    ax.set_ylabel("y [m]")
    ax.set_aspect("equal", adjustable="box")
    ax.grid(True, linestyle=":", linewidth=0.6, alpha=0.6)


def _overlay_snapshot_rover(ax: plt.Axes, snapshot: xr.Dataset) -> None:
    rover_x = snapshot.attrs.get("rover_x")
    rover_y = snapshot.attrs.get("rover_y")
    if rover_x is None or rover_y is None:
        return

    ax.scatter(
        [float(rover_x)],
        [float(rover_y)],
        marker="*",
        color="crimson",
        edgecolor="black",
        linewidth=0.8,
        s=280,
        zorder=5,
    )
    ax.annotate(
        "rover",
        (float(rover_x), float(rover_y)),
        textcoords="offset points",
        xytext=(8, -12),
        fontsize=9,
        color="crimson",
        weight="bold",
    )


def overlay_antenna_positions(
    ax: plt.Axes,
    antenna_positions: dict[str, np.ndarray] | None = None,
    hostnames: Iterable[str] | None = None,
    positions_url: str = POSITIONS_URL,
    *,
    annotate: bool = False,
    tile_size_m: float = ANTENNA_TILE_SIZE_M,
) -> xr.Dataset:
    antenna_table = antenna_position_table(
        antenna_positions=antenna_positions,
        hostnames=hostnames,
        positions_url=positions_url,
    )
    x = antenna_table["antenna_x"].values.astype(float)
    y = antenna_table["antenna_y"].values.astype(float)
    hostnames = antenna_table["hostname"].values.astype(str)
    half_tile = float(tile_size_m) / 2.0

    for index, (xi, yi) in enumerate(zip(x, y)):
        rectangle = Rectangle(
            (xi - half_tile, yi - half_tile),
            width=tile_size_m,
            height=tile_size_m,
            facecolor="white",
            edgecolor="black",
            linewidth=0.9,
            alpha=0.95,
            zorder=4,
            label="Antennas" if index == 0 else None,
        )
        ax.add_patch(rectangle)

    if annotate:
        for xi, yi, hostname in zip(x, y, hostnames):
            ax.annotate(
                hostname,
                (xi, yi),
                textcoords="offset points",
                xytext=(4, 4),
                fontsize=7,
                color="black",
    )
    return antenna_table


def plot_spatial_phase_snapshot(snapshot: xr.Dataset, annotate: bool = True) -> tuple[plt.Figure, plt.Axes]:
    x, y, valid = _snapshot_points(snapshot)
    values = snapshot["csi_phase_deg"].values.astype(float)[valid]
    hostnames = snapshot["hostname"].values.astype(str)[valid]

    fig, ax = plt.subplots(figsize=(8, 6))
    scatter = ax.scatter(
        x,
        y,
        c=values,
        cmap="twilight",
        vmin=-180,
        vmax=180,
        s=520,
        marker="s",
        edgecolor="black",
        linewidth=0.8,
        zorder=3,
    )

    if annotate:
        for xi, yi, hostname in zip(x, y, hostnames):
            ax.text(
                xi,
                yi,
                hostname,
                ha="center",
                va="center",
                fontsize=8,
                bbox=dict(facecolor="white", alpha=0.65, edgecolor="none", pad=1.0),
                zorder=4,
            )

    rover_x = snapshot.attrs.get("rover_x")
    rover_y = snapshot.attrs.get("rover_y")
    _overlay_snapshot_rover(ax, snapshot)
    _set_plane_axes(ax, x, y, rover_x, rover_y)
    ax.set_title(
        "CSI phase [deg] on the antenna plane for "
        f"{snapshot.attrs['experiment_id']}, cycle {snapshot.attrs['cycle_id']}"
    )

    colorbar = fig.colorbar(scatter, ax=ax, pad=0.02)
    colorbar.set_label("Phase [deg]")
    fig.tight_layout()
    return fig, ax


def plot_spatial_power_snapshot(snapshot: xr.Dataset, annotate: bool = True) -> tuple[plt.Figure, plt.Axes]:
    x, y, valid = _snapshot_points(snapshot)
    values = snapshot["csi_power_db"].values.astype(float)[valid]
    hostnames = snapshot["hostname"].values.astype(str)[valid]
    power_norm = power_norm_from_values(values)

    fig, ax = plt.subplots(figsize=(8, 6))
    scatter = ax.scatter(
        x,
        y,
        c=values,
        cmap="viridis",
        norm=power_norm,
        s=520,
        marker="s",
        edgecolor="black",
        linewidth=0.8,
        zorder=3,
    )

    if annotate:
        for xi, yi, hostname in zip(x, y, hostnames):
            ax.text(
                xi,
                yi,
                hostname,
                ha="center",
                va="center",
                fontsize=8,
                bbox=dict(facecolor="white", alpha=0.65, edgecolor="none", pad=1.0),
                zorder=4,
            )

    rover_x = snapshot.attrs.get("rover_x")
    rover_y = snapshot.attrs.get("rover_y")
    _overlay_snapshot_rover(ax, snapshot)
    _set_plane_axes(ax, x, y, rover_x, rover_y)
    ax.set_title(
        "CSI power [dB] on the antenna plane for "
        f"{snapshot.attrs['experiment_id']}, cycle {snapshot.attrs['cycle_id']}"
    )

    colorbar = fig.colorbar(scatter, ax=ax, pad=0.02)
    colorbar.set_label("Power [dB]")
    fig.tight_layout()
    return fig, ax


def plot_position_cloud(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None = None,
    *,
    show_antennas: bool = True,
    antenna_positions: dict[str, np.ndarray] | None = None,
    annotate_antennas: bool = False,
    s: float = 18.0,
    alpha: float = 0.85,
    color: str = "tab:blue",
) -> tuple[plt.Figure, plt.Axes]:
    positions = positions_for_experiments(ds, experiment_id)
    if positions.sizes.get("measurement_index", 0) == 0:
        if experiment_id is None:
            raise ValueError("No valid rover positions available in the dataset.")
        raise ValueError(f"No valid rover positions for {experiment_phrase(experiment_id)}.")

    x = positions["rover_x"].values.astype(float)
    y = positions["rover_y"].values.astype(float)

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(
        x,
        y,
        s=s,
        color=color,
        alpha=alpha,
        edgecolor="black",
        linewidth=0.35,
        label="Rover positions",
        zorder=2,
    )
    antenna_table = None
    if show_antennas:
        antenna_table = overlay_antenna_positions(
            ax,
            antenna_positions=antenna_positions,
            hostnames=active_hostnames(ds, experiment_id),
            annotate=annotate_antennas,
        )
    antenna_x = None if antenna_table is None else antenna_table["antenna_x"].values
    antenna_y = None if antenna_table is None else antenna_table["antenna_y"].values
    if antenna_table is None:
        antenna_outline_x = None
        antenna_outline_y = None
    else:
        half_tile = float(antenna_table.attrs.get("antenna_tile_size_m", ANTENNA_TILE_SIZE_M)) / 2.0
        antenna_outline_x = np.concatenate([antenna_x - half_tile, antenna_x + half_tile])
        antenna_outline_y = np.concatenate([antenna_y - half_tile, antenna_y + half_tile])
    _set_plane_axes(
        ax,
        x,
        y,
        rover_x=None,
        rover_y=None,
        extra_x=antenna_outline_x,
        extra_y=antenna_outline_y,
    )

    if experiment_id is None:
        selected_experiment_ids = available_experiment_ids(ds)
    else:
        selected_experiment_ids = normalize_experiment_ids(experiment_id)
    ax.set_title(
        "All valid rover positions for "
        f"{experiment_phrase(selected_experiment_ids)} (merged point cloud)"
    )
    fig.tight_layout()
    return fig, ax


def plot_trajectory(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str],
) -> tuple[plt.Figure, plt.Axes]:
    experiment_ids = normalize_experiment_ids(experiment_id)
    if len(experiment_ids) == 1:
        selected_experiment_id = experiment_ids[0]
        x, y, cycle_ids = rover_track_for_experiment(ds, selected_experiment_id)
        if x.size == 0:
            raise ValueError(f"No valid rover positions for experiment {selected_experiment_id}.")

        fig, ax = plt.subplots(figsize=(7, 6))
        ax.plot(x, y, "-", color="0.6", linewidth=1.5, zorder=1)
        scatter = ax.scatter(
            x,
            y,
            c=cycle_ids,
            cmap="viridis",
            s=70,
            edgecolor="black",
            zorder=2,
        )

        if cycle_ids.size <= 30:
            for xi, yi, selected_cycle_id in zip(x, y, cycle_ids):
                ax.annotate(
                    str(selected_cycle_id),
                    (xi, yi),
                    textcoords="offset points",
                    xytext=(5, 5),
                    fontsize=8,
                )

        colorbar = fig.colorbar(scatter, ax=ax, pad=0.02)
        colorbar.set_label("Cycle ID")
        ax.set_title(f"Rover trajectory for experiment {selected_experiment_id}")
        _set_plane_axes(ax, x, y, rover_x=None, rover_y=None)
        fig.tight_layout()
        return fig, ax

    fig, ax = plt.subplots(figsize=(8, 6))
    cmap = plt.get_cmap("tab10")
    all_x: list[np.ndarray] = []
    all_y: list[np.ndarray] = []
    plotted = False
    for index, selected_experiment_id in enumerate(experiment_ids):
        x, y, cycle_ids = rover_track_for_experiment(ds, selected_experiment_id)
        if x.size == 0:
            print(f"No valid rover positions for experiment {selected_experiment_id}.")
            continue
        plotted = True
        color = cmap(index % cmap.N)
        ax.plot(x, y, "-", color=color, linewidth=1.5, alpha=0.75, zorder=1)
        ax.scatter(
            x,
            y,
            color=[color],
            s=60,
            edgecolor="black",
            linewidth=0.4,
            zorder=2,
            label=selected_experiment_id,
        )
        all_x.append(x)
        all_y.append(y)

    if not plotted:
        raise ValueError(f"No valid rover positions for experiments {experiment_label(experiment_ids)}.")

    _set_plane_axes(ax, np.concatenate(all_x), np.concatenate(all_y), rover_x=None, rover_y=None)
    ax.set_title(f"Rover trajectories for experiments {experiment_label(experiment_ids)}")
    ax.legend(title="Experiment")
    fig.tight_layout()
    return fig, ax


def _movie_sequence_label(experiment_ids: Sequence[str]) -> str:
    if len(experiment_ids) == 1:
        return f"experiment {experiment_ids[0]}"
    return f"{len(experiment_ids)} merged experiments"


def _prepare_snapshot_movie_data(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None,
    antenna_positions: dict[str, np.ndarray] | None,
    max_frames: int | None,
) -> dict[str, object]:
    frame_table = movie_frame_table(ds, experiment_id=experiment_id, max_frames=max_frames)
    selected_experiment_ids = frame_table.attrs.get("experiment_ids", [])
    all_positions = positions_for_experiments(ds, experiment_id)
    active_tiles = active_hostnames(ds, experiment_id)
    antenna_table = antenna_position_table(
        antenna_positions=antenna_positions,
        hostnames=active_tiles,
    )

    frames: list[dict[str, object]] = []
    power_values: list[np.ndarray] = []
    for frame_index in range(frame_table.sizes["measurement_index"]):
        selected_experiment_id = str(frame_table["experiment_id"].values[frame_index])
        selected_cycle_id = int(frame_table["cycle_id"].values[frame_index])
        snapshot = extract_csi_snapshot(
            ds,
            selected_experiment_id,
            selected_cycle_id,
            antenna_positions=antenna_positions,
        )
        x, y, valid = _snapshot_points(snapshot)
        power_db = snapshot["csi_power_db"].values.astype(float)[valid]
        phase_deg = snapshot["csi_phase_deg"].values.astype(float)[valid]
        frames.append(
            {
                "experiment_id": selected_experiment_id,
                "cycle_id": selected_cycle_id,
                "rover_x": snapshot.attrs.get("rover_x"),
                "rover_y": snapshot.attrs.get("rover_y"),
                "antenna_x": x,
                "antenna_y": y,
                "phase_deg": phase_deg,
                "power_db": power_db,
                "csi_host_count": int(snapshot.attrs.get("csi_host_count", x.size)),
            }
        )
        if power_db.size > 0:
            power_values.append(power_db)

    if power_values:
        power_norm = power_norm_from_values(np.concatenate(power_values), floor_db=POWER_DB_FLOOR)
    else:
        power_norm = power_norm_from_values(np.asarray([POWER_DB_FLOOR], dtype=float), floor_db=POWER_DB_FLOOR)

    return {
        "frame_table": frame_table,
        "frames": frames,
        "selected_experiment_ids": selected_experiment_ids,
        "sequence_label": _movie_sequence_label(selected_experiment_ids),
        "all_rover_x": all_positions["rover_x"].values.astype(float),
        "all_rover_y": all_positions["rover_y"].values.astype(float),
        "frame_rover_x": frame_table["rover_x"].values.astype(float),
        "frame_rover_y": frame_table["rover_y"].values.astype(float),
        "antenna_table": antenna_table,
        "power_norm": power_norm,
    }


def _snapshot_movie_status_text(
    movie_data: dict[str, object],
    frame: dict[str, object],
    frame_number: int,
) -> str:
    frame_table = movie_data["frame_table"]
    lines = [
        f"{frame['experiment_id']} cycle {frame['cycle_id']}",
        f"frame {frame_number}/{frame_table.attrs['frame_count']}",
        f"hosts with CSI: {frame['csi_host_count']}",
    ]
    if frame_table.attrs.get("sampled", False):
        lines.append(
            "evenly sampled from "
            f"{frame_table.attrs['total_valid_positions']} valid rover positions"
        )
    return "\n".join(lines)


def _export_snapshot_movie(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None,
    output_path: str | Path,
    *,
    value_key: str,
    title_prefix: str,
    colorbar_label: str,
    cmap_name: str,
    norm: Normalize,
    movie_data: dict[str, object] | None = None,
    antenna_positions: dict[str, np.ndarray] | None = None,
    max_frames: int | None = DEFAULT_MOVIE_MAX_FRAMES,
    fps: int = DEFAULT_MOVIE_FPS,
    dpi: int = DEFAULT_MOVIE_DPI,
    annotate_antennas: bool = False,
    bitrate: int = 2400,
) -> Path:
    if movie_data is None:
        movie_data = _prepare_snapshot_movie_data(
            ds,
            experiment_id=experiment_id,
            antenna_positions=antenna_positions,
            max_frames=max_frames,
        )
    output_path = Path(output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(8.5, 6.5))
    all_rover_x = np.asarray(movie_data["all_rover_x"], dtype=float)
    all_rover_y = np.asarray(movie_data["all_rover_y"], dtype=float)
    if all_rover_x.size > 0:
        ax.scatter(
            all_rover_x,
            all_rover_y,
            s=10,
            color="0.85",
            alpha=0.7,
            linewidth=0.0,
            zorder=1,
        )

    antenna_table = movie_data["antenna_table"]
    if antenna_table.sizes.get("hostname", 0) > 0:
        overlay_antenna_positions(
            ax,
            antenna_positions=antenna_positions,
            hostnames=antenna_table["hostname"].values.astype(str),
            annotate=annotate_antennas,
        )
        half_tile = float(antenna_table.attrs.get("antenna_tile_size_m", ANTENNA_TILE_SIZE_M)) / 2.0
        antenna_outline_x = np.concatenate(
            [
                antenna_table["antenna_x"].values.astype(float) - half_tile,
                antenna_table["antenna_x"].values.astype(float) + half_tile,
            ]
        )
        antenna_outline_y = np.concatenate(
            [
                antenna_table["antenna_y"].values.astype(float) - half_tile,
                antenna_table["antenna_y"].values.astype(float) + half_tile,
            ]
        )
        _set_plane_axes(
            ax,
            antenna_table["antenna_x"].values.astype(float),
            antenna_table["antenna_y"].values.astype(float),
            rover_x=None,
            rover_y=None,
            extra_x=np.concatenate([all_rover_x, antenna_outline_x]),
            extra_y=np.concatenate([all_rover_y, antenna_outline_y]),
        )
    else:
        first_frame = movie_data["frames"][0]
        _set_plane_axes(
            ax,
            np.asarray(first_frame["antenna_x"], dtype=float),
            np.asarray(first_frame["antenna_y"], dtype=float),
            rover_x=None,
            rover_y=None,
            extra_x=all_rover_x,
            extra_y=all_rover_y,
        )

    history_artist = ax.scatter(
        [],
        [],
        s=18,
        color="darkorange",
        alpha=0.9,
        edgecolor="white",
        linewidth=0.3,
        zorder=2,
    )
    snapshot_artist = ax.scatter(
        [],
        [],
        c=[],
        cmap=cmap_name,
        norm=norm,
        s=260,
        marker="s",
        edgecolor="black",
        linewidth=0.7,
        zorder=3,
    )
    rover_artist = ax.scatter(
        [],
        [],
        marker="*",
        color="crimson",
        edgecolor="black",
        linewidth=0.8,
        s=260,
        zorder=4,
    )
    status_text = ax.text(
        0.02,
        0.03,
        "",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=9,
        bbox=dict(facecolor="white", alpha=0.9, edgecolor="0.75", boxstyle="round,pad=0.35"),
        zorder=5,
    )
    colorbar = fig.colorbar(ScalarMappable(norm=norm, cmap=cmap_name), ax=ax, pad=0.02)
    colorbar.set_label(colorbar_label)
    fig.tight_layout()

    writer = FFMpegWriter(
        fps=int(fps),
        bitrate=int(bitrate),
        metadata={
            "title": f"{title_prefix} for {movie_data['sequence_label']}",
            "artist": "ELLIIIT dataset tutorial utilities",
        },
    )

    with writer.saving(fig, str(output_path), dpi=int(dpi)):
        for frame_number, frame in enumerate(movie_data["frames"], start=1):
            antenna_x = np.asarray(frame["antenna_x"], dtype=float)
            antenna_y = np.asarray(frame["antenna_y"], dtype=float)
            values = np.asarray(frame[value_key], dtype=float)
            snapshot_artist.set_offsets(np.column_stack([antenna_x, antenna_y]))
            snapshot_artist.set_array(values)

            history_offsets = np.column_stack(
                [
                    np.asarray(movie_data["frame_rover_x"], dtype=float)[:frame_number],
                    np.asarray(movie_data["frame_rover_y"], dtype=float)[:frame_number],
                ]
            )
            history_artist.set_offsets(history_offsets)

            rover_x = frame["rover_x"]
            rover_y = frame["rover_y"]
            if rover_x is None or rover_y is None:
                rover_artist.set_offsets(np.empty((0, 2), dtype=float))
            else:
                rover_artist.set_offsets(np.asarray([[float(rover_x), float(rover_y)]], dtype=float))

            ax.set_title(
                f"{title_prefix} for {movie_data['sequence_label']}"
            )
            status_text.set_text(_snapshot_movie_status_text(movie_data, frame, frame_number))
            writer.grab_frame()

    plt.close(fig)
    return output_path


def export_spatial_phase_movie(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None,
    output_path: str | Path,
    *,
    antenna_positions: dict[str, np.ndarray] | None = None,
    max_frames: int | None = DEFAULT_MOVIE_MAX_FRAMES,
    fps: int = DEFAULT_MOVIE_FPS,
    dpi: int = DEFAULT_MOVIE_DPI,
    annotate_antennas: bool = False,
) -> Path:
    return _export_snapshot_movie(
        ds,
        experiment_id=experiment_id,
        output_path=output_path,
        value_key="phase_deg",
        title_prefix="CSI phase [deg] on the antenna plane",
        colorbar_label="Phase [deg]",
        cmap_name="twilight",
        norm=Normalize(vmin=-180, vmax=180),
        antenna_positions=antenna_positions,
        max_frames=max_frames,
        fps=fps,
        dpi=dpi,
        annotate_antennas=annotate_antennas,
    )


def export_spatial_power_movie(
    ds: xr.Dataset,
    experiment_id: str | Sequence[str] | None,
    output_path: str | Path,
    *,
    antenna_positions: dict[str, np.ndarray] | None = None,
    max_frames: int | None = DEFAULT_MOVIE_MAX_FRAMES,
    fps: int = DEFAULT_MOVIE_FPS,
    dpi: int = DEFAULT_MOVIE_DPI,
    annotate_antennas: bool = False,
) -> Path:
    movie_data = _prepare_snapshot_movie_data(
        ds,
        experiment_id=experiment_id,
        antenna_positions=antenna_positions,
        max_frames=max_frames,
    )
    return _export_snapshot_movie(
        ds,
        experiment_id=experiment_id,
        output_path=output_path,
        value_key="power_db",
        title_prefix="CSI power [dB] on the antenna plane",
        colorbar_label="Power [dB]",
        cmap_name="viridis",
        norm=movie_data["power_norm"],
        movie_data=movie_data,
        antenna_positions=antenna_positions,
        max_frames=max_frames,
        fps=fps,
        dpi=dpi,
        annotate_antennas=annotate_antennas,
    )
