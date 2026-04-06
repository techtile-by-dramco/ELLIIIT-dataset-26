from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import nbformat
import matplotlib
from nbformat.validator import normalize

matplotlib.use("Agg")
import matplotlib.pyplot as plt


NOTEBOOK_FIGURES: dict[str, list[str]] = {
    "plot_csi_positions.ipynb": [
        "rf_overview_trajectory.png",
        "rf_overview_phase_heatmap.png",
        "rf_overview_amplitude_heatmap.png",
    ],
    "tutorial_rover_positions.ipynb": [
        "rover_position_cloud.png",
        "measurement_locations_trajectory.png",
    ],
    "tutorial_csi_per_position.ipynb": [
        "csi_spatial_phase_snapshot.png",
        "csi_spatial_power_snapshot.png",
        "csi_position_phase_heatmap.png",
        "csi_position_amplitude_heatmap.png",
    ],
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def processing_dir() -> Path:
    return repo_root() / "processing" / "tutorials"


def output_dir() -> Path:
    return repo_root() / "docs" / "public" / "images" / "notebook-exports"


def find_latest_dataset() -> Path:
    sys.path.insert(0, str(processing_dir()))
    import csi_plot_utils as csi

    dataset_paths = csi.find_dataset_paths()
    if not dataset_paths:
        raise FileNotFoundError("Could not find a dataset matching csi*.nc in results/, the working directory, or processing/tutorials/.")
    return dataset_paths[0]


def patch_dataset_path_source(source: str, dataset_path: Path) -> str:
    replacement = f'DATASET_PATH = r"{dataset_path.as_posix()}"'
    lines = source.splitlines()
    updated_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("DATASET_PATH = None"):
            indentation = line[: len(line) - len(line.lstrip())]
            updated_lines.append(f"{indentation}{replacement}")
        else:
            updated_lines.append(line)
    return "\n".join(updated_lines)


@contextmanager
def notebook_runtime(destination_dir: Path, file_names: list[str]):
    original_cwd = Path.cwd()
    original_show = plt.show
    saved_count = 0

    def export_show(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal saved_count
        figure_numbers = list(plt.get_fignums())
        for figure_number in figure_numbers:
            if saved_count >= len(file_names):
                raise ValueError(
                    f"Notebook produced more figures than expected. Already saved {saved_count},"
                    f" configured for {len(file_names)}."
                )
            figure = plt.figure(figure_number)
            target_path = destination_dir / file_names[saved_count]
            figure.savefig(target_path, dpi=160, bbox_inches="tight")
            saved_count += 1
        plt.close("all")

    try:
        os.chdir(processing_dir())
        plt.close("all")
        plt.show = export_show  # type: ignore[assignment]
        yield lambda: saved_count
    finally:
        plt.show = original_show  # type: ignore[assignment]
        plt.close("all")
        os.chdir(original_cwd)


def execute_notebook(source_path: Path, dataset_path: Path, destination_dir: Path, file_names: list[str]) -> int:
    validation_error: dict[str, object] = {}
    notebook = nbformat.read(source_path, as_version=4, capture_validation_error=validation_error)
    normalize(notebook)
    namespace: dict[str, object] = {"__name__": "__main__"}

    with notebook_runtime(destination_dir, file_names) as saved_count:
        for cell in notebook.cells:
            if cell.get("cell_type") != "code":
                continue
            source = "".join(cell.get("source", []))
            if not source.strip():
                continue
            exec(patch_dataset_path_source(source, dataset_path), namespace)
        return saved_count()


def main() -> None:
    destination_dir = output_dir()
    destination_dir.mkdir(parents=True, exist_ok=True)
    for path in destination_dir.glob("*.png"):
        path.unlink()

    dataset_path = find_latest_dataset()
    print(f"Using dataset: {dataset_path}")

    for notebook_name, figure_names in NOTEBOOK_FIGURES.items():
        source_path = processing_dir() / notebook_name
        if not source_path.exists():
            raise FileNotFoundError(f"Notebook not found: {source_path}")

        image_count = execute_notebook(source_path, dataset_path, destination_dir, figure_names)
        if image_count != len(figure_names):
            raise ValueError(
                f"Expected {len(figure_names)} figure outputs from {notebook_name}, found {image_count}."
            )
        print(
            f"Exported {image_count} figure(s) from {notebook_name} "
            f"to {destination_dir.relative_to(repo_root())}"
        )


if __name__ == "__main__":
    main()
