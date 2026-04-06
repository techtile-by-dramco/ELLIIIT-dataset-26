from __future__ import annotations

import sys
from pathlib import Path


PHASE_MOVIE_NAME = "phase_rover_merged.mp4"
POWER_MOVIE_NAME = "power_rover_merged.mp4"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def processing_dir() -> Path:
    return repo_root() / "processing" / "tutorials"


def output_dir() -> Path:
    return repo_root() / "docs" / "public" / "media"


def main() -> None:
    sys.path.insert(0, str(processing_dir()))
    import csi_plot_utils as csi

    destination_dir = output_dir()
    destination_dir.mkdir(parents=True, exist_ok=True)

    ds, dataset_path = csi.open_dataset()
    experiment_ids = csi.available_experiment_ids(ds)
    antenna_positions = csi.load_antenna_positions()

    print(f"Using dataset: {dataset_path}")
    print(f"Using merged experiments: {experiment_ids}")

    phase_movie_path = csi.export_spatial_phase_movie(
        ds,
        experiment_ids,
        destination_dir / PHASE_MOVIE_NAME,
        antenna_positions=antenna_positions,
        max_frames=csi.DEFAULT_MOVIE_MAX_FRAMES,
        fps=csi.DEFAULT_MOVIE_FPS,
    )
    print(f"Exported phase movie -> {phase_movie_path.relative_to(repo_root())}")

    power_movie_path = csi.export_spatial_power_movie(
        ds,
        experiment_ids,
        destination_dir / POWER_MOVIE_NAME,
        antenna_positions=antenna_positions,
        max_frames=csi.DEFAULT_MOVIE_MAX_FRAMES,
        fps=csi.DEFAULT_MOVIE_FPS,
    )
    print(f"Exported power movie -> {power_movie_path.relative_to(repo_root())}")

    ds.close()


if __name__ == "__main__":
    main()
