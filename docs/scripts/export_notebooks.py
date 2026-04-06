from __future__ import annotations

from pathlib import Path
import warnings

import nbformat
from nbconvert import HTMLExporter
from nbformat.validator import normalize


NOTEBOOKS = [
    "plot_csi_positions.ipynb",
    "tutorial_xarray_structure.ipynb",
    "tutorial_acoustic_xarray_structure.ipynb",
    "tutorial_rover_positions.ipynb",
    "tutorial_csi_per_position.ipynb",
    "tutorial_rf_acoustic_position.ipynb",
    "tutorial_csi_movies.ipynb",
]

warnings.filterwarnings(
    "ignore",
    message="IPython3 lexer unavailable, falling back on Python 3",
    category=UserWarning,
    module="nbconvert.filters.highlight",
)


def main() -> None:
    docs_root = Path(__file__).resolve().parent.parent
    repo_root = docs_root.parent
    processing_dir = repo_root / "processing" / "tutorials"
    output_dir = docs_root / "public" / "notebooks"
    output_dir.mkdir(parents=True, exist_ok=True)

    for pattern in ("*.html", "*.ipynb"):
        for path in output_dir.glob(pattern):
            path.unlink()

    exporter = HTMLExporter(template_name="lab")
    exporter.exclude_input_prompt = True
    exporter.exclude_output_prompt = True

    for notebook_name in NOTEBOOKS:
        source_path = processing_dir / notebook_name
        if not source_path.exists():
            raise FileNotFoundError(f"Notebook not found: {source_path}")

        validation_error: dict[str, object] = {}
        notebook = nbformat.read(source_path, as_version=4, capture_validation_error=validation_error)
        normalize(notebook)
        html_body, _resources = exporter.from_notebook_node(
            notebook,
            resources={"metadata": {"name": source_path.stem}},
        )

        html_path = output_dir / f"{source_path.stem}.html"
        html_path.write_text(html_body, encoding="utf-8")
        nbformat.write(notebook, output_dir / source_path.name)
        print(f"Exported {source_path.name} -> {html_path.relative_to(docs_root)}")


if __name__ == "__main__":
    main()
