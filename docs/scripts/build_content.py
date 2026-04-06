from __future__ import annotations

import importlib.util
from pathlib import Path
import re


DOCS_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = DOCS_ROOT.parent
PROCESSING_TUTORIALS_DIR = REPO_ROOT / "processing" / "tutorials"
DOCS_TUTORIALS_DIR = DOCS_ROOT / "src" / "content" / "docs" / "tutorials"
ASTRO_CONFIG_PATH = DOCS_ROOT / "astro.config.mjs"
NOTEBOOK_PATH_PATTERN = re.compile(r'notebookPath="notebooks/(?P<name>[^"]+\.ipynb)"')


def load_script_module(script_name: str):
    script_path = DOCS_ROOT / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load script module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def notebook_names() -> list[str]:
    return sorted(path.name for path in PROCESSING_TUTORIALS_DIR.glob("*.ipynb"))


def notebook_page_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for page_path in sorted(DOCS_TUTORIALS_DIR.glob("notebook-*.mdx")):
        text = page_path.read_text(encoding="utf-8")
        match = NOTEBOOK_PATH_PATTERN.search(text)
        if match is None:
            raise ValueError(f"Could not find notebookPath in {page_path}")
        mapping[match.group("name")] = page_path.stem
    return mapping


def validate_tutorial_docs() -> None:
    notebooks = set(notebook_names())
    page_map = notebook_page_map()
    documented = set(page_map)
    sidebar_text = ASTRO_CONFIG_PATH.read_text(encoding="utf-8")
    export_notebooks_module = load_script_module("export_notebooks.py")
    exported_notebooks = set(export_notebooks_module.NOTEBOOKS)

    missing_pages = sorted(notebooks - documented)
    extra_pages = sorted(documented - notebooks)
    missing_exports = sorted(notebooks - exported_notebooks)
    extra_exports = sorted(exported_notebooks - notebooks)
    missing_sidebar = sorted(
        f"{notebook_name} -> tutorials/{slug}"
        for notebook_name, slug in page_map.items()
        if f'slug: "tutorials/{slug}"' not in sidebar_text
    )

    errors: list[str] = []
    if missing_pages:
        errors.append(f"missing docs pages for notebooks: {', '.join(missing_pages)}")
    if extra_pages:
        errors.append(f"docs pages reference missing notebooks: {', '.join(extra_pages)}")
    if missing_exports:
        errors.append(f"notebooks missing from export_notebooks.py: {', '.join(missing_exports)}")
    if extra_exports:
        errors.append(f"export_notebooks.py references missing notebooks: {', '.join(extra_exports)}")
    if missing_sidebar:
        errors.append(f"tutorial pages missing from sidebar: {', '.join(missing_sidebar)}")

    if errors:
        raise SystemExit("Tutorial docs validation failed:\n- " + "\n- ".join(errors))

    print(f"Validated tutorial docs coverage for {len(notebooks)} notebook(s).")


def newest_mtime_ns(paths: list[Path]) -> int:
    return max(path.stat().st_mtime_ns for path in paths)


def assets_are_stale(outputs: list[Path], sources: list[Path]) -> bool:
    if not outputs or any(not path.exists() for path in outputs):
        return True
    return newest_mtime_ns(sources) > min(path.stat().st_mtime_ns for path in outputs)


def export_notebooks() -> None:
    module = load_script_module("export_notebooks.py")
    module.main()


def optional_dependency_name(exc: BaseException) -> str | None:
    if isinstance(exc, ModuleNotFoundError):
        return exc.name or "unknown"
    return None


def optional_runtime_issue(exc: BaseException) -> bool:
    if optional_dependency_name(exc) is not None:
        return True
    if isinstance(exc, FileNotFoundError):
        return "ffmpeg" in str(exc).lower()
    if isinstance(exc, RuntimeError):
        return "ffmpeg" in str(exc).lower()
    return False


def log_optional_asset_skip(exc: BaseException, step: str) -> None:
    dependency_name = optional_dependency_name(exc)
    if dependency_name is not None:
        print(
            f"Skipping {step}: optional Python dependency missing ({dependency_name}). "
            "Notebook HTML exports were still generated."
        )
        return
    print(f"Skipping {step}: {exc}. Notebook HTML exports were still generated.")


def export_notebook_figures_and_movies() -> None:
    try:
        figures_module = load_script_module("export_notebook_figures.py")
        movies_module = load_script_module("export_notebook_movies.py")
    except BaseException as exc:
        if optional_runtime_issue(exc):
            log_optional_asset_skip(exc, "notebook figure/movie export")
            return
        raise

    try:
        dataset_path = figures_module.find_latest_dataset()
    except FileNotFoundError as exc:
        print(f"Skipping notebook figure/movie export: {exc}")
        return
    except BaseException as exc:
        if optional_runtime_issue(exc):
            log_optional_asset_skip(exc, "notebook figure/movie export")
            return
        raise

    print(f"Found RF dataset for dynamic docs assets: {dataset_path}")

    figure_outputs = [
        figures_module.output_dir() / file_name
        for file_names in figures_module.NOTEBOOK_FIGURES.values()
        for file_name in file_names
    ]
    figure_sources = [
        DOCS_ROOT / "scripts" / "export_notebook_figures.py",
        PROCESSING_TUTORIALS_DIR / "csi_plot_utils.py",
        dataset_path,
    ] + [PROCESSING_TUTORIALS_DIR / notebook_name for notebook_name in figures_module.NOTEBOOK_FIGURES]

    try:
        if assets_are_stale(figure_outputs, figure_sources):
            figures_module.main()
        else:
            print("Notebook figures are already up to date.")
    except BaseException as exc:
        if optional_runtime_issue(exc):
            log_optional_asset_skip(exc, "notebook figure export")
            return
        raise

    movie_outputs = [
        movies_module.output_dir() / movies_module.PHASE_MOVIE_NAME,
        movies_module.output_dir() / movies_module.POWER_MOVIE_NAME,
    ]
    movie_sources = [
        DOCS_ROOT / "scripts" / "export_notebook_movies.py",
        PROCESSING_TUTORIALS_DIR / "csi_plot_utils.py",
        PROCESSING_TUTORIALS_DIR / "tutorial_csi_movies.ipynb",
        dataset_path,
    ]

    try:
        if assets_are_stale(movie_outputs, movie_sources):
            movies_module.main()
        else:
            print("Notebook movies are already up to date.")
    except BaseException as exc:
        if optional_runtime_issue(exc):
            log_optional_asset_skip(exc, "notebook movie export")
            return
        raise


def main() -> None:
    validate_tutorial_docs()
    export_notebooks()
    export_notebook_figures_and_movies()


if __name__ == "__main__":
    main()
