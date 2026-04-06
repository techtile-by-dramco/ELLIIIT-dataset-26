#!/usr/bin/env python3
"""List and download processed acoustic NetCDF datasets from the DRAMCO server.

Examples:
    python processing/dataset-download/download_acoustic_datasets.py --list
    python processing/dataset-download/download_acoustic_datasets.py --experiment-id EXP003
    python processing/dataset-download/download_acoustic_datasets.py acoustic_EXP003.nc acoustic_EXP005.nc
    python processing/dataset-download/download_acoustic_datasets.py --all --overwrite
"""

from __future__ import annotations

import argparse
import contextlib
from html.parser import HTMLParser
from pathlib import Path
import re
import sys
from urllib.parse import quote, unquote, urljoin, urlsplit
from urllib.request import Request, urlopen


SCRIPT_DIR = Path(__file__).resolve().parent
PROCESSING_ROOT = SCRIPT_DIR.parent
PROJECT_ROOT = PROCESSING_ROOT.parent
DEFAULT_BASE_URL = "https://dramco.be/datasets/ELLIIT-2026/"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "results"
DEFAULT_TIMEOUT_SECONDS = 30.0
CHUNK_SIZE_BYTES = 1024 * 1024
EXPERIMENT_ID_RE = re.compile(r"^EXP\d+$", re.IGNORECASE)
DATASET_FILENAME_RE = re.compile(r"^acoustic_.*\.nc$", re.IGNORECASE)
REQUESTED_DATASET_RE = re.compile(r"^(?:acoustic_)?(EXP\d+)(?:\.nc)?$", re.IGNORECASE)
USER_AGENT = "ELLIIIT-acoustic-downloader/1.0"


class DirectoryIndexParser(HTMLParser):
    """Extract href targets from a simple Apache-style directory listing."""

    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)
                return


def ensure_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else f"{url}/"


def dedupe_preserve_order(values: list[str]) -> list[str]:
    unique_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return unique_values


def normalize_experiment_id(value: str) -> str:
    experiment_id = value.strip().upper()
    if not EXPERIMENT_ID_RE.fullmatch(experiment_id):
        raise ValueError(f"Invalid experiment id {value!r}. Expected a value such as EXP003.")
    return experiment_id


def parse_available_dataset_names(index_html: str) -> list[str]:
    parser = DirectoryIndexParser()
    parser.feed(index_html)

    dataset_names: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        file_name = Path(unquote(urlsplit(href).path)).name
        if DATASET_FILENAME_RE.fullmatch(file_name) is None:
            continue

        if file_name in seen:
            continue

        seen.add(file_name)
        dataset_names.append(file_name)

    return dataset_names


def fetch_available_dataset_names(base_url: str, timeout_seconds: float) -> list[str]:
    index_url = ensure_trailing_slash(base_url)
    request = Request(index_url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout_seconds) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        index_html = response.read().decode(charset, errors="replace")

    dataset_names = parse_available_dataset_names(index_html)
    if not dataset_names:
        raise RuntimeError(f"No acoustic_*.nc files were found in the server listing at {index_url}.")

    return dataset_names


def matching_datasets_for_experiment(experiment_id: str, available_dataset_names: list[str]) -> list[str]:
    normalized_experiment_id = normalize_experiment_id(experiment_id)
    patterns = (
        re.compile(rf"^acoustic_{normalized_experiment_id}\.nc$", re.IGNORECASE),
        re.compile(rf"^acoustic_{normalized_experiment_id}_.*\.nc$", re.IGNORECASE),
        re.compile(rf"^acoustic_{normalized_experiment_id}.*\.nc$", re.IGNORECASE),
    )

    matches: list[str] = []
    for pattern in patterns:
        for dataset_name in available_dataset_names:
            if pattern.fullmatch(dataset_name):
                matches.append(dataset_name)

    return dedupe_preserve_order(matches)


def resolve_positional_dataset_request(dataset_name: str, available_dataset_names: list[str]) -> list[str]:
    raw_value = Path(dataset_name.strip()).name
    if not raw_value:
        raise ValueError("Dataset names must not be empty.")

    shorthand_match = REQUESTED_DATASET_RE.fullmatch(raw_value)
    if shorthand_match is not None:
        normalized_experiment_id = normalize_experiment_id(shorthand_match.group(1))
        exact_name = f"acoustic_{normalized_experiment_id}.nc"
        if exact_name in available_dataset_names:
            return [exact_name]
        matching_dataset_names = matching_datasets_for_experiment(normalized_experiment_id, available_dataset_names)
        if matching_dataset_names:
            return matching_dataset_names
        raise FileNotFoundError(f"No acoustic datasets were found for experiment {normalized_experiment_id}.")

    if raw_value.lower().endswith(".nc"):
        return [raw_value]

    raise ValueError(
        f"Invalid dataset value {dataset_name!r}. Use an experiment id such as EXP003 or a filename such as acoustic_EXP003.nc."
    )


def resolve_requested_dataset_names(
    positional_datasets: list[str],
    experiment_ids: list[str],
    *,
    download_all: bool,
    available_dataset_names: list[str],
) -> list[str]:
    if download_all:
        return list(available_dataset_names)

    requested_dataset_names: list[str] = []
    for dataset_name in positional_datasets:
        requested_dataset_names.extend(
            resolve_positional_dataset_request(dataset_name, available_dataset_names)
        )
    for experiment_id in experiment_ids:
        matching_dataset_names = matching_datasets_for_experiment(experiment_id, available_dataset_names)
        if not matching_dataset_names:
            normalized_experiment_id = normalize_experiment_id(experiment_id)
            raise FileNotFoundError(f"No acoustic datasets were found for experiment {normalized_experiment_id}.")
        requested_dataset_names.extend(matching_dataset_names)
    requested_dataset_names = dedupe_preserve_order(requested_dataset_names)

    if not requested_dataset_names:
        raise ValueError(
            "No datasets were requested. Use --list, --all, one or more --experiment-id values, or dataset filenames."
        )

    available_dataset_set = set(available_dataset_names)
    missing_dataset_names = [
        dataset_name
        for dataset_name in requested_dataset_names
        if dataset_name not in available_dataset_set
    ]
    if missing_dataset_names:
        preview = ", ".join(available_dataset_names[:10])
        suffix = "" if len(available_dataset_names) <= 10 else f", ... ({len(available_dataset_names) - 10} more)"
        raise FileNotFoundError(
            "Requested dataset(s) are not present in the server listing: "
            f"{', '.join(missing_dataset_names)}. Available datasets: {preview}{suffix}"
        )

    return requested_dataset_names


def format_size(num_bytes: int) -> str:
    size = float(num_bytes)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"


def download_dataset(
    dataset_name: str,
    *,
    base_url: str,
    output_dir: Path,
    overwrite: bool,
    timeout_seconds: float,
) -> tuple[Path, int, bool]:
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / dataset_name
    if destination.exists() and not overwrite:
        return destination, destination.stat().st_size, True

    temp_destination = destination.with_suffix(f"{destination.suffix}.part")
    dataset_url = urljoin(ensure_trailing_slash(base_url), quote(dataset_name))
    request = Request(dataset_url, headers={"User-Agent": USER_AGENT})

    try:
        with urlopen(request, timeout=timeout_seconds) as response, temp_destination.open("wb") as handle:
            total_written = 0
            while True:
                chunk = response.read(CHUNK_SIZE_BYTES)
                if not chunk:
                    break
                handle.write(chunk)
                total_written += len(chunk)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            temp_destination.unlink()
        raise

    temp_destination.replace(destination)
    return destination, total_written, False


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download selected acoustic NetCDF datasets from the DRAMCO server into the local results directory."
        )
    )
    parser.add_argument(
        "datasets",
        nargs="*",
        help="Dataset filenames or shorthand experiment ids such as EXP003 or acoustic_EXP003.nc.",
    )
    parser.add_argument(
        "--experiment-id",
        action="append",
        default=[],
        metavar="EXP###",
        help="Experiment id to download. Repeat the flag to request multiple experiments.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the acoustic datasets currently visible in the server directory listing and exit if nothing else was requested.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Download every dataset found in the server directory listing.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Destination directory for downloaded datasets. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Dataset directory URL. Default: {DEFAULT_BASE_URL}",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing local file instead of skipping it.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        metavar="SECONDS",
        help=f"Network timeout in seconds. Default: {DEFAULT_TIMEOUT_SECONDS}",
    )
    return parser


def run(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    base_url = ensure_trailing_slash(args.base_url)
    output_dir = args.output_dir.expanduser()
    if not output_dir.is_absolute():
        output_dir = output_dir.resolve()

    available_dataset_names = fetch_available_dataset_names(base_url, args.timeout)

    if args.list:
        print(f"Available acoustic datasets at {base_url}:")
        for dataset_name in available_dataset_names:
            print(f"- {dataset_name}")
        if not args.all and not args.datasets and not args.experiment_id:
            return 0

    selected_dataset_names = resolve_requested_dataset_names(
        args.datasets,
        args.experiment_id,
        download_all=args.all,
        available_dataset_names=available_dataset_names,
    )

    print(f"Downloading {len(selected_dataset_names)} dataset(s) into {output_dir}:")
    for dataset_name in selected_dataset_names:
        destination, num_bytes, skipped_existing = download_dataset(
            dataset_name,
            base_url=base_url,
            output_dir=output_dir,
            overwrite=args.overwrite,
            timeout_seconds=args.timeout,
        )
        action = "kept existing" if skipped_existing else "downloaded"
        print(f"- {action}: {destination} ({format_size(num_bytes)})")

    return 0


def main() -> int:
    try:
        return run()
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
