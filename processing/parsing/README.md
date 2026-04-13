# Parsing

This folder contains the scripts that convert raw measurement outputs into processed dataset products.

## Contents

- `extract_csi_from_smb_v2.py`: builds the processed RF xarray/NetCDF from RF runtime logs and rover positions
- `acoustic_parser.py`: builds the processed acoustic xarray/NetCDF from acoustic captures and metadata
- `summarize_error_logs_from_smb.py`: summarizes RF runtime error logs for diagnostics and QA
- `check_measurement_completeness.py`: downloads published acoustic datasets, then checks per-experiment/per-cycle presence across RF, rover positions, and acoustic files

## Typical outputs

- `results/csi_<experiment_id>.nc`
- `results/acoustic_<experiment_id>.nc`
- optional JSON summaries written by the error log summarizer
- `results/measurement_completeness_report.json`
- `results/measurement_completeness_report.md`

Use this folder when you need to regenerate processed datasets from raw files.
