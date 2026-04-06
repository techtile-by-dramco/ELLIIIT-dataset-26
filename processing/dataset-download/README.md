# Dataset Download

This folder contains download helpers for datasets that are published on external storage and should not be committed to the repository.

## Contents

- `download_acoustic_datasets.py`: lists available acoustic `.nc` files on the DRAMCO server and downloads them into `results/`

## Typical usage

```bash
python processing/dataset-download/download_acoustic_datasets.py --list
python processing/dataset-download/download_acoustic_datasets.py --experiment-id EXP003
```

Downloaded files are written to `results/`.
