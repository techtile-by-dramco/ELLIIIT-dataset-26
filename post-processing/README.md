# Post-Processing

This folder is the user workspace for dataset analysis after the processed `.nc` files are available.

Put your own notebooks, scripts, plots, reports, and derived analysis here.

Examples:

- exploratory notebooks built on top of the RF and acoustic datasets
- custom figures for papers or presentations
- model-training or feature-extraction scripts
- comparison notebooks across experiments

## Minimal Getting Started

1. Clone the repository:

```bash
git clone https://github.com/techtile-by-dramco/ELLIIIT-dataset-26.git
cd ELLIIIT-dataset-26
```

2. Create a Python environment and install the basic notebook dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install jupyterlab matplotlib numpy requests xarray pyyaml h5netcdf
```

3. Download the published acoustic datasets into `results/`:

```bash
python processing/dataset-download/download_acoustic_datasets.py --list
python processing/dataset-download/download_acoustic_datasets.py --experiment-id EXP003
```

4. Make sure your processed RF dataset is also present in `results/`, for example as:

```text
results/csi_<experiment_id>.nc
```

5. Start Jupyter and use the repository tutorials as reference:

```bash
jupyter lab
```

Useful starting points:

- `processing/tutorials/tutorial_xarray_structure.ipynb`
- `processing/tutorials/tutorial_acoustic_xarray_structure.ipynb`
- `processing/tutorials/tutorial_rf_acoustic_position.ipynb`

6. Create your own files in this folder:

- `post-processing/my-analysis.ipynb`
- `post-processing/compare_experiments.py`
- `post-processing/figures/`

## Suggested Workflow

- keep the published datasets in `results/`
- keep your own analysis code and outputs in `post-processing/`
- reuse the tutorial notebooks as templates, but avoid editing them directly unless you intend to improve the shared examples

## Contributing Back

If your post-processing results are useful for other users of the dataset, create a branch, commit the relevant files, and open a pull request against the main repository.

That is the preferred path when:

- your notebook or script is reusable
- your figures or summaries improve the shared docs
- your analysis fixes a bug or clarifies the interpretation of the dataset

Wait until the results are in good shape, then open the pull request so the work can be reviewed and merged into the main repo.
