# ELLIIIT Dataset Repository

This repository contains the acquisition, orchestration, storage, and post-processing code used to collect the ELLIIIT acoustic and RF dataset inside Techtile.

The user-facing documentation now lives in the GitHub Pages site under [`docs/`](docs/).

Primary entry points:

- Published docs: <https://techtile-by-dramco.github.io/ELLIIIT-dataset-26/>
- Local docs source: [`docs/`](docs/)
- Runnable notebook tutorials: [`processing/plot_csi_positions.ipynb`](processing/plot_csi_positions.ipynb), [`processing/tutorial_rover_positions.ipynb`](processing/tutorial_rover_positions.ipynb), [`processing/tutorial_csi_per_position.ipynb`](processing/tutorial_csi_per_position.ipynb)

Local docs workflow:

```bash
cd docs
npm install
python -m pip install -r requirements.txt
npm run dev
```

Key code paths remain in:

- `server/` for orchestration and control-plane logic
- `client/` for rover, RF, and auxiliary clients
- `acoustic/` for acoustic capture and processing
- `processing/` for RF extraction, xarray utilities, and notebook-based analysis
