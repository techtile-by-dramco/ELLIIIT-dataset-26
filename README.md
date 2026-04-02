# ELLIIIT Dataset Measurement and Processing Guide

This repository contains the acquisition, orchestration, storage, and processing code used to collect the ELLIIIT acoustic and RF dataset inside Techtile.

The README is organized in two views:

1. View 1 is a tutorial for dataset users who need to understand what was measured, how one measurement should be interpreted, and which files belong together.
2. View 2 is a reference for operators and developers who need to deploy the system, run new acquisitions, or extend the control plane.

## Contents

1. [What This Repository Produces](#what-this-repository-produces)
2. [What One Measurement Means](#what-one-measurement-means)
3. [Physical Measurement Setup](#physical-measurement-setup)
4. [Measurement Sequence](#measurement-sequence)
5. [Data Products and File Locations](#data-products-and-file-locations)
6. [How to Interpret and Join the Data](#how-to-interpret-and-join-the-data)
7. [View 1 Tutorial: Using the Dataset](#view-1-tutorial-using-the-dataset)
8. [View 2: Operator and Developer Reference](#view-2-operator-and-developer-reference)
9. [Runtime Architecture](#runtime-architecture)
10. [Configuration Files](#configuration-files)
11. [Deployment and Acquisition](#deployment-and-acquisition)
12. [RF Post-Processing](#rf-post-processing)
13. [Operational Utilities](#operational-utilities)

## What This Repository Produces

The project collects paired measurements from two sensing modalities:

- Acoustic measurements from a distributed microphone array using an NI DAQ and chirp excitation.
- RF measurements from distributed USRP-based receivers and transmitters inside Techtile.

The acoustic source and RF user-equipment antenna are mounted on the same moving rig. At each rover stop, the system records:

- A rover position command and completion event.
- A Qualisys position sample captured after the move.
- One acoustic measurement.
- One RF measurement cycle.

This means the dataset is spatially paired:

- The acoustic and RF observations correspond to the same rig pose.
- The position sample is captured before the acoustic and RF measurements for that pose.
- Acoustic and RF are not acquired simultaneously with each other. They are acquired sequentially at the same position.

## What One Measurement Means

The most important concept in this repository is the orchestrator cycle.

In the current control flow, one cycle means:

1. Move the rover to the next configured waypoint.
2. Wait for the rover to report `MOVE_DONE`.
3. Capture one Qualisys position sample.
4. Trigger one acoustic measurement.
5. Trigger one RF measurement cycle.

Identifiers used throughout the system:

- `experiment_id`: the logical run identifier configured in `server/serverConfig.yaml`.
- `cycle_id`: the sequential index of the orchestrator loop.
- `meas_id`: the sequential measurement index sent in the protocol.

In the current implementation, `cycle_id` and `meas_id` both increment once per loop and are usually equal, but they should still be treated as separate fields because the code keeps both.

Practical interpretation:

- One row in `exp-<experiment_id>-positions.csv` corresponds to one rover stop and one later acoustic/RF acquisition pair.
- One RF JSON-line record with a given `experiment_id` and `cycle_id` should be joined to the position row with the same pair.
- Acoustic captures are triggered once per cycle as well, but the saved acoustic CSV filenames currently use timestamps rather than `experiment_id` or `cycle_id`.

## Physical Measurement Setup

The measurement rig combines:

- An ultrasonic speaker for the acoustic channel.
- An RF UE antenna for the RF channel.
- A common mechanical mount so both modalities share the same sampled location.

Ground-truth position is provided by the Qualisys system configured in the `positioning` block of `experiment-settings.yaml`.

The RF infrastructure includes:

- Ceiling and wall tile workers started from `client_scripts` in `experiment-settings.yaml`.
- A continuous reference transmitter from `client/run-ref.py`.
- A dedicated RF synchronization layer using PUB/REP sockets.

For the reciprocity dataset path in this repository, the active RF aperture is the ceiling group:

- 42 ceiling receiver tiles: `A05` through `G10`, i.e. the 7-by-6 ceiling block defined in the Techtile inventory.
- 1 synchronized pilot transmitter, which makes `rf_sync.num_subscribers = 43` in `experiment-settings.yaml`.
- 1 separate continuous reference transmitter started by `client/run-ref.py`; this reference host is part of the outer orchestrator readiness check, but not part of the `ALIVE`/`DONE` count used by the RF fan-out cycle.

External geometry references:

- Ceiling group membership: <https://github.com/techtile-by-dramco/tile-management/blob/main/inventory/hosts.yaml>
- Per-tile RF channel coordinates and normals: <https://github.com/techtile-by-dramco/plotter/blob/main/src/TechtilePlotter/positions.yml>

Important interpretation detail:

- The repository stores one RF summary row per receiver host and per cycle.
- The external geometry file lists two RF channels per ceiling tile.
- The current processed dataset therefore represents 42 receiver hosts per fully populated cycle, not a separate stored CSI row for every physical RF connector in `positions.yml`.

The acoustic infrastructure includes:

- An NI DAQ-based excitation and capture pipeline in `acoustic/acousticMeasurement.py`.
- A ZMQ-triggered acoustic client in `acoustic/ZMQclient_acoustic.py`.
- Distributed microphone definitions in `acoustic/dicts.py`.

## Measurement Sequence

The outer measurement sequence is implemented in `server/zmq_orchestrator.py`.

Per cycle:

```text
orchestrator
  -> MOVE to rover
  <- MOVE_DONE
  -> capture one Qualisys position sample
  -> START_MEAS to acoustic client
  <- MEAS_DONE
  -> START_MEAS to RF orchestrator
  <- MEAS_DONE
```

Important interpretation details:

- The position sample is taken after the rover has reached the commanded waypoint.
- The acoustic and RF measurements are then performed while the rig is assumed to remain at that location.
- The RF cycle contains its own internal synchronization with the tile workers.

### RF Sub-Sequence

Per `START_MEAS` to `server/record/RF-orchestrator.py`:

1. Wait for `rf_sync.num_subscribers` ALIVE messages on `rf_sync.alive_port`.
2. Wait `rf_sync.pre_sync_delay_s` seconds.
3. Publish one `SYNC` message on `rf_sync.sync_port` with payload `<cycle_id> <experiment_id>`.
4. Wait for `rf_sync.num_subscribers` DONE messages on `rf_sync.done_port`.
5. Append a measurement entry to `server/record/data/exp-<experiment_id>.yml`.

This RF cycle produces one logically synchronized RF snapshot across the participating tiles.

## Data Products and File Locations

The repository writes several different outputs. The files do not all serve the same purpose.

### 1. Rover Position Log

Written by:

- `server/zmq_orchestrator.py`

Location:

- `server/record/data/exp-<experiment_id>-positions.csv`

Meaning:

- One row per completed rover move.
- This is the main file used to recover the physical pose for each orchestrator cycle.

Important columns:

- `experiment_id`
- `cycle_id`
- `meas_id`
- `move_status`
- `position_status`
- `captured_at_utc`
- `position_t`
- `x`, `y`, `z`
- `position`
- `rotation_matrix_json`
- `error`

Typical usage:

- Join RF rows on `(experiment_id, cycle_id)`.
- Use `position_status == ok` or `position_available == 1` in processed data to filter valid positions.

### 2. RF Measurement Log

Written by:

- `client/run_reciprocity.py`
- `client/run_uncalibrated.py`

Location:

- Per-host runtime output directory resolved from `experiment_config.storage_path`
- File pattern: `data_<HOSTNAME>_<experiment_id>.txt`

Meaning:

- Each line is a JSON record for one RF measurement on one host.
- The records contain the identifiers needed to join measurements back to rover positions.
- The active path stores per-cycle summary metrics, not raw IQ captures.

Typical fields per successful line:

- `timestamp_utc`
- `hostname`
- `file_name`
- `experiment_id`
- `cycle_id`
- `pilot_phase`
- `pilot_phase_deg`
- `pilot_amplitude`
- `avg_amplitude_ch0`
- `avg_amplitude_ch1`
- `rms_amplitude_ch0`
- `rms_amplitude_ch1`
- `captured_samples`

Interpretation:

- Each host writes its own local view of the same RF cycle.
- The full RF aperture is reconstructed later by grouping rows with the same `experiment_id` and `cycle_id` across hostnames.
- In a fully populated reciprocity run, that means up to 42 ceiling receiver rows per cycle.
- The synchronized pilot transmitter participates in the RF timing handshake, but it does not write the receiver-side RF summary rows described here.

### 3. RF Runtime Error Log

Written by:

- `client/run_reciprocity.py`
- `client/run_uncalibrated.py`

Location:

- Per-host runtime output directory
- File name: `error.log`

Meaning:

- JSON-line diagnostics for failed or suspect RF captures.
- Useful for debugging missing CSI, metadata errors, clock lock issues, and capture anomalies.

Typical fields:

- `timestamp_utc`
- `hostname`
- `error_type`
- `message`
- `experiment_id`
- `cycle_id`
- `file_name`

### 4. RF Orchestrator Summary Log

Written by:

- `server/record/RF-orchestrator.py`

Location:

- `server/record/data/exp-<experiment_id>.yml`

Meaning:

- One YAML summary file per experiment.
- Records which tiles were active for each RF cycle.

Typical contents:

- `experiment`
- `num_subscribers`
- `measurements`
  - `meas_id`
  - `cycle_id`
  - `experiment_id`
  - `active_tiles`

Use this file to verify:

- Which RF workers participated in a given cycle.
- Whether a missing host measurement is due to a worker not being active.

### 5. Acoustic Capture Files

Written by:

- `acoustic/acousticMeasurement.py`

Location:

- `acoustic/results/Measured_Signal_<timestamp>.csv`

Meaning:

- One CSV per acoustic acquisition.
- Each row stores one microphone channel or the chirp excitation itself.

Columns:

- `duration`
- `f_start`
- `f_stop`
- `chirp_amp`
- `microphone_coordinates`
- `microphone_label`
- `values`

Interpretation:

- `values` contains either the saved ESS response or an RIR-like derived signal depending on `acoustic/config.json`.
- Unused or faulty channels are stored with `values = "unused"`.
- The final row contains the transmitted `chirp_excitation`.

Important caveat:

- The current acoustic file naming is timestamp-based only.
- `experiment_id`, `cycle_id`, and `meas_id` are visible in `acoustic/ZMQclient_acoustic.py` logs, but they are not embedded into the acoustic CSV filename or CSV rows by `run_acoustic_measurement()`.
- If you need exact cycle-to-acoustic-file matching, you currently need to use acquisition order, timestamps, and acoustic client logs together.

### 6. Preview and Rover Planning Outputs

Written by:

- `client/rover/ZMQclient_rover.py --preview-sweeps`

Location:

- `client/rover/config_sweep_preview.png` by default

Meaning:

- Static preview of the configured rover sweep pattern.
- This is a planning and validation artifact, not measurement data.

## How to Interpret and Join the Data

### The Main Join Key

For RF and position data, the main join key is:

- `(experiment_id, cycle_id)`

That join is exactly what `processing/extract_csi_from_smb_v2.py` uses.

### What Is Synchronized and What Is Not

The dataset has several layers of synchronization:

- Acoustic microphones are synchronized within one acoustic capture.
- RF workers are synchronized within one RF cycle.
- Acoustic and RF are paired by rover position and orchestration order, not acquired at the exact same instant.
- The position sample is captured immediately after motion completion and before both sensing modalities.

This means:

- Cross-microphone timing within acoustic data is meaningful.
- Cross-host RF phase and amplitude interpretation depends on the worker mode and cable correction.
- Cross-modality fusion should be treated as same-pose pairing, not necessarily same-instant timing.

### RF Aperture and Geometry

For the reciprocity measurement path, the RF data should be read as a ceiling-receiver dataset:

- The intended receiver aperture is the 42 ceiling tiles `A05` to `G10`.
- The synchronized RF cycle also includes one pilot transmitter node, giving 43 `ALIVE`/`DONE` participants in total.
- Exact tile membership is maintained in the Techtile inventory:
  - <https://github.com/techtile-by-dramco/tile-management/blob/main/inventory/hosts.yaml>
- Exact RF channel coordinates are maintained in the Techtile plotter geometry:
  - <https://github.com/techtile-by-dramco/plotter/blob/main/src/TechtilePlotter/positions.yml>

When using those geometry files, keep in mind:

- Each ceiling tile has two RF channels in the geometry definition.
- The active acquisition path in this repository stores one summarized complex pilot measurement per receiver host and cycle.
- In other words, the dataset currently behaves as a 42-node ceiling receiver aperture with one stored complex value per node and per cycle.

### RF Signalling and Timing

The RF path uses two transmitters with different roles.

1. Continuous reference transmitter

- `client/run-ref.py` runs as a long-lived process on the reference node.
- In the current experiment settings it transmits a continuous sine at:
  - center frequency `920e6`
  - sample rate `250e3`
  - baseband waveform frequency `0`
  - amplitude `0.8`
  - gain `73`
- This process also registers itself as the `ref` client for the outer orchestrator heartbeat.

2. Per-cycle pilot transmitter

- `client/usrp_pilot.py` is the synchronized pilot source triggered by the RF fan-out cycle.
- After receiving `SYNC`, it resets its USRP time to `0` on the next PPS edge, waits 2 seconds, tunes at `t = 3 s`, and starts the pilot TX at `t = 5 s`.
- The configured pilot TX duration is 10 seconds.
- In the current settings the transmitted pilot phase is `0` and the active TX amplitude is `0.8`.

3. Ceiling receiver timing

- `client/run_reciprocity.py` follows the same PPS-based reset and tuning sequence after each `SYNC`.
- The ceiling receivers tune at `t = 3 s`.
- The pilot receive capture starts at `t = 6 s` and lasts 5 seconds.
- This gives a 1-second guard interval between pilot-TX start and pilot-RX start, and the 5-second receive window lies inside the 10-second pilot transmission.

4. Why the received signal appears as a 1 kHz tone

- Both `client/usrp_pilot.py` and `client/run-ref.py` transmit at `920 MHz`.
- The receivers in `client/run_reciprocity.py` are intentionally tuned to `freq - 1e3`, i.e. `919.999 MHz`.
- As a result, a received carrier at `920 MHz` appears in complex baseband as an approximately `1 kHz` tone.
- The phase extraction helper therefore band-pass filters around `1 kHz ± 100 Hz`.
- In the current channel mapping, the software treats RX channel 0 as the reference channel and RX channel 1 as the measurement channel.

For the active JSON-based RF path:

- `pilot_phase` is the measured phase before cable correction.
- `processing/extract_csi_from_smb_v2.py` subtracts the cable phase from `client/ref-RF-cable.yml`.
- The complex CSI is formed as:
  - `csi = pilot_amplitude * exp(1j * (pilot_phase - phi_cable))`

The processed NetCDF stores:

- `csi_real`
- `csi_imag`
- `csi_available`

Reconstruct complex CSI in analysis as:

```python
csi = ds["csi_real"] + 1j * ds["csi_imag"]
phase = np.angle(csi)
amplitude = np.abs(csi)
```

### How the RF Phase Angle Is Computed

The stored RF "angle" is a complex phase angle in radians. It is not an angle-of-arrival estimate.

For each receiver host in `client/run_reciprocity.py`:

1. A 5-second capture is recorded on the two RX channels.
2. The first second is discarded for analysis, leaving a steadier window.
3. Each channel is band-pass filtered around the expected 1 kHz beat note in `client/tools.py`.
4. Instantaneous phase is taken from the filtered complex samples.
5. The per-sample phase difference is formed as:

```python
phase_diff = wrap_to_pi(angle(ch0) - angle(ch1))
```

6. The stored `pilot_phase` is the circular mean of that wrapped phase difference.
7. The stored `pilot_amplitude` is the RMS amplitude of the channel-1 samples over the analysis window.

This is the same logic as:

```python
phase_ch0, _, _ = tools.get_phases_and_apply_bandpass(iq_samples[0, :])
phase_ch1, _, _ = tools.get_phases_and_apply_bandpass(iq_samples[1, :])
phase_diff = tools.to_min_pi_plus_pi(phase_ch0 - phase_ch1, deg=False)
pilot_phase = tools.circmean(phase_diff, deg=False)
pilot_amplitude = np.sqrt(np.mean(np.abs(iq_samples[1, :]) ** 2))
```

Post-processing then converts that stored magnitude/phase pair into a complex CSI value and subtracts the per-host cable calibration phase from `client/ref-RF-cable.yml`.

Practical consequence:

- `pilot_phase` is a calibrated channel phase surrogate, not a geometric direction estimate.
- If you want an actual angle-of-arrival or beam direction, that must be estimated later from the spatial geometry of multiple ceiling receivers together.

### Position Interpretation

The position file stores the Qualisys output after each rover move.

Key points:

- `move_status` tells you whether the rover phase succeeded.
- `position_status` tells you whether the positioner returned valid data.
- `x`, `y`, `z` are the ground-truth pose used by the processing script.

In the processed NetCDF:

- `rover_x`
- `rover_y`
- `rover_z`
- `position_available`

are indexed by `experiment_id` and `cycle_id`.

### Acoustic Interpretation

The acoustic CSV is currently less integration-friendly than the RF data because it is not keyed directly by `experiment_id` and `cycle_id`.

Use it as follows:

- Treat each CSV as one acoustic measurement event.
- Use file timestamps and acoustic client logs to place it in run order.
- Interpret the channel rows using `microphone_label` and `microphone_coordinates`.
- Use the final `chirp_excitation` row for deconvolution or verification if needed.

## View 1 Tutorial: Using the Dataset

If you only want to use the measured data, read this section from top to bottom. The goal of View 1 is simple: start from one experiment, find the files that belong together, build the RF-plus-position dataset, and understand how to interpret the acoustic files alongside it.

### Step 1. Start From One Experiment

1. Pick the `experiment_id` you want to analyze.
2. Open `server/record/data/exp-<experiment_id>-positions.csv`.
3. Find the RF host result files named `data_<HOSTNAME>_<experiment_id>.txt` under the runtime storage root configured by `experiment_config.storage_path`.
4. If you also need acoustic data, collect the timestamped CSV files from `acoustic/results/`.

At this point, the most important rule is:

- RF data and position data join directly on `(experiment_id, cycle_id)`.
- Acoustic data is also recorded once per cycle, but it currently has to be matched by run order, timestamps, and logs rather than by an embedded `cycle_id`.

### Step 2. Understand What One Row Means

Before joining anything, keep this mental model in mind:

1. One orchestrator cycle means one rover pose.
2. That pose gets one Qualisys position sample.
3. That same pose then gets one acoustic capture.
4. That same pose then gets one RF measurement cycle.

In practice:

- One row in `exp-<experiment_id>-positions.csv` is one rover stop.
- One RF JSON-line record with the same `experiment_id` and `cycle_id` belongs to that stop.
- For the reciprocity path, a fully populated cycle can contain up to 42 ceiling receiver rows, one per ceiling host.

### Step 3. Build the RF and Position Dataset

Run the extractor:

```bash
python processing/extract_csi_from_smb_v2.py
```

Useful variants:

```bash
python processing/extract_csi_from_smb_v2.py --max-measurements 10
python processing/extract_csi_from_smb_v2.py --data-root /path/to/storage/root
python processing/extract_csi_from_smb_v2.py --positions-root server/record/data
```

What this does:

1. It reads the per-host RF result files.
2. It extracts `experiment_id`, `cycle_id`, `hostname`, `pilot_phase`, and `pilot_amplitude`.
3. It applies the cable-phase correction from `client/ref-RF-cable.yml`.
4. It joins the RF rows with the rover position file.
5. It writes `processing/csi_<experiment_id>.nc`.

Filtering applied along the way:

- At logging time, `server/zmq_orchestrator.py` writes `position_status = no_data` and empty `x/y/z` fields when the positioner has no fresh update. This prevents stale Qualisys samples from being logged as new rover poses.
- At extraction time, `processing/extract_csi_from_smb.py` and `processing/extract_csi_from_smb_v2.py` remove consecutive cycles whose rover position is effectively unchanged from the last kept cycle for the same experiment.
- The duplicate-position tolerance is taken from `client/rover/config.yaml` as `grid.min_spacing / 5` per axis.
- Because `grid.min_spacing` is configured in millimeters while the logged Qualisys coordinates are in meters, the extractor converts that tolerance to meters before filtering.
- When a cycle is removed by this duplicate-position filter, both the position row and all CSI rows for that `(experiment_id, cycle_id)` are removed from the resulting xarray dataset.

### Step 4. Open the NetCDF and Read It Correctly

Once `processing/csi_<experiment_id>.nc` exists, open it in xarray. This file is the main RF-based measurement xarray produced by the repository.

Use these coordinates:

- `experiment_id`
- `cycle_id`
- `hostname`

Variable layout:

- `rover_x`, `rover_y`, `rover_z`: shape `(experiment_id, cycle_id)`
- `position_available`: shape `(experiment_id, cycle_id)`
- `csi_real`, `csi_imag`: shape `(experiment_id, cycle_id, hostname)`
- `csi_available`: shape `(experiment_id, cycle_id, hostname)`

Meaning:

- `rover_x`, `rover_y`, `rover_z` are the joined Qualisys rover coordinates for that experiment and cycle.
- `position_available == 1` means the position row reported `position_status == ok`.
- `csi_real + 1j * csi_imag` is the cable-corrected complex RF quantity per host and cycle.
- `csi_available == 1` means that host contributed a usable RF record for that experiment and cycle.

Important structural detail:

- `cycle_id` is a shared coordinate axis across the dataset, not a guarantee that every experiment has RF data for every listed cycle.
- Always use `csi_available` to determine which cycles and hostnames are actually populated for a given experiment.
- Rover coordinates can still be `NaN` even when a CSI row exists, so check both `position_available` and finite coordinates before using a rover pose.
- Consecutive duplicate rover positions may already have been filtered out by the extractor before the NetCDF file is written.

The dataset also carries summary metadata in `ds.attrs`. Current extractor output includes:

- `description`
- `csi_definition`
- `csi_pair_count`
- `matched_position_rows`
- `missing_position_rows`
- `valid_position_rows`
- `invalid_status_rows`
- `missing_coordinate_rows`
- `invalid_or_missing_position_rows`
- `duplicate_position_filter_enabled`
- `duplicate_position_filter_rover_config_path`
- `duplicate_position_filter_min_spacing_mm`
- `duplicate_position_filter_axis_tolerance_m`
- `duplicate_position_filtered_cycles`
- `duplicate_position_filtered_position_rows`
- `duplicate_position_filtered_csi_rows`
- `last_measurement_timestamp`
- `last_measurement_timestamp_source`
- `last_measurement_source_path`

The timestamp metadata is taken from the processed source file modification time in the current extractor implementation.

Reconstruct the complex RF quantity as:

```python
csi = ds["csi_real"] + 1j * ds["csi_imag"]
phase = np.angle(csi)
amplitude = np.abs(csi)
```

A safe starting pattern for one experiment is:

```python
exp = ds.sel(experiment_id="EXP003")
cycle_mask = exp["csi_available"].any(dim="hostname")
cycle_ids = exp["cycle_id"].values[cycle_mask.values]

csi = (exp["csi_real"] + 1j * exp["csi_imag"]).sel(cycle_id=cycle_ids)
position_ok = (
    exp["position_available"].sel(cycle_id=cycle_ids) > 0
) & (
    np.isfinite(exp["rover_x"].sel(cycle_id=cycle_ids))
) & (
    np.isfinite(exp["rover_y"].sel(cycle_id=cycle_ids))
) & (
    np.isfinite(exp["rover_z"].sel(cycle_id=cycle_ids))
)
```

Interpretation notes:

- `phase` here is a calibrated complex channel phase, not an angle-of-arrival.
- `position_available` should be checked before trusting the rover coordinates for a cycle.
- Missing RF rows usually mean a host did not produce a valid record for that cycle.

### Step 5. Read the Acoustic Files

For acoustic data, treat each CSV as one acoustic event.

Work through it like this:

1. Inspect the files in `acoustic/results/`.
2. Use timestamps and the acoustic client logs to place them in run order.
3. Read the rows by `microphone_label` and `microphone_coordinates`.
4. Treat the final `chirp_excitation` row as the transmitted reference waveform.
5. Apply your own deconvolution or RIR extraction flow if needed.

Important limitation:

- Acoustic files are not yet keyed directly by `experiment_id` and `cycle_id`, so they are less convenient to join than the RF files.

### Step 6. Check the Data Visually

For quick inspection, use:

- `processing/plot_csi_positions.ipynb`

This is the fastest way to verify:

- the rover trajectory
- the RF phase evolution
- CSI-derived amplitude or power patterns

Example videos from the latest processed run:

- [Phase and rover position MP4](processing/phase_rover_EXP003.mp4)
- [Power and rover position MP4](processing/power_rover_EXP003.mp4)

These MP4 files show one processed experiment as cycle-by-cycle animations, with RF phase or RF power over the ceiling receiver aperture on the top panel and the rover position on the bottom panel. The notebook now exports MP4 by default because generation is faster and the files are much smaller than the GIF versions.

### Step 7. Use the Detailed Reference When Needed

The sections above this tutorial explain the dataset structure in detail:

- `What One Measurement Means` explains the cycle concept.
- `Data Products and File Locations` explains which file is produced by which component.
- `How to Interpret and Join the Data` explains RF timing, RF geometry, cable correction, and acoustic caveats.

Use those sections when you need the exact semantics behind a field or file, but the workflow above should be enough to start analyzing the data.

## View 2: Operator and Developer Reference

If you need to deploy the system, run new measurements, debug the control plane, or extend the software, the remaining sections are the reference view.

## Runtime Architecture

There are two layers in the repository.

### 1. Deployment Layer

Responsible for:

- selecting tiles
- copying the repository and settings
- installing dependencies
- starting long-lived services on the tiles

Main scripts:

- `server/setup-clients.py`
- `server/update-experiment.py`
- `server/run-clients.py`

### 2. Orchestration Layer

Responsible for:

- coordinating one complete cycle across rover, position logging, acoustic, and RF

Main scripts:

- `server/zmq_orchestrator.py`
- `client/rover/ZMQclient_rover.py`
- `acoustic/ZMQclient_acoustic.py`
- `server/record/RF-orchestrator.py`
- `client/run-ref.py`

Important distinction:

- `server/run-clients.py` manages services on the tiles.
- `server/zmq_orchestrator.py` is the outer measurement loop and must be run separately.

## Configuration Files

### `experiment-settings.yaml`

This is the main deployment and runtime configuration file.

It controls:

- tile selection through `tiles`
- orchestrator host discovery through `server.host`
- RF synchronization via `rf_sync`
- tile-side worker scripts through `client_scripts`
- runtime storage through `experiment_config.storage_path`
- position logging through `positioning`

### `server/serverConfig.yaml`

This controls the outer orchestrator loop:

- `experiment_id`
- `bind`
- `cycles`
- `meas_start`
- `timeouts.poll_ms`

### `client/rover/config.yaml`

This controls the rover sweep plan:

- sweep bounds
- spacing schedule
- cycle behavior
- work area
- serial settings

## Deployment and Acquisition

Clone the repository:

```bash
git clone https://github.com/techtile-by-dramco/ELLIIIT-dataset-26.git
cd ELLIIIT-dataset-26
```

Prepare the server environment:

```bash
cd server
./setup-server.sh
source bin/activate
cd ..
```

Prepare tiles:

```bash
python server/setup-clients.py --ansible-output
python server/update-experiment.py --ansible-output
```

Start tile-side services:

```bash
python server/run-clients.py --start
```

Run the orchestrator:

```bash
python server/zmq_orchestrator.py server --config server/serverConfig.yaml --experiment-settings experiment-settings.yaml
```

If you want to run the long-lived clients manually instead of through tile services, the main commands are:

```bash
python acoustic/ZMQclient_acoustic.py --id acoustic
python client/rover/ZMQclient_rover.py --config-file client/rover/config.yaml
python server/record/RF-orchestrator.py --id rf --experiment-settings experiment-settings.yaml
```

The reference transmitter and RF tile workers are usually started through `client_scripts` in `experiment-settings.yaml` via `server/run-clients.py`. If you want to run `client/run-ref.py` manually, use the same RF arguments that are configured there, for example:

```bash
python client/run-ref.py \
  --config-file experiment-settings.yaml \
  --args "type=b200" \
  --freq 920e6 \
  --rate 250e3 \
  --duration 1E6 \
  --channels 0 \
  --wave-ampl 0.8 \
  --gain 73 \
  --waveform sine \
  --wave-freq 0
```

## RF Post-Processing

The active RF post-processing path is:

- `processing/extract_csi_from_smb_v2.py`

It performs:

1. Scan each hostname folder for RF result files.
2. Parse JSON, JSON-lines, text, YAML, CSV, or legacy archive formats.
3. Recover `experiment_id`, `cycle_id`, `hostname`, `pilot_phase`, and `pilot_amplitude`.
4. Apply the cable correction from `client/ref-RF-cable.yml`.
5. Join with rover positions from `server/record/data/exp-*-positions.csv`.
6. Drop consecutive duplicate-position cycles using `grid.min_spacing / 5` per axis from `client/rover/config.yaml`.
7. Write one NetCDF dataset.

The resulting dataset contains:

- coordinates:
  - `experiment_id`
  - `cycle_id`
  - `hostname`
- rover variables with shape `(experiment_id, cycle_id)`:
  - `rover_x`
  - `rover_y`
  - `rover_z`
  - `position_available`
- RF variables with shape `(experiment_id, cycle_id, hostname)`:
  - `csi_real`
  - `csi_imag`
  - `csi_available`
- dataset attributes:
  - `description`
  - `csi_definition`
  - coverage counts such as `csi_pair_count`, `valid_position_rows`, and `invalid_or_missing_position_rows`
  - duplicate-position filter metadata such as `duplicate_position_filter_axis_tolerance_m` and `duplicate_position_filtered_cycles`
  - the most recent processed measurement metadata: `last_measurement_timestamp`, `last_measurement_timestamp_source`, `last_measurement_source_path`

Practical xarray interpretation:

- The file is an RF-plus-position cube indexed by experiment, orchestrator cycle, and receiver hostname.
- `csi_real + 1j * csi_imag` reconstructs the complex RF measurement.
- `csi_available` is the authoritative mask for whether a given host/cycle entry exists.
- `position_available` plus finite `rover_x`, `rover_y`, `rover_z` indicates whether the rover pose is usable for that CSI cycle.
- In the active `extract_csi_from_smb_v2.py` path, only experiment/cycle pairs that appear in the extracted CSI rows are kept. Cycles are still stored on a shared `cycle_id` axis, so sparse regions are normal when multiple experiments are combined.
- Some original rover stops may be absent from the NetCDF file because the extractor filters consecutive duplicate positions before writing the dataset.

Useful commands:

```bash
python processing/extract_csi_from_smb_v2.py
python processing/extract_csi_from_smb_v2.py --max-measurements 10
python processing/extract_csi_from_smb_v2.py --output processing/csi_custom.nc
```

To summarize RF runtime failures:

```bash
python processing/summarize_error_logs_from_smb.py
python processing/summarize_error_logs_from_smb.py --host wallEast --tail 20
python processing/summarize_error_logs_from_smb.py --error-type CLOCK_LOCK_FAILED --json-output processing/error_summary.json
```

## Operational Utilities

### Rover Sweep Preview

To preview the configured sweep before running the rover:

```bash
python client/rover/ZMQclient_rover.py --preview-sweeps --config-file client/rover/config.yaml
```

This writes a PNG preview next to the rover config by default.

### Socket Cleanup

If ZMQ ports remain occupied after an interrupted run:

```bash
server/close-sockets.sh --dry-run
server/close-sockets.sh
```

This is useful when ports such as `5555`, `5557`, `5558`, `5559`, `5678`, `5679`, or `50001` remain bound after a crash or a suspended process.

## Summary

For data users, the core mental model is:

- one orchestrator cycle equals one rover pose
- that pose gets one position sample
- then one acoustic measurement
- then one RF measurement cycle

For RF analysis, `experiment_id` and `cycle_id` are the main keys.

For acoustic analysis, the data is recorded per cycle but currently needs timestamp and log-based matching because the saved CSVs do not yet embed those identifiers directly.
