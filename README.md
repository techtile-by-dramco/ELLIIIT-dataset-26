# Dataset Creation in Techtile

This repository contains the code and measurement flow used to build synchronized acoustic and RF datasets inside the Techtile environment.

The documentation is split into two views:

- View 1 is for dataset users who mainly want to understand what was measured and how the data was collected at a high level.
- View 2 is for people who want to run the code, debug the control plane, or extend the project.

<details>
<summary><strong>1. Dataset Collection Overview for Data Users</strong></summary>

## Overview

The dataset combines multi-microphone ultrasonic measurements with distributed RF measurements, sampled over a dense three-dimensional spatial grid inside Techtile.

Each measurement snapshot is paired with ground-truth position information from a Qualisys motion capture system. This provides reproducible geometry for both the ultrasonic speaker and the RF user-equipment antenna.

## Joint Acoustic and RF Measurement Setup

The ultrasonic speaker and the RF UE antenna are mechanically co-located and mounted on the same positioning rig. They move jointly through the measurement volume so that each sampled point has a direct acoustic and RF counterpart.

![holder](holder.jpg)

This gives:

- Spatial alignment between acoustic and RF observations
- A shared geometric reference for both modalities
- Ground-truth 3D position information for each snapshot

## Acoustic Data

The acoustic side uses the Techtile DAQ infrastructure for synchronized excitation and capture.

- An omnidirectional ultrasonic speaker is placed at each grid point.
- Roughly 80 microphones distributed through the walls and ceiling record the wavefield.
- A shared DAQ clock and trigger provide coherent, phase-consistent sampling across channels.

For each position, the acoustic dataset contains:

- Raw multi-channel received chirp recordings
- Time-synchronized microphone captures
- Metadata such as timestamps and absolute speaker position

Post-processing reconstructs impulse responses and propagation structure from the synchronized recordings.

## RF Data

The RF side combines multiple distributed antenna groups inside Techtile.

- The ceiling array supports phase-calibrated measurements.
- The wall arrays support time- and frequency-synchronized measurements without global phase calibration.
- Narrowband pilot signaling is used to derive stable amplitude and phase information.

For each position, the RF dataset contains:

- Raw complex IQ samples
- Multi-antenna synchronized captures
- Calibration-related data for the calibrated RF aperture
- Absolute UE antenna position from Qualisys

## Measurement Procedure

Measurements are taken on a three-dimensional spatial grid. At each point:

1. The shared acoustic and RF rig moves to the next position.
2. A ground-truth position sample is captured.
3. An acoustic measurement is recorded.
4. An RF measurement is recorded.

This produces paired acoustic and RF measurements with known geometry.

## Artificial Path and Trajectory Construction

Because the dataset consists of dense, static, geometrically referenced samples, it can be re-sampled into synthetic motion trajectories. A synthetic trajectory is built by ordering static grid positions as if they were successive time steps.

Each selected point provides:

- Coherent RF IQ data
- Coherent acoustic impulse responses
- Absolute ground-truth position

## Unique Aspects of the Dataset

- Joint acoustic and RF sensing on one shared moving rig
- Dense three-dimensional spatial sampling
- Absolute Qualisys ground-truth positioning
- DAQ-synchronized acoustic acquisition
- Combination of coherent and non-coherent RF apertures
- Static measurements that can be re-used as synthetic trajectories

</details>

<details>
<summary><strong>2. Internal Workings, Deployment, and Extension Guide</strong></summary>

## Architecture

There are two distinct layers in this project:

1. The deployment layer, based on `experiment-settings.yaml`, `server/update-experiment.py`, and `server/run-clients.py`, installs and starts long-running worker scripts on the selected tiles.
2. The orchestration layer, based on `server/zmq_orchestrator.py`, coordinates one full measurement cycle across rover, acoustic, reference, and RF services.

`server/zmq_orchestrator.py` is launched separately. It is not started automatically by `server/run-clients.py`.

## Bootstrap and Deployment

Clone the repository:

```bash
git clone https://github.com/techtile-by-dramco/ELLIIIT-dataset-26.git
```

Bootstrap the server virtual environment and pull tile-management dependencies:

```bash
cd ELLIIIT-dataset-26/server
./setup-server.sh
source bin/activate
cd ..
```

Configure `experiment-settings.yaml`:

- Set `server.host` to the host name or IP that tile workers should use.
- Select tile group(s) under `tiles`.
- Configure RF parameters and `rf_sync`.
- Define tile-side services in `client_scripts`.
- Set any required `extra_packages`.
- Configure the `positioning` block if you want post-move Qualisys logging from the orchestrator.

`server/update-experiment.py` reads `client_scripts`. The older top-level `client_script_name` and `client_script_args` fields remain legacy compatibility fields and are not used by the multi-script deployment path.

Prepare tiles:

```bash
python server/setup-clients.py --ansible-output
```

Optional flags:

- `--skip-apt`
- `--repos-only`
- `--install-only`
- `--check-uhd-only`

Push code and settings to tiles:

```bash
python server/update-experiment.py --ansible-output
```

Start or stop the tile-side services:

```bash
python server/run-clients.py --start
# or
python server/run-clients.py --stop
```

## Orchestrated Run

For a full orchestrated experiment, the following long-lived processes are expected:

```bash
# tile-side workers from experiment-settings.yaml
python server/update-experiment.py --ansible-output
python server/run-clients.py --start

# outer control plane
python server/zmq_orchestrator.py server --config server/serverConfig.yaml --experiment-settings experiment-settings.yaml
python acoustic/ZMQclient_acoustic.py --id acoustic
python client/rover/ZMQclient_rover.py --config-file client/rover/config.yaml
python server/record/RF-orchestrator.py --id rf --experiment-settings experiment-settings.yaml
```

If `--connect` is omitted, the ZMQ clients derive the orchestrator endpoint from `server.host` and `server.orchestrator_port` in `experiment-settings.yaml`.

## Runtime Roles

Important distinctions between the runtime processes:

- `server/zmq_orchestrator.py` is the outer ROUTER-based coordinator.
- `server/record/RF-orchestrator.py` is the `rf` client as seen by `server/zmq_orchestrator.py`.
- `client/run_reciprocity.py`, `client/run_uncalibrated.py`, and `client/usrp_pilot.py` are RF tile workers started via `client_scripts`. They are not the outer `rf` client.
- `client/run-ref.py` is a continuous support transmitter and the mandatory `ref` readiness client.
- `client/rover/ZMQclient_rover.py` is the rover client used by the orchestrator.
- `client/rover/simulate_roverMove.py` is only a standalone rover test script.
- `server/run_server.py` and `server/utils/server_com.py` are the older server messaging path using `server.messaging_port` and `server.sync_port`, not the outer `server/zmq_orchestrator.py` ROUTER port.

## Detailed Measurement Procedure

### Spatial Sampling

Measurements are performed on a three-dimensional grid with discrete intervals in `x`, `y`, and `z`. The ultrasonic speaker and RF UE antenna move together, so every measurement index maps to one shared physical location.

### Schematic Flow

```text
Per cycle (cycle_id = k, experiment_id = EXP):

  zmq_orchestrator
      |
      |-- MOVE -----------------------> rover client
      |<------------------------- MOVE_DONE
      |
      |-- capture position sample ----> exp-<id>-positions.csv
      |
      |-- START_MEAS -----------------> acoustic client
      |<------------------------- MEAS_DONE
      |
      |-- START_MEAS -----------------> RF-orchestrator client
                                      |
                                      |-- wait ALIVE x N on alive_port
                                      |-- wait pre_sync_delay_s
                                      |-- publish SYNC "<cycle_id> <experiment_id>" on sync_port
                                      |-- wait DONE  x N on done_port
                                      |-- append server/record/data/exp-<experiment_id>.yml
      |<------------------------- MEAS_DONE
```

### Control-Plane Handshake

There is no literal `OK` message in the outer protocol. Progress is controlled by message types and matching IDs:

- Reference host: sends one `HELLO` as `ref`, then responds to periodic `PING` with `PONG`.
- Rover host: receives `MOVE`, executes the motion, and replies `MOVE_DONE`.
- Acoustic host: receives `START_MEAS`, performs one acoustic capture, and replies `MEAS_DONE`.
- RF orchestrator host: receives `START_MEAS`, runs one RF fan-out cycle, and replies `MEAS_DONE`.
- RF tile workers: send `ALIVE`, wait for `SYNC "<cycle_id> <experiment_id>"`, perform one local RF action, then send `DONE`.

Fixed outer identities:

- `ref` for `client/run-ref.py`
- `rover` for [`client/rover/ZMQclient_rover.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/client/rover/ZMQclient_rover.py)
- `acoustic` for [`acoustic/ZMQclient_acoustic.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/acoustic/ZMQclient_acoustic.py)
- `rf` for [`server/record/RF-orchestrator.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/server/record/RF-orchestrator.py)

### Position Logging

When the `positioning` block is enabled in `experiment-settings.yaml`, `server/zmq_orchestrator.py` captures a position sample immediately after each successful rover move and appends it to `server/record/data/exp-<experiment_id>-positions.csv`.

That log contains:

- Experiment, cycle, and measurement identifiers
- Move status
- Position status and timestamp
- `x`, `y`, and `z`
- A combined `position` field
- Rotation matrix data when available

## RF Synchronization Internals

For orchestrated runs, RF synchronization is handled inside `server/record/RF-orchestrator.py` during each `START_MEAS` command from `server/zmq_orchestrator.py`.

Per `START_MEAS`, exactly one RF cycle is executed:

1. Wait for `rf_sync.num_subscribers` ALIVE messages on `rf_sync.alive_port` (default `5558`).
2. Wait `rf_sync.pre_sync_delay_s` seconds to avoid PUB/SUB slow-joiner loss.
3. Publish one `SYNC` message on `rf_sync.sync_port` (default `5557`) with payload `<cycle_id> <experiment_id>`.
4. Wait for `rf_sync.num_subscribers` DONE messages on `rf_sync.done_port` (default `5559`).

`experiment_id` is supplied by the outer orchestrator and reused in `server/record/data/exp-<experiment_id>.yml`.

The RF worker mode is selected per tile group in `experiment-settings.yaml` under `client_scripts`:

- `client/run_reciprocity.py`: receives one pilot RX capture and appends one JSON-line result record per successful RF capture to `data_<HOSTNAME>_<experiment_id>.txt`. The active path no longer stores raw IQ captures.
- `client/run_uncalibrated.py`: receives one pilot RX capture and appends the same JSON-line result schema to `data_<HOSTNAME>_<experiment_id>.txt`. It also no longer stores raw IQ captures in the active path.
- `client/usrp_pilot.py`: transmits the configured pilot waveform for the selected phase.
- `client/run-ref.py`: provides the continuous reference transmission and readiness registration used by the outer control plane.

For both `client/run_reciprocity.py` and `client/run_uncalibrated.py`, each successful RF capture stores:

- `timestamp_utc`
- `hostname`
- `file_name` as `data_<HOSTNAME>_<experiment_id>_<cycle_id>`
- `experiment_id`
- `cycle_id`
- `pilot_phase` in radians
- `pilot_phase_deg` in degrees
- `pilot_amplitude`
- `avg_amplitude_ch0` and `avg_amplitude_ch1`
- `rms_amplitude_ch0` and `rms_amplitude_ch1`
- `max_i_ch0`, `max_i_ch1`, `max_q_ch0`, and `max_q_ch1`
- `freq_offset_ch0_before_hz`, `freq_offset_ch0_after_hz`, `freq_offset_ch1_before_hz`, and `freq_offset_ch1_after_hz`
- `captured_samples`

Both scripts also append JSON-line runtime diagnostics to `error.log`. Each error entry stores:

- `timestamp_utc`
- `hostname`
- `error_type`
- `message`
- `experiment_id`, `cycle_id`, and `file_name` when that context is available
- any extra error-specific fields, such as `capture_type`, buffer sizes, metadata error names, or numeric measurements related to the failure

## RF Processing

Position logs are written by the orchestrator to `server/record/data/exp-<experiment_id>-positions.csv`.

The legacy CSI extraction helper is [`processing/extract_csi_from_smb.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/processing/extract_csi_from_smb.py). It builds one xarray/NetCDF file by:

- scanning each hostname folder for measurement archives named `data_<HOSTNAME>_<experiment_id>_<cycle_id>*.npz`
- extracting `pilot_phase` and `pilot_amplitude` directly when present, or deriving them from legacy `pilot_iq` stored inside the archive
- applying the cable correction from `client/ref-RF-cable.yml`
- joining RF CSI with rover positions on `experiment_id` and `cycle_id`
- writing a dataset with `csi_real`, `csi_imag`, `rover_x`, `rover_y`, `rover_z`, and availability masks

Useful commands:

```bash
python processing/extract_csi_from_smb.py
python processing/extract_csi_from_smb.py --max-measurements 10
python processing/extract_csi_from_smb.py --data-root /path/to/data/root
```

On Windows, `processing/extract_csi_from_smb.py` defaults `--data-root` to the UNC network path `\\10.128.48.9\elliit`.

For the active JSON-based RF output format, use [`processing/extract_csi_from_smb_v2.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/processing/extract_csi_from_smb_v2.py). It:

- scans each hostname folder for result files named `data_<HOSTNAME>_*.json`, `*.jsonl`, `*.txt`, or `*.log`
- reads the JSON records written by the active RF workers
- applies the same cable correction and position join as the legacy extractor
- ignores position-only experiments or cycles that have no extracted CSI rows
- writes the NetCDF dataset to `processing/csi_<experiment_id>.nc` by default (or joins multiple experiment IDs when one dataset spans more than one)

Useful commands:

```bash
python processing/extract_csi_from_smb_v2.py
python processing/extract_csi_from_smb_v2.py --max-measurements 10
python processing/extract_csi_from_smb_v2.py --data-root /path/to/data/root
```

To summarize runtime failures recorded by the RF workers, use [`processing/summarize_error_logs_from_smb.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/processing/summarize_error_logs_from_smb.py). It scans each host folder for `error.log`, reads the JSON-line entries, and prints grouped counts by error type, hostname, experiment, and capture type.

Useful commands:

```bash
python processing/summarize_error_logs_from_smb.py
python processing/summarize_error_logs_from_smb.py --host wallEast --tail 20
python processing/summarize_error_logs_from_smb.py --error-type CLOCK_LOCK_FAILED --json-output processing/error_summary.json
```

For quick inspection and plotting, use [`processing/plot_csi_positions.ipynb`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/processing/plot_csi_positions.ipynb). The notebook reads the NetCDF dataset, plots CSI phase as `np.angle(csi_real + 1j * csi_imag)` in degrees versus cycle ID, and plots the 2D rover trajectory.

## Operational Utilities

### Socket Cleanup

If a local ZMQ process is suspended or left behind, use [`server/close-sockets.sh`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/server/close-sockets.sh). The script checks the repository's server-side ZMQ bind points and known runtime entrypoints, resumes suspended jobs with `CONT`, then sends `TERM` and `KILL` if needed.

Typical usage:

```bash
server/close-sockets.sh --dry-run
server/close-sockets.sh
```

The script covers the main server-side ZMQ roles defined in:

- `server/zmq_orchestrator.py`
- `server/record/RF-orchestrator.py`
- `server/record/sync-server.py`
- `server/run_server.py` and `server/utils/server_com.py`
- `client/rover/ZMQserverTest_rover.py`
- `client/run_reciprocity.py`
- `client/usrp_pilot.py`

Use it when a port such as `5555`, `5557`, `5558`, `5559`, `5678`, `5679`, or `50001` remains occupied after an interrupted run.

</details>
