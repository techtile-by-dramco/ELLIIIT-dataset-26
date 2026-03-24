# Dataset Creation in Techtile

## Overview

This document describes the methodology used to construct synchronized acoustic and RF propagation datasets inside the Techtile environment. The dataset combines multi-microphone ultrasonic measurements with large-scale distributed RF measurements, both sampled over a dense three-dimensional spatial grid.

All measurements include ground-truth position references obtained using a Qualisys motion capture system. The dataset also contains spatial coordinates of the measurement equipment for reproducible geometric reconstruction, including the absolute 3D position of both the ultrasonic speaker and the RF UE antenna for every measurement snapshot.

## Experiment Setup

1. Clone the repository in your VM:

```bash
git clone https://github.com/techtile-by-dramco/ELLIIIT-dataset-26.git
```

2. Bootstrap the server virtual environment and pull tile-management dependencies:

```bash
cd ELLIIIT-dataset-26/server
./setup-server.sh
source bin/activate
cd ..
```

3. Configure `experiment-settings.yaml`:

- Set `server.host` to the host name or IP that the RF tile workers should connect to
- Select tile group(s)
- Configure RF parameters
- Configure RF sync settings under `rf_sync` (`num_subscribers`, `host`, `sync_port`, `alive_port`, `done_port`, `pre_sync_delay_s`, `wait_timeout_s`)
- Define the tile-side services under `client_scripts`
- Set `extra_packages` (apt packages to install on tiles)

`server/update-experiment.py` reads `client_scripts`. The older top-level `client_script_name` and `client_script_args` fields are not used by the multi-script deployment flow.

4. Prepare tiles (apt, repositories, UHD) from repo root while the server venv is active:

```bash
python server/setup-clients.py --ansible-output
```

Optional flags:

- `--skip-apt`
- `--repos-only`
- `--install-only`
- `--check-uhd-only`

5. Push code and settings to tiles:

```bash
python server/update-experiment.py --ansible-output
```

6. Start or stop the experiment service:

```bash
python server/run-clients.py --start
# or
python server/run-clients.py --stop
```

### Deployment and Control Plane

There are two different layers in this project:

1. `experiment-settings.yaml` + `server/update-experiment.py` + `server/run-clients.py`
   This installs and starts long-running worker scripts on the tiles listed in `client_scripts`.
2. `server/zmq_orchestrator.py`
   This is the outer measurement coordinator. It does not get started by `run-clients.py`; you launch it separately on the server.

For a full orchestrated run, the following long-lived processes are expected:

```bash
# tile-side workers from experiment-settings.yaml
python server/update-experiment.py --ansible-output
python server/run-clients.py --start

# outer control plane
python server/zmq_orchestrator.py server --config server/serverConfig.json --experiment-settings experiment-settings.yaml
python acoustic/ZMQclient_acoustic.py --connect tcp://SERVER:5555 --id acoustic
python client/rover/ZMQclient_rover.py --connect tcp://SERVER:5555 --config client/rover/config.json
python server/record/RF-orchestrator.py --connect tcp://SERVER:5555 --id rf --experiment-settings experiment-settings.yaml
```

Replace `SERVER` with the host running `server/zmq_orchestrator.py`.

Important distinctions:

- `server/record/RF-orchestrator.py` is the `rf` client seen by `server/zmq_orchestrator.py`.
- `client/run_reciprocity.py`, `client/run_uncalibrated.py`, and `client/usrp_pilot.py` are not the `rf` client. They are RF tile workers started through `client_scripts`.
- `client/run-ref.py` is a continuous support transmitter and does not wait for a per-measurement start command.
- `client/rover/simulate_roverMove.py` is a standalone test script. For orchestrated rover control, use `client/rover/ZMQclient_rover.py`.
- `server/zmq_orchestrator.py` uses [`server/serverConfig.json`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/server/serverConfig.json) for its ROUTER bind address and port. That is separate from the `server.messaging_port` and `server.sync_port` fields used by the older `server/run_server.py` path.

## Joint Acoustic and RF Measurement Setup

The ultrasonic speaker and the RF UE antenna are mechanically co-located and mounted on the same positioning rig. They are moved jointly across a predefined XYZ grid inside the Techtile measurement volume, enabling a one-to-one correspondence between acoustic and RF observations at each sampled position.

![holder](holder.jpg)

This ensures:

- Spatial alignment between acoustic and RF measurements
- Identical geometric reference for both domains
- Consistent comparison between acoustic and electromagnetic propagation

For each grid point, the dataset contains:

- Absolute 3D position of the ultrasonic speaker (Qualisys reference)
- Absolute 3D position of the UE RF antenna (Qualisys reference)
- Timestamped Qualisys ground-truth reference

## Acoustic Dataset

### Hardware Configuration

#### DAQ-Based Time Synchronization

All microphones and the ultrasonic speaker are connected to the Techtile data acquisition (DAQ) system. The DAQ provides a shared sampling clock and deterministic triggering, ensuring precise time synchronization across all acoustic channels.

As a result:

- All microphones are sampled coherently
- The transmitted chirp is time-aligned with the receiver recordings
- Phase-consistent and coherent impulse response estimation is possible in the acoustic domain

This infrastructure enables coherent reading and transmitting in the acoustic domain, analogous to synchronized RF measurements.

#### Transmitter (UE Side)

An omnidirectional ultrasonic speaker is placed at predefined grid locations within the Techtile volume. The speaker is driven directly by the synchronized DAQ system, ensuring deterministic emission timing.

#### Receiver (BS Side)

Approximately 80 microphones are distributed throughout the walls and ceiling of the Techtile infrastructure. The microphones:

- Share the DAQ reference clock
- Are tightly time-synchronized
- Have calibrated and fixed three-dimensional positions

This configuration enables coherent multi-microphone capture of the acoustic wavefield.

### Excitation Signal

The transmitted acoustic waveform is a linear chirp sweeping from 20 kHz to 40 kHz. The wide bandwidth enables high temporal resolution in impulse response estimation.

### Recorded Data

For each grid position, the following data are recorded:

- Raw received chirp signals at all microphones
- Multi-channel synchronized recordings via the Techtile DAQ
- Metadata including absolute speaker position (Qualisys reference), microphone positions, and timestamps

### Post-Processing

Processing scripts perform matched filtering or deconvolution to estimate a room impulse response per microphone. Due to the coherent DAQ-based synchronization, phase-consistent impulse responses can be reconstructed across the full microphone array.

This yields time-of-arrival and multipath structure information and provides a spatially dense acoustic propagation characterization.

## RF Dataset

<details>
<summary><strong>RF Dataset (Optional, Click to Expand)</strong></summary>

### Infrastructure Configuration

#### Ceiling Array (Phase-Calibrated)

The ceiling contains 42 antennas, with a potential extension to 84 antennas. These antennas share a common reference clock and local oscillator distribution (`run-ref.py`). Phase-calibrated captures are collected with `client/run_reciprocity.py`, enabling coherent distributed MIMO measurements.

#### Wall Arrays (Time/Frequency Synchronized)

Approximately 100 antennas are embedded in the walls. These antennas are time- and frequency-synchronized but not globally phase-calibrated, supporting amplitude-based and non-coherent processing.

### Transmitted Signal

The RF excitation consists of a narrowband single-carrier waveform to enable stable amplitude estimation and precise phase extraction. This pilot is in `client/usrp_pilot.py`.

### Recorded Data

For each grid position, the following RF data are recorded:

- Raw complex baseband IQ samples per antenna
- Time-aligned multi-antenna recordings
- Calibration data for the phase-calibrated ceiling array
- Absolute UE antenna position (Qualisys reference)

After calibration, per-antenna amplitude and phase are derived.

### Runtime Capture Modes

The RF worker mode is selected per tile group in `experiment-settings.yaml` under `client_scripts`.

- `client/run_reciprocity.py`: per RF `SYNC` cycle, performs one pilot RX capture and one internal loopback capture, then saves `<file_name>_iq.npz` with `pilot_iq`, `loopback_iq`, `pilot_phase`, `pilot_amplitude`, `loopback_phase`, `loopback_amplitude`, `hostname`, `meas_id`, and `file_name`.
- `client/run_uncalibrated.py`: per RF `SYNC` cycle, performs pilot RX only (no loopback, no TX/BF runtime path). During pilot mode, both RX channels (0 and 1) use `TX/RX` antennas. It saves `<file_name>_iq.npz` with `pilot_iq`, `hostname`, `meas_id`, and `file_name`.
- `client/usrp_pilot.py`: per RF `SYNC` cycle, transmits the pilot waveform for the configured phase on the selected host.
- `client/run-ref.py`: continuous reference transmission, independent of the per-cycle ZMQ handshake.

</details>

## Measurement Procedure

### Spatial Sampling

Measurements are performed on a three-dimensional grid with discrete intervals in x, y, and z. The speaker and UE antenna move jointly across the predefined XYZ grid. Each grid point contains synchronized acoustic and RF measurements and an absolute 3D ground-truth position from Qualisys.

### Schematic Flow (ASCII)

```text
Per cycle (cycle_id = k, experiment_id = EXP):

  zmq_orchestrator
      |
      |-- MOVE -----------------------> rover client
      |<------------------------- MOVE_DONE
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

### Client Handshake Summary

There is no literal `OK` message in the current code. The "go" signal is one of these messages:

- Rover host: waits for `MOVE`, performs the motion, replies `MOVE_DONE`.
- Acoustic host: waits for `START_MEAS`, runs one acoustic capture, replies `MEAS_DONE`.
- RF orchestrator host: waits for `START_MEAS`, then coordinates one RF fan-out cycle and replies `MEAS_DONE`.
- RF tile workers: send `ALIVE`, wait for `SYNC "<cycle_id> <experiment_id>"`, perform one local RF action, then send `DONE`.

The outer control-plane identities are fixed:

- `rover` for [`client/rover/ZMQclient_rover.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/client/rover/ZMQclient_rover.py)
- `acoustic` for [`acoustic/ZMQclient_acoustic.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/acoustic/ZMQclient_acoustic.py)
- `rf` for [`server/record/RF-orchestrator.py`](/mnt/c/Users/Calle/OneDrive/Documenten/GitHub/ELLIIIT-dataset-26/server/record/RF-orchestrator.py)

### RF Synchronization Protocol

For orchestrated runs, RF synchronization is handled inside `server/record/RF-orchestrator.py` during each `START_MEAS` command from `server/zmq_orchestrator.py`.

Per `START_MEAS`, exactly one RF cycle is executed:

1. Wait for `rf_sync.num_subscribers` ALIVE/ready messages on `rf_sync.alive_port` (default `5558`).
2. Wait `rf_sync.pre_sync_delay_s` seconds to avoid PUB/SUB slow-joiner loss after the per-cycle client reconnect.
3. Publish one SYNC message on `rf_sync.sync_port` (default `5557`) with payload `<cycle_id> <experiment_id>`.
4. Wait for `rf_sync.num_subscribers` DONE messages on `rf_sync.done_port` (default `5559`).

`experiment_id` is taken directly from the orchestrator (not locally generated) and is reused in RF logging output `server/record/data/exp-<experiment_id>.yml`.

The RF tile workers derive their connect host from `server.host` in `experiment-settings.yaml` and their ports from `rf_sync.*`, so the YAML now matches the runtime endpoint selection for `run_reciprocity.py`, `run_uncalibrated.py`, and `usrp_pilot.py`.

## Artificial Path and Trajectory Construction

Because the dataset consists of dense, static, geometrically referenced grid measurements, it can be re-sampled to construct synthetic dynamics. A synthetic trajectory is generated by selecting an ordered sequence of static grid points and interpreting this sequence as time evolution.

Each selected point provides:

- Coherent RF IQ data
- Coherent acoustic impulse responses
- Absolute ground-truth position (Qualisys)

By concatenating measurement snapshots, the dataset emulates motion under fully known geometry.

## Unique Aspects of the Dataset

- Large-scale distributed infrastructure across acoustic and RF domains
- Co-located ultrasonic speaker and RF UE antenna
- Absolute ground-truth positioning via Qualisys
- DAQ-based coherent synchronization in the acoustic domain
- Combination of coherent and non-coherent RF apertures
- Dense three-dimensional spatial sampling enabling synthetic trajectories
