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

- Set server host/IP
- Select tile group(s)
- Configure RF parameters
- Set `client_script_name` and `client_script_args`
- Set `extra_packages` (apt packages to install on tiles)

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

## Joint Acoustic and RF Measurement Setup

The ultrasonic speaker and the RF UE antenna are mechanically co-located and mounted on the same positioning rig. They are moved jointly across a predefined XYZ grid inside the Techtile measurement volume, enabling a one-to-one correspondence between acoustic and RF observations at each sampled position.

**TODO ADD PICTURE OF HOLDER**

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

The RF client mode is selected in `experiment-settings.yaml` via `client_script_name`.

- `client/run_reciprocity.py`: per SYNC cycle, performs one pilot RX capture and one internal loopback capture, then saves `<file_name>_iq.npz` with `pilot_iq`, `loopback_iq`, `pilot_phase`, `pilot_amplitude`, `loopback_phase`, `loopback_amplitude`, `hostname`, `meas_id`, and `file_name`.
- `client/run_uncalibrated.py`: per SYNC cycle, performs pilot RX only (no loopback, no TX/BF runtime path). During pilot mode, both RX channels (0 and 1) use `TX/RX` antennas. It saves `<file_name>_iq.npz` with `pilot_iq`, `hostname`, `meas_id`, and `file_name`.

## Measurement Procedure

### Spatial Sampling

Measurements are performed on a three-dimensional grid with discrete intervals in x, y, and z. The speaker and UE antenna move jointly across the predefined XYZ grid. Each grid point contains synchronized acoustic and RF measurements and an absolute 3D ground-truth position from Qualisys.

### RF Synchronization Protocol

`server/record/sync-server.py` currently drives the RF run loop in two phases:

1. Wait for all clients to send an ALIVE/ready message on port `5558`.
2. Publish one SYNC message on port `5557` with `<meas_id> <unique_id>`.
3. Wait for all clients to send a post-capture DONE message, also on port `5558`.
4. Sleep for the configured interval and repeat.

Note: In this flow, the legacy server `data-port` (`5559`) is not used.

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
