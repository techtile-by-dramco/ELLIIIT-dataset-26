# Acoustic Measurement System

A Python based system for performing and analysing room impulse response (RIR) measurements using a NI-DAQ acquisition setup, exponential sine sweeps (ESS), and a ZMQ-based orchestration layer.

---

## Overview

The system emits a logarithmic chirp excitation signal through a speaker and simultaneously records the response across an array of up to 98 microphones. RIRs can be extracted via deconvolution and saved to CSV for post-processing and comparison against simulated responses from [pyroomacoustics](https://pyroomacoustics.readthedocs.io/).

Measurements can be triggered locally or remotely via a ZMQ server/client protocol, with per-measurement parameter overrides (speaker position, chirp parameters, etc.).

---

## Repository Structure

```
acoustics/
├── acousticMeasurement.py          # Core DAQ acquisition and RIR extraction engine
├── config.json                     # Global measurement parameters
├── dicts.py                        # Microphone and speaker channel/coordinate mappings
├── simulate_acousticMeasurement.py # Run a measurement locally (no ZMQ)
├── ZMQclient_acoustic.py           # ZMQ DEALER client (runs on measurement machine)
│
├── results/
│   ├── direct_rir/                 # CSVs from deconvolved RIR measurements
│   ├── save_ess/                   # CSVs with raw ESS recordings
│   ├── plot_deconv.py              # Plot measured vs simulated RIRs (deconv CSVs)
│   └── plot_save_ess.py            # Plot measured vs simulated RIRs (ESS CSVs)
│
└── server/
    ├── zmq_orchestrator.py         # ZMQ ROUTER server / experiment orchestrator
    └── measureConfig.json          # Per-cycle measurement plan for the orchestrator
```

---

## Configuration

All core parameters live in `config.json`:

| Parameter | Description |
|---|---|
| `room_dim` | Room dimensions `[x, y, z]` in metres |
| `speaker_coordinates` | Active speaker position `[x, y, z]` |
| `chirp_f_start / f_stop` | Sweep frequency range (Hz) |
| `chirp_duration` | Sweep duration (s) |
| `chirp_ampl` | Output amplitude (V) |
| `sample_rate` | DAQ sampling rate (Hz) |

---

## Measurement Methods

RIRs can be extracted using three methods, selected via the `method` argument in `calculateRIRS()`:

- **`save_ess`**: saves the raw recorded sweep for offline deconvolution (default)
- **`deconv`**: applies an inverse filter in the time domain immediately after acquisition
- **`fft`**: frequency-domain deconvolution via Wiener regularisation (NOT TESTED YET)

---

## Measurement Plan (`measureConfig.json`)

The orchestrator accepts a JSON measurement plan, a list of parameter dicts, one per cycle. Each entry overrides any subset of the default `config.json` values for that cycle. Only `speaker_coordinates` is required; all chirp parameters fall back to `config.json` defaults if omitted.

```json
[
  {"speaker_coordinates": [2.0, 2.0, 1.2]},
  {"speaker_coordinates": [3.0, 2.0, 1.2]},
  {
    "speaker_coordinates": [6.0, 3.0, 0.215],
    "chirp_f_start": 10000,
    "chirp_f_stop":  35000,
    "chirp_duration": 0.06,
    "chirp_ampl": 0.074
  }
]
```

If the number of cycles exceeds the number of entries, the last entry is repeated. This allows you to define a spatial sweep across speaker positions in a single experiment run without restarting the orchestrator.

---

## Running a Measurement

### Local (no ZMQ)
```bash
python simulate_acousticMeasurement.py
```

### Remote via ZMQ

Start the orchestrator server (with an optional per-cycle measurement plan):
```bash
python server/zmq_orchestrator.py server \
    --bind tcp://*:5555 \
    --experiment-id EXP001 \
    --cycle 5 \
    --meas-plan server/measureConfig.json
```

Start the measurement client on the acquisition machine:
```bash
python ZMQclient_acoustic.py --connect tcp://<server_ip>:5555 --id meas1
```

The server sends `START_MEAS` with per-cycle parameters; the client replies with `MEAS_DONE` once acquisition is complete.

---

## ZMQ Communication Flow
Server                            meas1 (ZMQclient_acoustic)
  |                                       |
  |-------- START_MEAS ------------------>|
  |         experiment_id, cycle_id,      |
  |         meas_id, speaker_coordinates, |
  |         chirp_f_start/stop/duration   |
  |                                       |
  |                      run_acoustic_measurement()
  |                      ├─ play chirp via NI-DAQ AO
  |                      ├─ record all mic channels (AI)
  |                      └─ save results to CSV
  |                                       |
  |<-------- MEAS_DONE -------------------|
  |          status, csv_file,            |
  |          n_mics, duration_s           |
  |                                       |
  [meas_id increments, next cycle starts] |
  |                                       |

```

## Output CSV Format

Each measurement is saved as a CSV with one row per microphone:

```
speaker, duration, f_start, f_stop, chirp_amp, microphone_coordinates, microphone_label, values
[4.555, 2.645, 0.215], 0.03, 20000, 40000, 0.075, (7.408, 4, 2.066), A11, [0.002, ...]
...
chirp_excitation, chirp_excitation, [0.075, ...]
```

Channels with hardware faults are written with `values = unused`.

---

## Post-processing & Plotting

From the `results/` directory, run either plotting script to compare measured and pyroomacoustics-simulated RIRs for a random subset of microphones:

```bash
python plot_deconv.py   # for direct_rir/ CSVs
python plot_save_ess.py # for save_ess/ CSVs (deconvolves on the fly)
```

Both scripts normalise and overlay the measured and simulated envelopes and save a high-resolution figure (`rir_comparison_*.png`).

---

## Dependencies

```
numpy
scipy
matplotlib
pyroomacoustics
pyzmq
nidaqmx
```