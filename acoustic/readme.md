# Acoustic Measurement System

A Python based system for performing and analysing room impulse response (RIR) measurements using a NI-DAQ acquisition setup, exponential sine sweeps (ESS), and a ZMQ-based orchestration layer.

---

## Overview

The system emits a logarithmic chirp excitation signal through a speaker and simultaneously records the response across an array of up to 98 distributed microphones. RIRs can be extracted via deconvolution and saved to CSV for post-processing and comparison against simulated responses from [pyroomacoustics](https://pyroomacoustics.readthedocs.io/).

Measurements can be triggered locally or remotely via a ZMQ server/client protocol, with per-measurement parameter overrides (speaker position, chirp parameters, etc.).

---

## Repository Structure

REWRITE