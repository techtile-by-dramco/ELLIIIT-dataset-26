# Acoustic RIR Measurement System

This repository provides an automated setup for Room Impulse Response (RIR) measurements using National Instruments (NI) hardware and ZMQ messaging.

### File Breakdown

* **`acousticMeasurement.py`**: The core engine. Handles data acquisition via NI-DAQ and calculates the RIR.
* **`ZMQclient_acoustic.py`**: A ZMQ poller script that listens for commands and triggers measurements.
* **`simulate_acousticMeasurement.py`**: A simulator to test the measurement workflow.
* **`config.json`**: Central configuration for hardware settings.
* **`dicts.py`**: Containing microphone/speaker coordinates.
* **`EXAMPLE_...json`**: Template files showing the expected JSON format for starting measurements via ZMQ.
