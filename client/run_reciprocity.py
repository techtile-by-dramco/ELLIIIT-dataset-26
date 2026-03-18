import logging
import os
import socket
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import uhd
import yaml
import tools
import argparse
import zmq
import queue
import runtime_storage

# =============================================================================
#                           Experiment Configuration
# =============================================================================
# This section defines the default settings and timing parameters
# used throughout the measurement and loopback procedure.
# Values can be overridden by entries in the configuration YAML file.
# =============================================================================

CMD_DELAY = 0.05  # Command delay (50 ms) between USRP instructions
RX_TX_SAME_CHANNEL = True  # True if loopback occurs between the same RF channel
CLOCK_TIMEOUT = 1000  # Timeout for external clock locking (in ms)
INIT_DELAY = 0.2  # Initial delay before starting transmission (200 ms)
RATE = 250e3  # Sampling rate in samples per second (250 kSps)
LOOPBACK_TX_GAIN = (
    50  # 70     # Empirically determined transmit gain for loopback tests
)
RX_GAIN = 22  # Empirically determined receive gain (22 dB without splitter, 27 dB with splitter)
CAPTURE_TIME = 10  # Duration of each capture in seconds
FREQ = 0  # Base frequency offset (Hz); 0 means use default center frequency
# SERVER_IP = "10.128.52.53"  # Optional remote server address (commented out)
meas_id = 0  # Measurement identifier
exp_id = 0  # Experiment identifier
# =============================================================================
# =============================================================================

results = []

SWITCH_LOOPBACK_MODE = 0x00000006  # which is 110
SWITCH_RESET_MODE = 0x00000000
ENABLE_USER_SETTINGS_SWITCH = True

context = zmq.Context()

iq_socket = context.socket(zmq.PUB)

iq_socket.bind(f"tcp://*:{50001}")

HOSTNAME_RAW = socket.gethostname()
HOSTNAME = HOSTNAME_RAW[4:] if len(HOSTNAME_RAW) > 4 else HOSTNAME_RAW
file_open = False
# SERVER_IP = None  # populated by settings.yml
_user_settings_iface = None
_user_settings_iface_attempted = False
RUNTIME_OUTPUT_DIR = None
data_file = None
data_file_path = None
log_file_handler = None


# =============================================================================
#                           Custom Log Formatter
# =============================================================================
# This formatter adds timestamps with fractional seconds to log messages,
# allowing for more precise event timing (useful in measurement systems).
# =============================================================================


class LogFormatter(logging.Formatter):
    """Custom log formatter that prints timestamps with fractional seconds."""

    @staticmethod
    def pp_now():
        """Return the current time of day as a formatted string with milliseconds."""
        now = datetime.now()
        return "{:%H:%M}:{:05.2f}".format(now, now.second + now.microsecond / 1e6)

    def formatTime(self, record, datefmt=None):
        """Override the default time formatter to include fractional seconds."""
        converter = self.converter(record.created)
        if datefmt:
            formatted_date = converter.strftime(datefmt)
        else:
            formatted_date = LogFormatter.pp_now()
        return formatted_date


class ColoredFormatter(LogFormatter):
    """Console formatter with ANSI colors per level."""

    COLORS = {
        logging.DEBUG: "\033[36m",     # cyan
        logging.INFO: "\033[32m",      # green
        logging.WARNING: "\033[33m",   # yellow
        logging.ERROR: "\033[31m",     # red
        logging.CRITICAL: "\033[35m",  # magenta
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        reset = self.RESET if color else ""
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


def fmt(val):
    """Format float to 0.3f."""
    try:
        return f"{float(val):.3f}"
    except Exception:
        return str(val)


DEG = "\u00b0"


# =============================================================================
#                           Logger and Channel Configuration
# =============================================================================
# This section initializes the global logger and defines the
# channel mapping used for reference and loopback measurements.
# =============================================================================

global logger
global begin_time

connected_to_server = False
begin_time = 2.0

# -------------------------------------------------------------------------
# Logger setup
# -------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Stream logs to console
console = logging.StreamHandler()
logger.addHandler(console)

# Custom log format (includes time, level, and thread name)
formatter = LogFormatter(
    fmt="[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s"
)
# Colored for console
console.setFormatter(ColoredFormatter(fmt=formatter._fmt))


# -------------------------------------------------------------------------
# Topic identifiers for ZMQ or internal messaging
# -------------------------------------------------------------------------
TOPIC_CH0 = b"CH0"
TOPIC_CH1 = b"CH1"

# -------------------------------------------------------------------------
# Channel configuration depending on whether RX and TX share the same channel
# -------------------------------------------------------------------------
if RX_TX_SAME_CHANNEL:
    # Reference signal received on CH0, loopback on CH1
    REF_RX_CH = FREE_TX_CH = 0
    LOOPBACK_RX_CH = LOOPBACK_TX_CH = 1
    logger.debug("\nPLL REF → CH0 RX\nCH1 TX → CH1 RX\nCH0 TX →")
else:
    # Reference and loopback channels are swapped
    LOOPBACK_RX_CH = FREE_TX_CH = 0
    REF_RX_CH = LOOPBACK_TX_CH = 1
    logger.debug("\nPLL REF → CH1 RX\nCH1 TX → CH0 RX\nCH0 TX →")
# =============================================================================
# =============================================================================


def configure_file_logging(output_dir):
    global RUNTIME_OUTPUT_DIR, log_file_handler

    RUNTIME_OUTPUT_DIR = Path(output_dir)
    RUNTIME_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if log_file_handler is not None:
        logger.removeHandler(log_file_handler)
        log_file_handler.close()

    log_file_handler = logging.FileHandler(
        RUNTIME_OUTPUT_DIR / f"{Path(__file__).stem}.log",
        mode="w",
    )
    log_file_handler.setFormatter(formatter)
    logger.addHandler(log_file_handler)


def build_output_path(file_name):
    if RUNTIME_OUTPUT_DIR is None:
        raise RuntimeError("Runtime output directory is not configured.")
    return RUNTIME_OUTPUT_DIR / file_name


def rx_ref(
    usrp,
    rx_streamer,
    quit_event,
    duration,
    result_queue,
    capture_type,
    start_time=None,
):
    # https://files.ettus.com/manual/page_sync.html#sync_phase_cordics
    # The CORDICs are reset at each start-of-burst command, so users should ensure that every start-of-burst also has a time spec set.
    logger.debug(f"GAIN IS CH0: {usrp.get_rx_gain(0)} CH1: {usrp.get_rx_gain(1)}")

    global results
    num_channels = rx_streamer.get_num_channels()
    max_samps_per_packet = rx_streamer.get_max_num_samps()
    buffer_length = int(duration * RATE * 2)
    iq_data = np.empty((num_channels, buffer_length), dtype=np.complex64)

    recv_buffer = np.zeros((num_channels, max_samps_per_packet), dtype=np.complex64)
    rx_md = uhd.types.RXMetadata()
    # Craft and send the Stream Command
    stream_cmd = uhd.types.StreamCMD(uhd.types.StreamMode.start_cont)
    # The stream now parameter controls when the stream begins. When true, the device will begin streaming ASAP. When false, the device will begin streaming at a time specified by time_spec.
    stream_cmd.stream_now = False
    timeout = 1.0
    if start_time is not None:
        stream_cmd.time_spec = start_time
        time_diff = start_time.get_real_secs() - usrp.get_time_now().get_real_secs()
        if time_diff > 0:
            timeout = 1.0 + time_diff
    else:
        stream_cmd.time_spec = uhd.types.TimeSpec(
            usrp.get_time_now().get_real_secs() + INIT_DELAY + 0.1
        )
    rx_streamer.issue_stream_cmd(stream_cmd)
    num_rx = 0
    result_pushed = False
    try:
        while not quit_event.is_set():
            try:
                num_rx_i = rx_streamer.recv(recv_buffer, rx_md, timeout)
                if rx_md.error_code != uhd.types.RXMetadataErrorCode.none:
                    logger.error(rx_md.error_code)
                else:
                    if num_rx_i > 0:
                        # samples = recv_buffer[:,:num_rx_i]
                        # send_rx(samples)
                        samples = recv_buffer[:, :num_rx_i]
                        if num_rx + num_rx_i > buffer_length:
                            logger.error(
                                "more samples received than buffer long, not storing the data"
                            )
                        else:
                            iq_data[:, num_rx : num_rx + num_rx_i] = samples
                            # threading.Thread(target=send_rx,
                            #                  args=(samples,)).start()
                            num_rx += num_rx_i
            except RuntimeError as ex:
                error_msg = f"Runtime error in receive ({capture_type}): {ex}"
                logger.error(error_msg)
                result_queue.put(
                    {
                        "ok": False,
                        "capture_type": capture_type,
                        "iq_capture": iq_data[:, :num_rx].copy(),
                        "phase": None,
                        "amplitude": None,
                        "num_rx": num_rx,
                        "error": error_msg,
                    }
                )
                result_pushed = True
                return
    except KeyboardInterrupt:
        pass
    finally:
        logger.debug("CTRL+C is pressed or duration is reached, closing off ")
        rx_streamer.issue_stream_cmd(
            uhd.types.StreamCMD(uhd.types.StreamMode.stop_cont)
        )
        if result_pushed:
            return
        iq_capture = iq_data[:, :num_rx].copy()

        if num_rx <= 0:
            error_msg = f"No IQ samples captured during {capture_type}."
            logger.error(error_msg)
            result_queue.put(
                {
                    "ok": False,
                    "capture_type": capture_type,
                    "iq_capture": iq_capture,
                    "phase": None,
                    "amplitude": None,
                    "num_rx": num_rx,
                    "error": error_msg,
                }
            )
            return

        # Keep analysis behavior, but fall back to full capture when too short.
        analysis_start = int(RATE * 1)
        if num_rx > analysis_start:
            iq_samples = iq_data[:, analysis_start:num_rx]
        else:
            logger.warning(
                "Capture %s shorter than analysis window; using full capture for phase/amplitude summary.",
                capture_type,
            )
            iq_samples = iq_capture

        try:
            phase_ch0, freq_slope_ch0_before, freq_slope_ch0_after = (
                tools.get_phases_and_apply_bandpass(iq_samples[0, :])
            )
            phase_ch1, freq_slope_ch1_before, freq_slope_ch1_after = (
                tools.get_phases_and_apply_bandpass(iq_samples[1, :])
            )
        except Exception as ex:
            error_msg = f"IQ analysis failed for {capture_type}: {ex}"
            logger.error(error_msg)
            result_queue.put(
                {
                    "ok": False,
                    "capture_type": capture_type,
                    "iq_capture": iq_capture,
                    "phase": None,
                    "amplitude": None,
                    "num_rx": num_rx,
                    "error": error_msg,
                }
            )
            return

        logger.debug(
            "Frequency offset CH0:     %.2f Hz     %.2f Hz", float(freq_slope_ch0_before), float(freq_slope_ch0_after)
        )
        logger.debug(
            "Frequency offset CH1:     %.2f Hz     %.2f Hz", float(freq_slope_ch1_before), float(freq_slope_ch1_after)
        )

        # logger.debug(
        #     "Phase CH0: mean %s%s min %s%s max %s%s",
        #     fmt(np.rad2deg(tools.circmean(phase_ch0, deg=False))), DEG,
        #     fmt(np.rad2deg(phase_ch0).min()), DEG,
        #     fmt(np.rad2deg(phase_ch0).max()), DEG,
        # )
        # logger.debug(
        #     "Phase CH1: mean %s%s min %s%s max %s%s",
        #     fmt(np.rad2deg(tools.circmean(phase_ch1, deg=False))), DEG,
        #     fmt(np.rad2deg(phase_ch1).min()), DEG,
        #     fmt(np.rad2deg(phase_ch1).max()), DEG,
        # )

        phase_diff = tools.to_min_pi_plus_pi(phase_ch0 - phase_ch1, deg=False)

        logger.debug(
            "Phase CH1: mean %s%s min %s%s max %s%s",
            fmt(np.rad2deg(tools.circmean(phase_diff, deg=False))),
            DEG,
            fmt(np.rad2deg(phase_diff).min()),
            DEG,
            fmt(np.rad2deg(phase_diff).max()),
            DEG,
        )

        # phase_diff = phase_ch0 - phase_ch1

        _circ_mean = tools.circmean(phase_diff, deg=False)
        # _mean = np.mean(phase_diff)

        # logger.debug("Diff cirmean and mean: %s", fmt(_circ_mean - _mean))

        # result_queue.put(_mean)

        avg_ampl = np.mean(np.abs(iq_samples), axis=1)
        # var_ampl = np.var(np.abs(iq_samples), axis=1)

        A_rms = np.sqrt(np.mean(np.abs(iq_samples) ** 2, axis=1))

        result_queue.put(
            {
                "ok": True,
                "capture_type": capture_type,
                "iq_capture": iq_capture,
                "phase": float(_circ_mean),
                "amplitude": float(A_rms[1]),
                "num_rx": num_rx,
                "error": "",
            }
        )

        max_I = np.max(np.abs(np.real(iq_samples)), axis=1)
        max_Q = np.max(np.abs(np.imag(iq_samples)), axis=1)

        logger.debug(
            "MAX AMPL IQ CH0: I %s Q %s CH1: I %s Q %s",
            fmt(max_I[0]),
            fmt(max_Q[0]),
            fmt(max_I[1]),
            fmt(max_Q[1]),
        )

        logger.debug(
            "AVG AMPL IQ CH0: %s CH1: %s",
            fmt(avg_ampl[0]),
            fmt(avg_ampl[1]),
        )


def setup_clock(usrp, clock_src, num_mboards):
    usrp.set_clock_source(clock_src)
    logger.debug("Now confirming lock on clock signals...")
    end_time = datetime.now() + timedelta(milliseconds=CLOCK_TIMEOUT)
    # Lock onto clock signals for all mboards
    for i in range(num_mboards):
        is_locked = usrp.get_mboard_sensor("ref_locked", i)
        while (not is_locked) and (datetime.now() < end_time):
            time.sleep(1e-3)
            is_locked = usrp.get_mboard_sensor("ref_locked", i)
        if not is_locked:
            logger.error("Unable to confirm clock signal locked on board %d", i)
            return False
        else:
            logger.debug("Clock signals are locked")
    return True


def get_user_settings_iface_once(usrp, chan=1):
    """Get and cache the UHD user settings interface once per process.

    Repeated calls to get_user_settings_iface() can be unstable on some setups.
    """
    global _user_settings_iface, _user_settings_iface_attempted
    if _user_settings_iface_attempted:
        return _user_settings_iface

    _user_settings_iface_attempted = True
    try:
        _user_settings_iface = usrp.get_user_settings_iface(chan)
        if _user_settings_iface is None:
            logger.warning("User settings interface is unavailable (chan=%d).", chan)
    except Exception as ex:
        logger.warning("Failed to acquire user settings interface: %s", ex)
        _user_settings_iface = None

    return _user_settings_iface


def setup_pps(usrp, pps):
    """Setup the PPS source"""

    logger.debug("Setting PPS")
    usrp.set_time_source(pps)
    return True


def print_tune_result(tune_res):
    logger.debug(
        "Tune Result:\n    Target RF  Freq: %s MHz\n    Actual RF  Freq: %s MHz\n    Target DSP Freq: %s MHz\n    Actual DSP Freq: %s MHz",
        fmt(tune_res.target_rf_freq / 1e6),
        fmt(tune_res.actual_rf_freq / 1e6),
        fmt(tune_res.target_dsp_freq / 1e6),
        fmt(tune_res.actual_dsp_freq / 1e6),
    )


def tune_usrp(usrp, freq, channels, at_time):
    """Synchronously set the device's frequency.
    If a channel is using an internal LO it will be tuned first
    and every other channel will be manually tuned based on the response.
    This is to account for the internal LO channel having an offset in the actual DSP frequency.
    Then all channels are synchronously tuned."""
    treq = uhd.types.TuneRequest(freq)
    usrp.set_command_time(uhd.types.TimeSpec(at_time))
    treq.dsp_freq = 0.0
    treq.target_freq = freq
    treq.rf_freq = freq
    treq.rf_freq_policy = uhd.types.TuneRequestPolicy(ord("M"))
    treq.dsp_freq_policy = uhd.types.TuneRequestPolicy(ord("M"))
    args = uhd.types.DeviceAddr("mode_n=integer")
    treq.args = args
    rx_freq = freq - 1e3
    rreq = uhd.types.TuneRequest(rx_freq)
    rreq.rf_freq = rx_freq
    rreq.target_freq = rx_freq
    rreq.dsp_freq = 0.0
    rreq.rf_freq_policy = uhd.types.TuneRequestPolicy(ord("M"))
    rreq.dsp_freq_policy = uhd.types.TuneRequestPolicy(ord("M"))
    rreq.args = uhd.types.DeviceAddr("mode_n=fractional")
    for chan in channels:
        logger.debug("RX tuning...")
        print_tune_result(usrp.set_rx_freq(rreq, chan))
        logger.debug("TX tuning...")
        print_tune_result(usrp.set_tx_freq(treq, chan))
    while not usrp.get_rx_sensor("lo_locked").to_bool():
        print(".")
        time.sleep(0.01)
    logger.info("RX LO is locked")
    while not usrp.get_tx_sensor("lo_locked").to_bool():
        print(".")
        time.sleep(0.01)
    logger.info("TX LO is locked")


def wait_till_go_from_server(ip, _connect=True):

    global meas_id, file_open, data_file, data_file_path, file_name
    # Connect to the publisher's address
    logger.debug("Connecting to server %s.", ip)
    sync_socket = context.socket(zmq.SUB)

    alive_socket = context.socket(zmq.REQ)

    sync_port = str(globals().get("SYNC_PORT", "5557"))
    alive_port = str(globals().get("ALIVE_PORT", "5558"))

    sync_socket.connect(f"tcp://{ip}:{sync_port}")
    alive_socket.connect(f"tcp://{ip}:{alive_port}")
    # Subscribe to topics
    sync_socket.subscribe("")

    logger.debug("Sending ALIVE")
    alive_socket.send_string(HOSTNAME)
    # Receives a string format message
    logger.debug("Waiting on SYNC from server %s.", ip)

    meas_id, unique_id = sync_socket.recv_string().split(" ")

    file_name = f"data_{HOSTNAME}_{unique_id}_{meas_id}"

    next_data_file_path = build_output_path(f"data_{HOSTNAME}_{unique_id}.txt")
    if data_file is None or data_file_path != next_data_file_path:
        if data_file is not None and not data_file.closed:
            data_file.close()
        data_file = open(next_data_file_path, "a", encoding="utf-8")
        data_file_path = next_data_file_path
        file_open = True

    logger.debug(meas_id)

    alive_socket.close()
    sync_socket.close()


def send_usrp_done_mode(ip):
    done_port = str(globals().get("DONE_PORT", globals().get("DATA_PORT", "5559")))
    done_mode_socket = context.socket(zmq.REQ)
    done_mode_socket.connect(f"tcp://{ip}:{done_port}")
    logger.debug("USRP IN DONE MODE")
    done_mode_socket.send_string(HOSTNAME)
    done_mode_socket.close()


def setup(usrp, SERVER_IP, connect=True):
    rate = RATE
    mcr = 20e6
    assert (
        mcr / rate
    ).is_integer(), f"The masterclock rate {mcr} should be an integer multiple of the sampling rate {rate}"
    # Manual selection of master clock rate may also be required to synchronize multiple B200 units in time.
    usrp.set_master_clock_rate(mcr)
    channels = [0, 1]
    setup_clock(usrp, "external", usrp.get_num_mboards())
    setup_pps(usrp, "external")
    # smallest as possible (https://files.ettus.com/manual/page_usrp_b200.html#b200_fe_bw)
    rx_bw = 200e3
    for chan in channels:
        usrp.set_rx_rate(rate, chan)
        usrp.set_tx_rate(rate, chan)
        # NOTE DC offset is enabled
        usrp.set_rx_dc_offset(True, chan)
        usrp.set_rx_bandwidth(rx_bw, chan)
        usrp.set_rx_agc(False, chan)
    # specific settings from loopback/REF PLL
    usrp.set_tx_gain(LOOPBACK_TX_GAIN, LOOPBACK_TX_CH)
    usrp.set_tx_gain(LOOPBACK_TX_GAIN, FREE_TX_CH)

    usrp.set_rx_gain(LOOPBACK_RX_GAIN, LOOPBACK_RX_CH)
    usrp.set_rx_gain(REF_RX_GAIN, REF_RX_CH)
    # streaming arguments
    st_args = uhd.usrp.StreamArgs("fc32", "sc16")
    st_args.channels = channels
    # streamers
    tx_streamer = usrp.get_tx_stream(st_args)
    rx_streamer = usrp.get_rx_stream(st_args)
    # Step1: wait for the last pps time to transition to catch the edge
    # Step2: set the time at the next pps (synchronous for all boards)
    # this is better than set_time_next_pps as we wait till the next PPS to transition and after that we set the time.
    # this ensures that the FPGA has enough time to clock in the new timespec (otherwise it could be too close to a PPS edge)
    
    return tx_streamer, rx_streamer


def rx_thread(
    usrp,
    rx_streamer,
    quit_event,
    duration,
    res,
    capture_type,
    start_time=None,
):
    _rx_thread = threading.Thread(
        target=rx_ref,
        args=(
            usrp,
            rx_streamer,
            quit_event,
            duration,
            res,
            capture_type,
            start_time,
        ),
    )
    _rx_thread.name = "RX_thread"
    _rx_thread.start()
    return _rx_thread


def tx_async_th(tx_streamer, quit_event):
    async_metadata = uhd.types.TXAsyncMetadata()
    try:
        while not quit_event.is_set():
            if not tx_streamer.recv_async_msg(async_metadata, 0.01):
                continue
            else:
                if async_metadata.event_code != uhd.types.TXMetadataEventCode.burst_ack:
                    logger.error(async_metadata.event_code)
    except KeyboardInterrupt:
        pass


def delta(usrp, at_time):
    return at_time - usrp.get_time_now().get_real_secs()


def get_current_time(usrp):
    return usrp.get_time_now().get_real_secs()


def tx_thread(
    usrp, tx_streamer, quit_event, phase=[0, 0], amplitude=[0.8, 0.8], start_time=None
):
    tx_thr = threading.Thread(
        target=tx_ref,
        args=(usrp, tx_streamer, quit_event, phase, amplitude, start_time),
    )

    tx_thr.name = "TX_thread"
    tx_thr.start()

    return tx_thr


def tx_ref(usrp, tx_streamer, quit_event, phase, amplitude, start_time=None):
    """
    Transmit a continuous reference signal on all active channels.

    This function generates a complex baseband signal based on the provided
    amplitude and phase values, then continuously transmits it using the
    specified USRP transmit streamer until `quit_event` is set.

    Args:
        usrp: The USRP device instance.
        tx_streamer: UHD transmit streamer used for sending samples.
        quit_event: Threading event used to stop transmission when set.
        phase (list or np.ndarray): Phase offset for each channel (in radians).
        amplitude (list or np.ndarray): Amplitude for each channel.
        start_time (uhd.types.TimeSpec, optional): Scheduled start time.
            If None, transmission begins after `INIT_DELAY` seconds.

    Notes:
        - This function continuously sends the same buffer of complex samples.
        - It is typically used to generate a reference signal for phase calibration.
    """

    # Retrieve USRP transmission parameters
    num_channels = tx_streamer.get_num_channels()
    max_samps_per_packet = tx_streamer.get_max_num_samps()

    # Convert inputs to NumPy arrays for element-wise operations
    amplitude = np.asarray(amplitude)
    phase = np.asarray(phase)

    # Compute the complex signal for each channel: A * e^(j * phi)
    sample = amplitude * np.exp(1j * phase)

    # Initialize a large transmit buffer filled with the reference signal
    transmit_buffer = np.ones(
        (num_channels, 1000 * max_samps_per_packet), dtype=np.complex64
    )
    transmit_buffer[0, :] *= sample[0]
    transmit_buffer[1, :] *= sample[1]

    # Create UHD transmit metadata (for timed transmission)
    tx_md = uhd.types.TXMetadata()

    # Schedule the transmission start time
    if start_time is not None:
        tx_md.time_spec = start_time
    else:
        tx_md.time_spec = uhd.types.TimeSpec(
            usrp.get_time_now().get_real_secs() + INIT_DELAY
        )

    tx_md.has_time_spec = True

    try:
        # Continuously transmit the reference signal until quit_event is triggered
        while not quit_event.is_set():
            tx_streamer.send(transmit_buffer, tx_md)

    except KeyboardInterrupt:
        logger.debug("CTRL+C detected — stopping transmission")

    finally:
        # Send an end-of-burst (EOB) packet to properly terminate streaming
        tx_md.end_of_burst = True
        tx_streamer.send(np.zeros((num_channels, 0), dtype=np.complex64), tx_md)


def tx_meta_thread(tx_streamer, quit_event):
    tx_meta_thr = threading.Thread(target=tx_async_th, args=(tx_streamer, quit_event))

    tx_meta_thr.name = "TX_META_thread"
    tx_meta_thr.start()
    return tx_meta_thr


def starting_in(usrp, at_time):
    return f"Starting in {delta(usrp, at_time):.2f}s"


def measure_pilot(
    usrp, tx_streamer, rx_streamer, quit_event, result_queue, at_time=None
):
    """
    Perform a pilot measurement using the specified USRP device and RX streamer.

    This function:
    - Configures the RX antenna for pilot capture
    - Starts a receive thread to record samples
    - Waits until the capture is complete
    - Restores the default antenna configuration

    Parameters
    ----------
    usrp : uhd.usrp.MultiUSRP
        The USRP device instance.
    rx_streamer : uhd.usrp.RXStreamer
        The receive streamer for data capture.
    quit_event : threading.Event
        Event object to control and stop the RX thread.
    result_queue : queue.Queue
        Queue used to store measurement results (e.g., captured phase).
    at_time : float, optional
        Scheduled start time for the measurement (in seconds).
    """
    logger.debug("########### Measure PILOT ###########")

    # ------------------------------------------------------------
    # 1. Configure RX antenna
    # ------------------------------------------------------------
    usrp.set_rx_antenna("TX/RX", 1)

    # ------------------------------------------------------------
    # 2. Set the transmission start time
    # ------------------------------------------------------------
    start_time = uhd.types.TimeSpec(at_time)
    logger.debug(starting_in(usrp, at_time))

    # ------------------------------------------------------------
    # 4. Start receive (RX) threads
    # ------------------------------------------------------------

    rx_thr = rx_thread(
        usrp,
        rx_streamer,
        quit_event,
        duration=CAPTURE_TIME,
        res=result_queue,
        capture_type="pilot",
        start_time=start_time,
    )

    # ------------------------------------------------------------
    # 5. Wait for the capture duration plus some safety margin (delta)
    # ------------------------------------------------------------
    time.sleep(CAPTURE_TIME + delta(usrp, at_time))

    # ------------------------------------------------------------
    # 6. Signal all threads to stop and wait for them to finish
    # ------------------------------------------------------------
    quit_event.set()  # Triggers thread termination
    rx_thr.join()

    # ------------------------------------------------------------
    # 7. Reset antenna
    # ------------------------------------------------------------
    usrp.set_rx_antenna("RX2", 1)

    # ------------------------------------------------------------
    # 8. Clear the quit event flag to prepare for the next measurement
    # ------------------------------------------------------------
    quit_event.clear()

    try:
        return result_queue.get(timeout=1.0)
    except queue.Empty:
        return {
            "ok": False,
            "capture_type": "pilot",
            "iq_capture": np.empty((0, 0), dtype=np.complex64),
            "phase": None,
            "amplitude": None,
            "num_rx": 0,
            "error": "Pilot result missing from queue.",
        }


def measure_loopback(
    usrp, tx_streamer, rx_streamer, quit_event, result_queue, at_time=None
):
    # ------------------------------------------------------------
    # Function: measure_loopback
    # Purpose:
    #   This function performs a loopback measurement using a USRP device.
    #   It transmits a known signal on one channel and simultaneously
    #   receives it on another channel (loopback). The result is captured,
    #   stored, and processed later.
    # ------------------------------------------------------------

    logger.debug("########### Measure LOOPBACK ###########")

    # ------------------------------------------------------------
    # 1. Configure transmit signal amplitudes
    # ------------------------------------------------------------
    amplitudes = [0.0, 0.0]  # Initialize amplitude array for both channels
    amplitudes[LOOPBACK_TX_CH] = 0.8  # Enable TX on the selected loopback channel

    # ------------------------------------------------------------
    # 2. Set the transmission start time
    # ------------------------------------------------------------
    start_time = uhd.types.TimeSpec(at_time)
    logger.debug(starting_in(usrp, at_time))

    # ------------------------------------------------------------
    # 3. (Legacy) Access user settings interface for low-level FPGA control
    #    Used to switch the USRP into "loopback mode" by writing to
    #    a register in the user settings interface.
    #    NOTE: This interface is no longer available in UHD 4.x.
    # ------------------------------------------------------------
    user_settings = None
    if ENABLE_USER_SETTINGS_SWITCH:
        user_settings = get_user_settings_iface_once(usrp, chan=1)
        if user_settings is not None:
            try:
                user_settings.poke32(0, SWITCH_LOOPBACK_MODE)
            except Exception as e:
                logger.error("Unable to switch loopback mode: %s", e)
                user_settings = None
        else:
            logger.warning(
                "Loopback switch control disabled: no user settings interface."
            )

    # ------------------------------------------------------------
    # 4. Start transmit (TX), metadata, and receive (RX) threads
    # ------------------------------------------------------------
    tx_thr = tx_thread(
        usrp,
        tx_streamer,
        quit_event,
        amplitude=amplitudes,
        phase=[0.0, 0.0],
        start_time=start_time,
    )

    # Thread responsible for handling TX metadata (timestamps, etc.)
    tx_meta_thr = tx_meta_thread(tx_streamer, quit_event)

    # Thread that captures received samples during loopback
    rx_thr = rx_thread(
        usrp,
        rx_streamer,
        quit_event,
        duration=CAPTURE_TIME,
        res=result_queue,
        capture_type="loopback",
        start_time=start_time,
    )

    # ------------------------------------------------------------
    # 5. Wait for the capture duration plus some safety margin (delta)
    # ------------------------------------------------------------
    time.sleep(CAPTURE_TIME + delta(usrp, at_time))

    # ------------------------------------------------------------
    # 6. Signal all threads to stop and wait for them to finish
    # ------------------------------------------------------------
    quit_event.set()  # Triggers thread termination
    tx_thr.join()
    rx_thr.join()
    tx_meta_thr.join()

    # ------------------------------------------------------------
    # 7. Reset the RF switch control (disable loopback mode)
    # ------------------------------------------------------------
    if user_settings is not None:
        try:
            user_settings.poke32(0, SWITCH_RESET_MODE)
        except Exception as e:
            logger.error("Unable to reset loopback switch mode: %s", e)

    # ------------------------------------------------------------
    # 8. Clear the quit event flag to prepare for the next measurement
    # ------------------------------------------------------------
    quit_event.clear()

    try:
        return result_queue.get(timeout=1.0)
    except queue.Empty:
        return {
            "ok": False,
            "capture_type": "loopback",
            "iq_capture": np.empty((0, 0), dtype=np.complex64),
            "phase": None,
            "amplitude": None,
            "num_rx": 0,
            "error": "Loopback result missing from queue.",
        }

def get_BF(ampl_P1, phi_P1, ampl_P2, phi_P2):
    import json

    dealer_socket = context.socket(zmq.DEALER)

    # Give this DEALER a unique identity so the ROUTER can reply properly

    dealer_socket.setsockopt_string(zmq.IDENTITY, HOSTNAME)

    dealer_socket.connect(f"tcp://{SERVER_IP}:{PILOT_PORT}")

    logger.debug("Sending CSI")

    # Create a message dict with CSI (complex split into real and imag)
    msg = {
        "host": HOSTNAME,
        "ampl_P1": float(ampl_P1),
        "phi_P1": float(phi_P1),
        "ampl_P2": float(ampl_P2),
        "phi_P2": float(phi_P2),
    }

    # Serialize to JSON and send
    dealer_socket.send(json.dumps(msg).encode())
    logger.debug("Message sent, waiting for response...")

    # Wait for response
    poller = zmq.Poller()
    poller.register(dealer_socket, zmq.POLLIN)
    socks = dict(poller.poll(30000))

    result = None

    if dealer_socket in socks and socks[dealer_socket] == zmq.POLLIN:
        reply = dealer_socket.recv()
        logger.debug("Raw reply: %r", reply)
        response = json.loads(reply.decode())
        logger.info("[%s] Received: %s", HOSTNAME, response)
        # Reconstruct complex number
        result = response["phi_BF"]
        logger.debug("Received response: %s", result)
    else:
        logger.warning("[%s] No reply from server, timed out.", HOSTNAME)

    dealer_socket.close()

    return result

def tx_phase_coh(usrp, tx_streamer, quit_event, phase_corr, at_time, long_time=True):
    """
    Transmit a coherent signal with an adjusted phase correction.

    This function starts a transmission thread that sends a signal with
    a specific phase correction on the loopback transmit channel.
    It also launches a metadata thread to handle UHD transmission metadata.
    The function blocks until the transmission time has elapsed, then stops
    both threads cleanly.

    Args:
        usrp: The USRP device instance.
        tx_streamer: UHD transmit streamer.
        quit_event: Threading event to signal thread termination.
        phase_corr (float): Phase correction value (in radians).
        at_time (float): Scheduled start time for transmission.
        long_time (bool): If True, use TX_TIME; otherwise, transmit for 10 seconds.
    """
    logger.debug("########### TX with adjusted phases ###########")

    # Initialize arrays for phase and amplitude per TX channel
    phases = [0.0, 0.0]
    amplitudes = [0.0, 0.0]

    # Apply phase correction and amplitude to the loopback transmit channel
    phases[LOOPBACK_TX_CH] = phase_corr
    amplitudes[LOOPBACK_TX_CH] = 0.8

    logger.debug(f"Phases: {phases}")
    logger.debug(f"amplitudes: {amplitudes}")
    logger.debug(f"TX Gain: {FREE_TX_GAIN}")

    # Set the transmit gain for the active channel
    usrp.set_tx_gain(FREE_TX_GAIN, LOOPBACK_TX_CH)

    # Define the UHD transmission start time
    start_time = uhd.types.TimeSpec(at_time)

    # Start the transmit thread
    tx_thr = tx_thread(
        usrp,
        tx_streamer,
        quit_event,
        amplitude=amplitudes,
        phase=phases,
        start_time=start_time,
    )

    # Start the metadata monitoring thread
    tx_meta_thr = tx_meta_thread(tx_streamer, quit_event)

   

    # Allow transmission to continue for the configured duration
    if long_time:
        time.sleep(TX_TIME + delta(usrp, at_time))
    else:
        time.sleep(10.0 + delta(usrp, at_time))

    # Signal all threads to stop
    quit_event.set()

    # Ensure both threads terminate cleanly
    tx_thr.join()
    tx_meta_thr.join()

    logger.debug("Transmission completed successfully")

    quit_event.clear()

    return tx_thr, tx_meta_thr


def parse_arguments():
    """
    Parse command-line arguments for the beamforming (BF) application.

    This function checks for the optional server IP argument (-i or --ip)
    and updates the global variable `SERVER_IP` if provided. It also
    allows overriding the TX phase file.

    Example:
        python script.py -i 192.168.1.10
    """
    global SERVER_IP

    # Create an argument parser with a brief description
    parser = argparse.ArgumentParser(description="Beamforming control script")

    # Optional argument for specifying the server IP
    parser.add_argument(
        "-i",
        "--ip",
        type=str,
        help="IP address of the server (optional)",
        required=False,
    )

    parser.add_argument("--config-file", type=str)
    parser.add_argument(
        "--tx-phase-file",
        type=str,
        default="tx-phases-smc2-old.yml",
        help="Path to TX phase YAML (default: tx-phases-smc2-old.yml)",
    )

    # Parse the command-line arguments
    args = parser.parse_args()

    # If the user provided an IP address, apply it
    if args.ip:
        logger.debug(f"Setting server IP to: {args.ip}")
        SERVER_IP = args.ip
    return args


def main():
    global meas_id, file_name_state, data_file

    args = parse_arguments()
    quit_event = None

    try:
        runtime_config = runtime_storage.resolve_runtime_output_dir(
            args.config_file,
            HOSTNAME,
        )
        configure_file_logging(runtime_config["host_output_dir"])
        logger.info("Invocation args: %s", " ".join(sys.argv))
        logger.info(
            "Loaded experiment settings from %s",
            runtime_config["settings_path"],
        )
        logger.info("Runtime output directory: %s", RUNTIME_OUTPUT_DIR)
    except Exception as e:
        logger.error("Unable to initialize runtime storage: %s", e)
        return

    try:
        # Attempt to open and load calibration settings from the YAML file
        with open(
            os.path.join(os.path.dirname(__file__), "cal-settings.yml"), "r"
        ) as file:
            vars = yaml.safe_load(file)
            globals().update(vars)  # update the global variables with the vars in yaml
    except FileNotFoundError:
        logger.error(
            "Calibration file 'cal-settings.yml' not found in the current directory."
        )
        exit()
    except yaml.YAMLError as e:
        logger.error(f"Error parsing 'cal-settings.yml': {e}")
        exit()
    except Exception as e:
        logger.error(f"Unexpected error while loading calibration settings: {e}")
        exit()

    try:
        # Get current path
        script_dir = os.path.dirname(os.path.realpath(__file__))

        # FPGA file path
        fpga_path = os.path.join(script_dir, "usrp_b210_fpga_loopback.bin")

        # Initialize USRP device with custom FPGA image and integer mode
        usrp = uhd.usrp.MultiUSRP(
            "enable_user_regs, " f"fpga={fpga_path}, " "mode_n=integer"
        )
        logger.info("Using Device: %s", usrp.get_pp_string())

        # -------------------------------------------------------------------------
        # STEP 0: Preparations
        # -------------------------------------------------------------------------

        # Set up TX and RX streamers and establish connection
        tx_streamer, rx_streamer = setup(usrp, SERVER_IP, connect=True)


        while 1:
            wait_till_go_from_server(SERVER_IP)
            logger.info("Setting device timestamp to 0...")
            usrp.set_time_unknown_pps(uhd.types.TimeSpec(0.0))
            logger.debug("[SYNC] Resetting time.")
            logger.info(f"RX GAIN PROFILE CH0: {usrp.get_rx_gain_names(0)}")
            logger.info(f"RX GAIN PROFILE CH1: {usrp.get_rx_gain_names(1)}")
            # we wait 2 seconds to ensure a PPS rising edge occurs and latches the 0.000s value to both USRPs.
            time.sleep(2)
            tune_usrp(usrp, FREQ, [0, 1], at_time=3.0)
            logger.info(
                f"USRP has been tuned and setup. ({usrp.get_time_now().get_real_secs()})"
            )

            # Event used to control thread termination
            quit_event = threading.Event()

            # Queue to collect measurement results and communicate between threads
            result_queue = queue.Queue()

            # -------------------------------------------------------------------------
            # STEP 1: Perform pilot measurement
            # -------------------------------------------------------------------------

            pilot_result = measure_pilot(
                usrp,
                tx_streamer,
                rx_streamer,
                quit_event,
                result_queue,
                at_time=START_PILOT_RX,
            )

            if not pilot_result["ok"]:
                logger.error(
                    "Pilot capture failed, skipping IQ save for this run. Reason: %s",
                    pilot_result["error"],
                )
                return

            # Print pilot phase
            logger.info(
                "Phase pilot reference signal: %s (rad) / %s%s",
                fmt(pilot_result["phase"]),
                fmt(np.rad2deg(pilot_result["phase"])),
                DEG,
            )

            # -------------------------------------------------------------------------
            # STEP 3: Perform internal loopback measurement with reference signal
            # -------------------------------------------------------------------------

            loopback_result = measure_loopback(
                usrp,
                tx_streamer,
                rx_streamer,
                quit_event,
                result_queue,
                at_time=START_LB_RX,
            )

            if not loopback_result["ok"]:
                logger.error(
                    "Loopback capture failed, skipping IQ save for this run. Reason: %s",
                    loopback_result["error"],
                )
                return

            # Print loopback phase
            logger.info(
                "Phase LB reference signal: %s (rad) / %s%s",
                fmt(loopback_result["phase"]),
                fmt(np.rad2deg(loopback_result["phase"])),
                DEG,
            )

            pilot_iq = pilot_result["iq_capture"]
            loopback_iq = loopback_result["iq_capture"]

            if pilot_iq.size == 0 or loopback_iq.size == 0:
                logger.error(
                    "One or more captures are empty (pilot size=%d, loopback size=%d); skipping IQ save for this run.",
                    pilot_iq.size,
                    loopback_iq.size,
                )
                return

            iq_file_name = build_output_path(f"{file_name}_iq.npz")
            np.savez(
                iq_file_name,
                pilot_iq=pilot_iq,
                loopback_iq=loopback_iq,
                pilot_phase=pilot_result["phase"],
                pilot_amplitude=pilot_result["amplitude"],
                loopback_phase=loopback_result["phase"],
                loopback_amplitude=loopback_result["amplitude"],
                hostname=HOSTNAME,
                meas_id=meas_id,
                file_name=file_name,
            )
            logger.info("Saved pilot and loopback IQ captures to %s", iq_file_name)

            print("DONE")
             # Send USRP is in TX mode for scope measurements
            send_usrp_done_mode(SERVER_IP)

    except Exception as e:
        # Handle any exception gracefully
        logger.debug("Sending signal to stop!")
        logger.error(e)
        if quit_event is not None:
            quit_event.set()

    finally:
        # Allow threads and streams to close properly
        if data_file is not None and not data_file.closed:
            data_file.close()
        time.sleep(1)
        sys.exit(0)


if __name__ == "__main__":
    while 1:
        main()
