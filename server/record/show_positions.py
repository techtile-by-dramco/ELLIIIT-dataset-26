# ****************************************************************************************** #
#                                       IMPORTS / PATHS                                      #
# ****************************************************************************************** #

from Positioner import PositionerClient
import os
import atexit
import signal
from time import sleep, time
import numpy as np
import zmq
import sys

# -------------------------------------------------
# Directory and file names
# -------------------------------------------------
server_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(server_dir)

# -------------------------------------------------
# lib imports
# -------------------------------------------------
PROJECT_ROOT = os.path.dirname(project_dir)
sys.path.insert(0, PROJECT_ROOT)
from lib.yaml_utils import read_yaml_file
from lib.ep import RFEP

# -------------------------------------------------
# config file
# -------------------------------------------------
settings = read_yaml_file("experiment-settings.yaml")

# ****************************************************************************************** #
#                                      INITIALIZATION                                        #
# ****************************************************************************************** #

positioner = PositionerClient(config=settings["positioning"], backend="zmq")

try:
    print("Starting positioner...")
    positioner.start()

    while True:
        pos = positioner.get_data()
        print(pos)
        sleep(1)

except KeyboardInterrupt:
    print("\nCtrl+C received. Stopping measurement...")

except Exception as e:
    print("Unexpected error:", e)
    raise

finally:
    try:
        positioner.stop()
    except Exception:
        pass

    print("Shutdown complete.")
    sys.exit(0)
