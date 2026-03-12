from utils.server_com import ServerSideCom
from utils.usrp_control import USRP_Control
import config
import os
import sys
import yaml
import time

# ---------------------------------------------------------
# Set up paths and configuration
# ---------------------------------------------------------
# Construct the path to the experiment settings YAML file
settings_path = os.path.join(config.PROJECT_DIR, "experiment-settings.yaml")

# Output general info about project location
print("Experiment project directory: ", config.PROJECT_DIR)  # Should point to tile-management repo clone

# Check if the tile-management repository exists in the expected location
# Exit with an error code if not
if not config.check_tile_management_repo():
    sys.exit(config.ERRORS["REPO_ERROR"])

# Add utils directory to the system path to import additional helper functions
sys.path.append(config.UTILS_DIR)
from ansible_utils import get_target_hosts, run_playbook

# ---------------------------------------------------------
# Load experiment settings
# ---------------------------------------------------------
with open(settings_path, "r") as f:
    experiment_settings = yaml.safe_load(f)

# Determine which tiles are targeted by the experiment
tiles = experiment_settings.get("tiles", "")
if len(tiles) == 0:
    print("The experiment doesn't target any tiles.")
    sys.exit(config.ERRORS["NO_TILES_ERROR"])

# Retrieve the host list for the targeted tiles
host_list = get_target_hosts(config.INVENTORY_PATH, limit=tiles, suppress_warnings=True)

# ---------------------------------------------------------
# Configure server parameters
# ---------------------------------------------------------

# Instantiate server object for communication with the tiles
server = ServerSideCom(settings_path)
# Instantiate usrp object for communication with the tiles' usrps
usrp = USRP_Control(server)

server.start() # optional
usrp.start()
usrp.set_required_hosts(host_list)

# ---------------------------------------------------------
# Main loop: monitor client connections and manage transmissions
# ---------------------------------------------------------
try:
    usrp.wait_until_connected(timeout_s=30)
    try:
        usrp.send_command(usrp.Command.SYNC, tiles=["A05"], at=10, dir="RX", timeout_s=1)
    except TimeoutError as e:
        print("this timemout is expected")
    usrp.send_command(usrp.Command.SYNC, at=10, dir="RX", timeout_s=10)
    print("sync completed")
    time.sleep(10)
except KeyboardInterrupt:
    # Catch Ctrl+C in main thread for clean shutdown
    pass

server.stop()
server.join()
print("Server terminated.")