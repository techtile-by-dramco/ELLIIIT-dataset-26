from utils.server_com import ServerSideCom
from enum import Enum
import json
import time

class USRP_Control:
    class Command(Enum):
        SYNC = "usrp_sync"
        CAL = "usrp_cal"
        PILOT = "usrp_pilot"
        UNDEFINED = ""
        
    class Response(Enum):
        ACK = "usrp_ack"
        DONE = "usrp_done"

    class ReturnCode(Enum):
        OK = 1
        MISSING_HOSTS = 2
        TIMEOUT = 3
        PREVIOUS_COMMAND_RUNNING = 4
        UNKNOWN_ERROR = 255
    
    class CommandStatus(Enum):
        RUNNING = 0
        ACK = 1
        DONE = 2
        TIMEOUT = 3
        MISSING_HOSTS = 4
    
    def __init__(self, server_side_com=None, silent=True):
        if not server_side_com:
            raise ValueError(f"{__class__}: server_side_com not specified")
        
        self.server = server_side_com
        self.server.on(self.Response.ACK.value, self._handle_ack)
        self.server.on(self.Response.DONE.value, self._handle_done)
        self.required_hosts = {}
        
    def start(self):
        if not self.server.running:
            print("starting server com")
            self.server.start()
    
    def set_required_hosts(self, host_list = []):
        for h in host_list:
            self.required_hosts[h] = {"command_status": self.CommandStatus.DONE}
    
    def wait_until_connected(self, host_list = None, *, timeout_s = None):
        start = time.monotonic()
        interval = 0.1

        while True:
            all_connected, missing_list = self._check_connected(host_list)
            if all_connected:
                break

            if timeout_s:
                if time.monotonic() - start >= timeout_s:
                    raise TimeoutError(f"Not all hosts connected in time. Missing {missing_list}.")

            time.sleep(interval)
    
    def send_command(self, command: Command = Command.UNDEFINED, **args):
        """
        Send a command to one or more tiles and block until completion.

        The method first verifies that all target tiles are currently connected
        (i.e., have sent a recent heartbeat). If any required tile is offline,
        the command is not sent and an exception is raised.

        If a list of tiles is provided, the command is sent sequentially as
        individual unicasts. If no tiles are specified, the command is broadcast
        to all known tiles.

        After sending, the method waits until all addressed tiles report
        CommandStatus.DONE or until an optional timeout expires.

        Parameters
        ----------
        command : Command, optional
            The command to send. Defaults to Command.UNDEFINED.
        **args
            Optional keyword arguments:
            - tiles (Iterable[str], optional):
                Identifiers of the target tiles. If omitted or None, the command
                is broadcast to all registered tiles.
            - timeout_s (float, optional):
                Maximum number of seconds to wait for all tiles to report
                CommandStatus.DONE. If omitted, wait indefinitely.
            - TODO: add command specific arguments

        Raises
        ------
        ConnectionError
            If one or more required tiles are not connected at send time.
        TimeoutError
            If not all tiles report CommandStatus.DONE before the timeout expires.
        """
        tiles = args.get("tiles", None)
        timeout_s = args.get("timeout_s", None)
        
        # check if all tiles we want to send to are connected (recently got a heartbeat)
        (all_connected, missing) = self._check_connected(tiles)
        
        # no use in continuing
        if not all_connected:
            raise ConnectionError(f"No connection with tiles {missing}")
        else:
            # seen all necessary tiles withing the last heartbeat interval
            if tiles: # tiles specified -> sequential unicast
                for tile in tiles:
                    # update command status
                    self.required_hosts[tile]["command_status"] = self.CommandStatus.RUNNING
                    # send command
                    self.server.send(tile.encode(), command.value, json.dumps(args))
            else: # no tiles specified -> broadcast
                # update command status
                for tile in self.required_hosts:
                    self.required_hosts[tile]["command_status"] = self.CommandStatus.RUNNING
                # send commmand
                self.server.broadcast(command.value, json.dumps(args))
            
            # wait for "CommandStatus.DONE"
            try:
                self._wait_until_done(timeout_s)
            except TimeoutError as e:
                raise e
    
    
    def _wait_until_done(self, timeout_s = None):
        start = time.monotonic()
        interval = 0.1

        while True:
            all_done = all(
                entry['command_status'] is self.CommandStatus.DONE
                for entry in self.required_hosts.values()
            )
            if all_done:
                break

            # check for timeout
            if timeout_s:
                if time.monotonic() - start >= timeout_s:
                    raise TimeoutError("Not all hosts reported done in time.")

            time.sleep(interval)
                
    
    def _check_connected(self, host_list=None):
        connected_hosts = self.server.get_connected()
        missing_cids = []
        
        if host_list:
            missing_cids = [cid for cid in host_list if cid.encode() not in connected_hosts]
        else:
            missing_cids = [cid for cid in self.required_hosts if cid.encode() not in connected_hosts]
            
        return (not missing_cids, missing_cids)
            
            
    def _handle_ack(self, id, args):
        try:
            self.required_hosts[id]["command_status"] = self.CommandStatus.ACK
        except LookupError as e:
            raise LookupError(f"Got ACK from {id}, but it is not in required host list.\nAre other tiles still running?\n{e}")
        
        
    def _handle_done(self, id, args):
        try:
            self.required_hosts[id]["command_status"] = self.CommandStatus.DONE
        except LookupError as e:
            raise LookupError(f"Got DONE from {id}, but it is not in required host list.\nAre other tiles still running?\n{e}")
