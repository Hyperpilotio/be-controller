import socket
import subprocess
from subprocess import Popen, PIPE
import uuid
import json

class CommandClient(object):
    def __init__(self, ctlloc):
        if ctlloc == "in":
            print "Using unix socket client for commands"
            self.client = UnixSocketClient()
        else:
            print "Using subprocess client for commands"
            self.client = SubprocessClient()

    def run_command(self, command):
        return self.client.run_command(command)

    def run_commands(self, commands):
        for command in commands:
            out, err = self.run_command(command)
            if err:
                return False
        return True

class SubprocessClient(object):
    def run_command(self, command):
        process = Popen(command, shell=True, executable="/bin/bash", stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            return (stdout, stderr)
        else:
            return (stdout, None)

class UnixSocketClient(object):
    SOCKET = "/var/run/command.sock"

    def _send(self, request):
        unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        unix_socket.connect(UnixSocketClient.SOCKET)
        unix_socket.sendall(json.dumps(request))
        response_string = unix_socket.recv(4096)
        unix_socket.close()
        return json.loads(response_string)

    def run_command(self, command):
        request = {"id": str(uuid.uuid1()), "command": command}
        response = self._send(request)
        exit_code = response["exit_code"]
        if exit_code != 0:
            return (None, "Command failed with exit code %d, stderr: %s" % (exit_code, response["stderr"]))

        return (response["stdout"], None)
