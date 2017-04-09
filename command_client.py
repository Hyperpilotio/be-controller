import socket
import uuid
import json

class CommandClient(object):
    SOCKET = "/var/run/command.sock"

    def send(self, request):
        unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        unix_socket.connect(CommandClient.SOCKET)
        unix_socket.sendall(json.dumps(request))
        response_string = unix_socket.recv(4096)
        unix_socket.close()
        return json.loads(response_string)

    def run_command(self, command):
        request = {"id": str(uuid.uuid1()), "command": command}
        response = self.send(request)
        exit_code = response["exit_code"]
        if exit_code != 0:
            return (None, "Command failed with exit code %d, stderr: %s" % (exit_code, response["stderr"]))

        return (response["stdout"], None)

    def run_commands(self, commands):
        for command in commands:
            _, err = self.run_command(command)
            if err:
                return False

        return True
