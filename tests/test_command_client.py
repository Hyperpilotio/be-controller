import unittest
from command_client import *

class CommandClientTestCase(unittest.TestCase):

    def runningInDocker(self):
        try:
            with open('/proc/self/cgroup', 'r') as procfile:
                for line in procfile:
                    fields = line.strip().split('/')
                    if fields[1] == 'docker':
                        return True

            return False
        except Exception as e:
            return False


    def setUp(self):
        if self.runningInDocker():
            self.ctloc = "in"
        else:
            self.ctloc = "out"
        
    def test_run_command(self):

        cmd = CommandClient(self.ctloc)
        if self.ctloc == "in":
            self.assertTrue(type(cmd.client) is UnixSocketClient, msg=None)
        else:
            self.assertTrue(type(cmd.client) is SubprocessClient, msg=None)

        result = cmd.run_command('echo hello world!!!')
        self.assertEqual(('hello world!!!\n', None), result, msg="Not expected result")

    def test_run_commands(self):
        commands = [
            'echo hello',
            'echo world',
            'echo command',
            'echo client'
        ]
        cmd = CommandClient(self.ctloc)
        result = cmd.run_commands(commands)
        self.assertTrue(result, msg="fail run commands")

if __name__ == '__main__':
    unittest.main()
