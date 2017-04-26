from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError

class InfluxWriter(object):
    def __init__(self):
        self.client = InfluxDBClient(
            "influxsrv", 8083, "root", "root", "be_controller")
        try:
            self.client.create_database("be_controller")
        except InfluxDBClientError:
            pass #Ignore

    def write(self, time, hostname, controller, data):
        stat = {
            'time': time,
            'tags': {
                'hostname': hostname,
                'controller': controller
            },
            'measurement': 'data',
            'fields': {
                'value': data
            }
        }

        try:
            self.client.write_points([stat])
        except InfluxDBClientError as e:
            print("Error writing to influx: " + str(e))
