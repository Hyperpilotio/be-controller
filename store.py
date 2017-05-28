from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
import json

class InfluxWriter(object):

    def __init__(self, host=None, port=None, user=None, password=None, db=None):
        if any(x is None for x in [host, port, user, password, db]):
            self.client = None
        else:
            self.client = InfluxDBClient(
                host, port, user, password, db)
            try:
                self.client.create_database(db)
            except InfluxDBClientError:
                pass #Ignore

    def write(self, time, hostname, controller, data):
        if self.client is None:
            raise Exception("store:ERROR: influxdb client not confgure properly")
        try:
            self.client.write_points([{
                "time": time,
                "tags": {
                    "hostname": hostname,
                },
                "measurement": controller,
                "fields": data,
            }])
        except InfluxDBClientError as e:
            print("Store:ERROR: Error writing to influx: " + str(e))
