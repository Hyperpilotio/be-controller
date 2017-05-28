# test_store.py
import unittest
from store import InfluxWriter
import influxdb
import docker
import datetime
import os


class StoreTestCase(unittest.TestCase):

    def test_write(self):
        try:
            result = self.influxWriter.write(datetime.datetime.now(), "test", "huh", {"field1": "value1", "field2": "value2", "field3": "value3"})
            # test if data is the same as write
        except Exception as e:
            self.assertTrue(False, msg="writting error with %s" % e)


    def setUp(self):
        try:
            influxHost = os.getenv('INFLUXDB_HOST', default='localhost')
            influxPort = os.getenv('INFLUXDB_PORT', default=8086)
            influxUser = os.getenv('INFLUXDB_USER', default='root')
            password = os.getenv('INFLUXDB_PASSWORD', default='root')
            db = os.getenv('INFLUXDB_DB', default='be_controller')
            self.influxWriter = InfluxWriter(host=influxHost, port=influxPort, user=influxUser, password=password, db=db)
        except Exception as e:
            raise AssertionError("Couldn't connect to InfluxDB, be sure InfluxDB is running and reachable.")
