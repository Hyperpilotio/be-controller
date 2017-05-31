import unittest
from blkioclass import BlkioClass
import json
import settings as st
import os
from kube_helper import KubeHelper
import time

class BlkioClassTestCase(unittest.TestCase):

    def setUp(self):
        # create demo pod
        self.kubehelper = KubeHelper()
        self.demoPod = self.kubehelper.createDemoPod(BE=True)
        self.cont_key = self.demoPod.status.container_statuses[0].container_id
        fileDir = os.path.dirname(os.path.realpath('__file__'))
        with open(os.path.join(fileDir, 'config.json'), 'r') as json_data_file:
            st.params = json.load(json_data_file)

        netst = st.params['blkio_controller']
        self.blkio = BlkioClass(netst['block_dev'], netst['max_rd_iops'], netst['max_wr_iops'])
        st.enabled = True

    def tearDown(self):
        self.kubehelper.deleteDemoPods()

    def test_addBeCont(self):
        # add be cont
        print "sleep 600s for debug"
        time.sleep(600)
        self.blkio.addBeCont(self.cont_key)
        self.assertTrue(self.cont_key in self.blkio.keys, msg='container key not add to keys')

        # double add same container id
        self.assertRaises
        with self.assertRaises(Exception):
            self.blkio.addBeCont(self.cont_key)

        # remove be cont
        self.blkio.removeBeCont(self.cont_key)
        self.assertFalse(self.cont_key in self.blkio.keys, msg='container key still remain in keys')


    def test_setIopsLimit(self):
        riops = st.params['blkio_controller']['max_rd_iops']
        wiops = st.params['blkio_controller']['max_wr_iops']
        
        # set iops limit under limit
        self.blkio.setIopsLimit(riops * 0.8, wiops * 0.8)

        # how to verify?
        iops = self.blkio.getIopUsed(self.cont_key)
        self.assertLessEqual(iops[0], riops, msg='riops still greater then upper limit')
        self.assertLessEqual(iops[1], wiops, msg='wiops still greater then upper limit')


        # set iops limit over limit
        with self.assertRaises(Exception):
            self.blkio.setIopsLimit(riops * 1.5, wiops * 1.5)

    def test_getIopUsed(self):
        self.blkio.getIopUsed(self.cont_key)
        # how to verify...?

    def test_clearIopsLimit(self):
        self.blkio.clearIopsLimit()
        # how to verify...?
