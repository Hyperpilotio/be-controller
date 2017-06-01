import unittest
from blkioclass import BlkioClass
import json
import settings as st
import os
from kube_helper import KubeHelper

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

        iops = self.blkio.getIopUsed(self.cont_key)
        self.assertLessEqual(iops[0], riops, msg='riops still greater then upper limit')
        self.assertLessEqual(iops[1], wiops, msg='wiops still greater then upper limit')

        # set iops limit over limit
        with self.assertRaises(Exception):
            self.blkio.setIopsLimit(riops * 1.5, wiops * 1.5)

        # test clearIopsLimit
        self.blkio.clearIopsLimit()
        _ = self.blkio.getIopUsed(self.cont_key)
        self.assertIsNone(_[0], msg="riops not reset, got value: {}".format(_[0]))
        self.assertIsNone(_[1], msg="wiops not reset, got value: {}".format(_[1]))