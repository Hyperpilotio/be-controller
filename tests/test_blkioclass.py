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
        self.cont_key = "kubepods/besteffort/pod{podId}/{contId}".format(podId=self.demoPod.metadata.uid, contId=self.demoPod.status.container_statuses[0].container_id.strip("docker://"))
        fileDir = os.path.dirname(os.path.realpath('__file__'))
        with open(os.path.join(fileDir, 'config.json'), 'r') as json_data_file:
            st.params = json.load(json_data_file)
        netst = st.params['blkio_controller']
        print "set max read iops: {}, max write iops: {}".format(netst['max_rd_iops'], netst['max_wr_iops'])
        self.blkio = BlkioClass(netst['block_dev'], netst['max_rd_iops'], netst['max_wr_iops'])
        st.enabled = True

    def tearDown(self):
        self.kubehelper.deleteDemoPods()

    def generateContKey(self, v1pod, container_id):
        path_template = "kubepods/besteffort/pod{podId}/{contId}"
        return path_template.format(podId=v1pod.metadata.uid, contId=container_id)

    def test_blkio(self):
        # add be cont
        
        self.blkio.addBeCont(self.cont_key)
        self.assertTrue(self.cont_key in self.blkio.keys, msg='container key not add to keys')

        # double add same container id
        self.assertRaises
        with self.assertRaises(Exception):
            self.blkio.addBeCont(self.cont_key)

    # test_setIopsLimit(self):
        print "test setIopsLimit"
        riops = st.params['blkio_controller']['max_rd_iops']
        wiops = st.params['blkio_controller']['max_wr_iops']
        
        # set iops limit under limit
        self.blkio.setIopsLimit(riops * 0.8, wiops * 0.8)

        iops = self.blkio.getIopUsed(self.cont_key)
        self.assertLessEqual(iops[0], riops, msg='riops still greater then upper limit')
        self.assertLessEqual(iops[1], wiops, msg='wiops still greater then upper limit')

        print "set iops limit over limit: riops: {}, wiops: {}".format(riops * 1.5, wiops * 1.5)

        with self.assertRaises(Exception):
            self.blkio.setIopsLimit(riops * 1.5, wiops * 1.5)

        print "test clearIopsLimit"
        self.blkio.clearIopsLimit()
        _ = self.blkio.getIopUsed(self.cont_key)
        print "after clear iops limit: {}".format(_)
        self.assertEqual(0, _[0], msg="riops not reset, got value: {}".format(_[0]))
        self.assertEqual(0, _[1], msg="wiops not reset, got value: {}".format(_[1]))

        # remove be cont
        self.blkio.removeBeCont(self.cont_key)
        self.assertFalse(self.cont_key in self.blkio.keys, msg='container key still remain in keys')

