# test_settings.py
import unittest
import settings as st
from kube_helper import KubeHelper
import time
import docker
import os


class ActivePodsTestCase(unittest.TestCase):

    def setUp(self):
        self.helperBE = KubeHelper()
        self.helperHP = KubeHelper()
        self.demoBEPod = self.helperBE.createDemoPod(BE=True)
        self.demoHPPod = self.helperHP.createDemoPod(BE=False)
        self.podBEkey = self.helperBE.podKey
        self.podHPkey = self.helperHP.podKey
        self.configSetting()

    def tearDown(self):
        self.helperBE.deleteDemoPods()
        self.helperHP.deleteDemoPods()

    def configSetting(self):
        helper = KubeHelper()
        st.node.keuv = helper.client.CoreV1Api()
        st.node.denv = docker.from_env()
        st.enabled = True
        
    def test_crud_pod(self):

        # test add pod
        oriBEPodCnt = st.active.be_pods
        st.active.add_pod(self.demoBEPod, self.podBEkey)
        self.assertTrue(len(list(filter(lambda x: st.active.pods[x].name == self.demoBEPod.metadata.name, st.active.pods))) > 0, msg="BE Pod not added to tracking list")
        self.assertTrue((st.active.be_pods - oriBEPodCnt == 1), msg="be_pods not update")

        oriHPPodCnt = st.active.hp_pods
        st.active.add_pod(self.demoHPPod, self.podHPkey)
        self.assertTrue(len(list(filter(lambda x: st.active.pods[x].name == self.demoHPPod.metadata.name, st.active.pods))) > 0, msg="HP Pod not added to tracking list")
        self.assertTrue((st.active.hp_pods - oriHPPodCnt == 1), msg="hp_pods number not update")

        # test modify pod
        minQuota = 50000
        cont_id = ''
        cont = None
        try:
            cont_id = self.demoBEPod.status.container_statuses[0].container_id.strip('docker://')
            cont = st.node.denv.containers.get(cont_id)

        except Exception as e:
            cont_id = self.demoBEPod.status.container_statuses[1].container_id.strip('docker://')
            cont = st.node.denv.containers.get(cont_id)
        
        beforeCpuQuota = cont.attrs['HostConfig']['CpuQuota']
        beforeCpuPeriod = cont.attrs['HostConfig']['CpuPeriod']
        
        st.active.modify_pod(self.demoBEPod, self.podBEkey, minQuota)
        
        time.sleep(10)
        cont = st.node.denv.containers.get(cont_id)

        afterCpuQuota = cont.attrs['HostConfig']['CpuQuota']
        afterCpuPeriod = cont.attrs['HostConfig']['CpuPeriod']
        
        self.assertEqual(minQuota, afterCpuQuota, msg="Not apply correctly")
        self.assertEqual(100000, afterCpuPeriod, msg="Cpu Period is not apply")


        st.active.modify_pod(self.demoHPPod, self.podHPkey, minQuota)
        try:
            cont_id = self.demoHPPod.status.container_statuses[0].container_id.strip('docker://')
            cont = st.node.denv.containers.get(cont_id)
        except Exception as e:
            cont_id = self.demoHPPod.status.container_statuses[1].container_id.strip('docker://')
            cont = st.node.denv.containers.get(cont_id)

        afterCpuQuota = cont.attrs['HostConfig']['CpuQuota']
        afterCpuPeriod = cont.attrs['HostConfig']['CpuPeriod']
        
        self.assertNotEqual(minQuota, afterCpuQuota, msg="should not apply cpu quota")
        self.assertNotEqual(100000, afterCpuPeriod, msg="should not apply cpu period")

        # st.active.modify_pod(self.demoPod, self.podkey)

        # def test delete pod
        st.active.delete_pod(self.podBEkey)
        self.assertEqual(len(list(filter(lambda x: st.active.pods[x].name == self.demoBEPod.metadata.name, st.active.pods))), 0, msg="BE pod not stop tracking")
        st.active.delete_pod(self.podHPkey)
        self.assertEqual(len(list(filter(lambda x: st.active.pods[x].name == self.demoHPPod.metadata.name, st.active.pods))), 0, msg="HP pod not stop tracking")

    # def test_ExtractWClass(self):
        self.assertEqual(st.ExtractWClass(self.demoBEPod), 'BE', msg="expect extract BE, but it's not")
        self.assertEqual(st.ExtractWClass(self.demoHPPod), 'HP', msg="expect extract HP, but it's not")

    # def test_K8SWatch(self):
        # print "Hmm...i donno know how to test this function..., let's just pass this for now"

