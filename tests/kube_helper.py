# helper.py
from kubernetes import client, config, watch
import datetime
import time
import json
import uuid

class Constants(object):
    """
    Constant Class use for kube_helper internally
    """
    # kubernetes pod status check interval
    K8S_POD_STATUS_CHECK_INTERVAL = 2
    # kubernetes response code: duplicated namespace
    K8S_RESP_CODE_DUPLICATE_NAMESPACE = 409
    # kubernetes response reason: already exist
    K8S_RESP_REASON_DUPLICATE_NAMESPACE = 'AlreadyExists'

class KubeHelper(object):
    """
    Kubernetes helper (for test)
    """
    def __init__(self):
        self.podKey = ""
        try:
            # default ckeck if this test running inside a kube clouster
            print('trying load incluster config')
            config.load_incluster_config()
            self.client = client
        except Exception as e:
            # load from Env
            try:
                print('trying load kube config file')
                config.load_kube_config()
            except Exception as e:
                print "Cannot initialize K8S environment:", e
                raise e

    def deleteDemoPods(self, namespace='kubernetes-plugin'):
        v1 = client.CoreV1Api()
        name = self.podName
        body = client.V1DeleteOptions()
        try:
            v1.delete_namespaced_pod(name, namespace, body)
        except Exception as e:
            print 'delete pod error', e

    def create_namespace(self, ns_name):
        v1 = client.CoreV1Api()
        body = client.V1Namespace()
        body.metadata = client.V1ObjectMeta(name=ns_name)
        try:
            v1.create_namespace(body)
        except client.rest.ApiException as ae:
            if ae.status == Constants.K8S_RESP_CODE_DUPLICATE_NAMESPACE and \
                    json.loads(ae.body)['reason'] == Constants.K8S_RESP_REASON_DUPLICATE_NAMESPACE:
                print "namespace {} already Exists".format(ns_name)
                return
            else:
                raise Exception("create namespace error: ", ae)
        
        
    def delete_namespace(self, namespace='kubernetes-plugin'):
        v1 = client.CoreV1Api()
        try:
            v1.delete_namespace(namespace, client.V1DeleteOptions())
        except Exception as e:
            pass

    def _generateTimeBaseRandomString(self):
        return uuid.uuid4().hex

    def createDemoPod(self, namespace='kubernetes-plugin', BE=True):
        """
        Create Demo Pod for unit test
        """
        v1 = client.CoreV1Api()
        pod = client.V1Pod()

        # check if namespace exists
        
        # if namespace not exists, create one
        self.create_namespace(namespace)


        label = {'hyperpilot.io/wclass' : 'HP'}
        postfix = self._generateTimeBaseRandomString()
        name = 'demo-hp-pod-' + postfix
        if BE:
            label = {'hyperpilot.io/wclass' : 'BE'}
            name = 'demo-be-pod-' + postfix

        pod.metadata = client.V1ObjectMeta(name=name, labels=label)
        # requirement = client.V1ResourceRequirements(requests={'cpu', '150m'})
        # pod.resources = requirement
        containers = []
        for x in range(2):
            container = client.V1Container()
            container.image = "busybox"
            container.args = ['sleep', '3600']
            container.name = 'busybox' + self._generateTimeBaseRandomString()
            container.security_context = client.V1SecurityContext(privileged=True)
            volumeMounts = []
            dockerSock = client.V1VolumeMount(mount_path='/var/run/docker.sock', name='docker-sock')
            volumeMounts.append(dockerSock)
            commandSock = client.V1VolumeMount(mount_path='/var/run/command.sock', name='command-sock')
            volumeMounts.append(commandSock)
            container.volume_mounts = volumeMounts
            containers.append(container)
        spec = client.V1PodSpec()
        spec.containers = containers
        volumes = []
        volumes.append(client.V1Volume(host_path=client.V1HostPathVolumeSource(path='/var/run/docker.sock'), name='docker-sock'))
        volumes.append(client.V1Volume(host_path=client.V1HostPathVolumeSource(path='/var/run/command.sock'), name='command-sock'))
        volumes.append(client.V1Volume(host_path=client.V1HostPathVolumeSource(path='/sbin'), name='sbin'))
        volumes.append(client.V1Volume(host_path=client.V1HostPathVolumeSource(path='/lib'), name='lib'))
        spec.volumes = volumes
        spec.security_context = client.V1SecurityContext(privileged=True)
        pod.spec = spec
        response = v1.create_namespaced_pod(namespace, pod)
        try:
            pod = self.watchForStatus(namespace, name)
            self.podKey = response.metadata.namespace + '/' + response.metadata.name
            self.podName = response.metadata.name
            return pod
        except Exception as e:
            print 'error response', e
            print response
            return ""

    def watchForStatus(self, namespace, podName):
        v1 = client.CoreV1Api()
        pod = None
        while True:
            time.sleep(Constants.K8S_POD_STATUS_CHECK_INTERVAL)
            pods = v1.list_namespaced_pod(namespace)
            find = filter(lambda x: x.metadata.name == podName, pods.items)
            # print "get pod: ", find[0]
            if find[0].status.phase == 'Running':
                # print "watched pod:", find[0]
                break
        return find[0]
