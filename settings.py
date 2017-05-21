"""
Global settings and utility functions for the best-effort workload controller

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

import sys
import docker
from kubernetes import watch
import rwlock
import store

class Container(object):
  """ A class for tracking active containers
  """
  def __init__(self):
    self.docker_name = ''
    self.docker_id = 0
    self.docker = None
    self.ipaddress = ''
    self.period = 0
    self.quota = 0
    self.cpu_percent = 0

  def __repr__(self):
    return "<Container:%s pod:%s>" %(self.docker_name)

  def __str__(self):
    return "<Container:%s pod:%s>" %(self.docker_name)

class Pod(object):
  """ A class for tracking active pods
  """
  def __init__(self):
    self.name = ''
    self.namespace = ''
    self.uid = ''
    self.qosclass = ''
    self.wclass = ''
    self.ipaddress = ''
    self.container_ids = set()
    self.containers = {}


class ActivePods(object):
  """ A class for tracking active pods
  """
  def __init__(self):
    self.pods = {}
    self.lock = rwlock.ReadWriteLock()
    self.hp_pods = 0
    self.be_pods = 0

  def delete_pod(self, key):
    """ Stop tracking pod
    """
    pod = self.pods[key]
    if pod.wclass == 'BE':
      self.be_pods -= 1
    else:
      self.hp_pods -= 1
    self.lock.acquire_write()
    pod.containers.clear()
    pod.container_ids.clear()
    self.pods.pop(key)
    self.lock.release_write()
    if verbose:
      print "K8SWatch: DELETED pod %s" %(key)

  def add_pod(self, k8s_object, key):
    """ Track new pod
    """
    if key in self.pods:
      print "K8SWatch:WARNING: Duplicate pod %s" %(key)
    pod = Pod()
    pod.name = k8s_object.metadata.name
    pod.namespace = k8s_object.metadata.namespace
    pod.uid = k8s_object.metadata.uid
    pod.ipaddress = k8s_object.status.pod_ip
    pod.qosclass = k8s_object.status.qos_class.lower()
    pod.wclass = ExtractWClass(k8s_object)
    if pod.wclass == 'BE' and pod.qosclass != 'besteffort':
      print "K8SWatch:WARNING: Pod %s is not BestEffort in K8S" %(key)
    if pod.wclass == 'BE':
      self.be_pods += 1
    else:
      self.hp_pods += 1
    self.lock.acquire_write()
    self.pods[key] = pod
    self.lock.release_write()
    if verbose:
      print "K8SWatch: ADDED pod %s (%s, %s)" %(key, pod.qosclass, pod.wclass)

  def modify_pod(self, k8s_object, key, min_quota):
    """ Modify tracked pod
    """
    pod = self.pods[key]
    # set with containers in event
    new_cont = set()
    for cont in k8s_object.status.container_statuses:
      if not cont.container_id:
        continue
      cid = cont.container_id[len('docker://'):]
      new_cont.add(cid)
    added_cont = new_cont.difference(pod.container_ids)
    deleted_cont = pod.container_ids.difference(new_cont)
    # process all added containers
    for _ in added_cont:
      c = Container()
      c.docker_id = _
      c.ipaddress = k8s_object.status.pod_ip
      try:
        c.docker = node.denv.containers.get(_)
      except (docker.errors.NotFound, docker.errors.APIError):
        print "K8SWatch:WARNING: Cannot find containers %s for pod %s" %(_, key)
        continue
      c.docker_name = c.docker.name
      c.quota = c.docker.attrs['HostConfig']['CpuQuota']
      c.period = c.docker.attrs['HostConfig']['CpuPeriod']
      # if the controller is enabled, set min quota for BE pods
      if enabled and pod.wclass == 'BE':
        if c.period != 100000:
          c.period = 100000
          c.docker.update(cpu_period=100000)
        if c.quota != min_quota:
          c.quota = min_quota
          c.docker.update(cpu_quota=c.quota)
      self.lock.acquire_write()
      pod.container_ids.add(_)
      pod.containers[_] = c
      self.lock.release_write()
    # process all deleted containers
    self.lock.acquire_write()
    for _ in deleted_cont:
      pod.containers.pop(_)
      pod.container_ids.remove(_)
    self.lock.release_write()
    if verbose:
      print "K8SWatch:UPDATED pod %s (%s, %s)" %(key, pod.qosclass, pod.wclass)


class NodeInfo(object):
  """ A class for tracking node stats
  """
  def __init__(self):
    # config
    self.cpu = 0
    self.name = ''
    self.qos_app = ''
    self.kenv = None
    self.denv = None
    # stats
    self.hp_cpu_percent = 0
    self.be_cpu_percent = 0
    self.be_quota = 0


def ExtractWClass(item):
  """ Extracts metadata label from V1Pod object
  """
  try:
    if item.metadata.labels['hyperpilot.io/wclass'] == 'BE':
      return 'BE'
    else:
      return 'HP'
  except (KeyError, NameError):
    return 'HP'

# globals
# controller parameters
verbose = False
k8sOn = True
enabled = False
reset_limits = False
params = {}
def get_param(name, section=None, default=None):
  keys = params
  if section and section in params:
    keys = keys[section]
  elif section:
    return default

  if name in keys:
    return keys[name]
  return default
# all active containers and pods
active = ActivePods()
# node info
node = NodeInfo()
# stats writer
stats_writer = store.InfluxWriter()

def K8SWatch():
  """ Maintains the list of active containers.
  """
  w = watch.Watch()
  selector = ''
  timeout = 100000
  min_quota = int(node.cpu * 100000 * params["quota_controller"]['min_be_quota'])

  # infinite loop listening to K8S pod events
  for event in w.stream(node.kenv.list_pod_for_all_namespaces,\
                        timeout_seconds=timeout, label_selector=selector):
    k8s_object = event['object']
    pod_key = k8s_object.metadata.namespace + '/' + k8s_object.metadata.name
    modify_event = (event['type'] == 'MODIFIED')
    add_event = (event['type'] == 'ADDED')
    delete_event = (event['type'] == 'DELETED') or (k8s_object.status.phase == 'Succeeded') \
                   or (k8s_object.status.phase == 'Failed')

    # check if this the QoS app and needs to be tracked
    try:
      if k8s_object.metadata.labels['hyperpilot.io/qos'] == 'true':
        if add_event or modify_event:
          node.qos_app = k8s_object.status.container_statuses[0].name
          if verbose:
            print "K8SWatch: Found QoS workload %s" %node.qos_app
        elif delete_event:
          if verbose:
            print "K8SWatch: Deleting QoS workload %s" %pod_key
          node.qos_app = ''
    except (KeyError, NameError, TypeError):
      pass

    # skip all events for pods for on different/unspecified nodes
    if not k8s_object.spec.node_name == node.name:
      continue
    if verbose:
      print "K8SWatch: Watcher (%d): %s %s" % (len(active.pods), event['type'], pod_key)

    # type of event and processing needed
    tracked_pod = (pod_key in active.pods)
    has_containers = (k8s_object.status.container_statuses) and \
                     len(k8s_object.status.container_statuses) and \
                     (k8s_object.status.container_statuses[0].container_id)
    add_pod = (add_event or modify_event) and  (not tracked_pod) and has_containers
    modify_pod = (add_event or modify_event) and has_containers
    delete_pod = delete_event and tracked_pod
    if not(add_pod or modify_pod or delete_pod):
      continue

    # add new pod
    if add_pod:
      active.add_pod(k8s_object, pod_key)
    # modify pod
    if modify_pod:
      active.modify_pod(k8s_object, pod_key, min_quota)
    # remove a pod
    if delete_pod:
      active.delete_pod(pod_key)


  #not watching K8S anymore
  print "K8SWatch:ERROR: cannot watch K8S pods stream anynore, terminating"
  sys.exit(-1)
