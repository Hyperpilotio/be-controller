"""
Global settings and utility functions for the best-effort workload controller

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

import sys
import threading
import docker
from kubernetes import watch

class Container(object):
  """ A class for tracking active containers
  """
  def __init__(self):
    self.docker_name = ''
    self.docker_id = 0
    self.k8s_pod_name = ''
    self.k8s_namespace = ''
    self.docker = None
    self.ipaddress = ''
    self.period = 0
    self.quota = 0
    self.cpu_percent = 0

  def __repr__(self):
    return "<Container:%s pod:%s>" \
           % (self.docker_name, self.k8s_pod_name)

  def __str__(self):
    return "<Container:%s pod:%s>" \
           % (self.docker_name, self.k8s_pod_name)

class Pod(object):
  """ A class for tracking active pods
  """
  def __init__(self):
    self.name = ''
    self.namespace = ''
    self.qosclass = ''
    self.wclass = ''
    self.ipaddress = ''
    self.container_ids = set()
    self.containers = {}

class ControllerStats(object):
  """ A class for tracking controller stats
  """
  def __init__(self):
    self.hp_pods = 0
    self.be_pods = 0
    self.hp_cpu_percent = 0
    self.be_cpu_percent = 0


class NodeInfo(object):
  """ A class for tracking node stats
  """
  def __init__(self):
    self.cpu = 0
    self.name = ''
    self.qos_app = ''
    self.kenv = None
    self.denv = None

# globals
# controller parameters
verbose = False
k8sOn = True
enabled = False
params = {}
def get_param(name, default):
  if name in params:
    return params[name]
  return default
# all active containers and pods
active_pods = {}
active_pods_rlock = threading.RLock()
# node info
node = NodeInfo()
# node stats
stats = ControllerStats()

def ExtractWClass(pod):
  """ Extract Hyperpilot workload class.
  """
  try:
    if pod.metadata.labels['hyperpilot.io/wclass'] == 'BE':
      return 'BE'
    else:
      return 'HP'
  except (KeyError, NameError):
    return 'HP'


def ActiveContainers():
  """ Maintains the list of active containers.
  """
  min_be_quota = int(node.cpu * 100000 * params['min_be_quota'])
  max_be_quota = int(node.cpu * 100000 * params['max_be_quota'])

  w = watch.Watch()
  selector = ''
  timeout = 100000

  # infinite loop listening to K8S pod events
  for event in w.stream(node.kenv.list_pod_for_all_namespaces,\
                        timeout_seconds=timeout, label_selector=selector):
    # type of event
    pod = event['object']
    pod_key = pod.metadata.namespace + '/' + pod.metadata.name
    modify_event = (event['type'] == 'MODIFIED')
    add_event = (event['type'] == 'ADDED')
    delete_event = (event['type'] == 'DELETED')
    delete_event |= pod.status.phase == 'Succeeded'
    delete_event |= pod.status.phase == 'Failed'
    if verbose:
      print "Watcher: pods %d" %(len(active_pods))
      print "Event: %s %s" % (event['type'], pod_key)

    # check if this the QoS app and needs to be tracked
    try:
      if pod.metadata.labels['hyperpilot.io/qos'] == 'true':
        if add_event or modify_event:
          node.qos_app = pod.status.container_statuses[0].name
          if verbose:
            print "Found QoS workload %s" %node.qos_app
        elif delete_event:
          if verbose:
            print "Deleting QoS workload %s" %pod_key
          node.qos_app = ''
    except (KeyError, NameError):
      pass

    # skip all events for pods for on different/unspecified nodes
    if not pod.spec.node_name == node.name:
      continue

    # determine the processing needed (add, modify, delete)
    add_pod = False
    delete_pod = False
    modify_pod = False
    tracked_pod = (pod_key in active_pods)
    has_containers = (pod.status.container_statuses) and \
                     len(pod.status.container_statuses) and \
                     (pod.status.container_statuses[0].container_id)
    if (add_event or modify_event) and  (not tracked_pod) and has_containers:
      add_pod = True
    if (add_event or modify_event) and has_containers:
      modify_pod = True
    if delete_event and tracked_pod:
      delete_pod = True
    if not(add_pod or modify_pod or delete_pod):
      continue

    # add new pod
    if add_pod:
      p = Pod()
      p.name = pod.metadata.name
      p.namespace = pod.metadata.namespace
      p.ipaddress = pod.status.pod_ip
      p.qosclass = pod.status.qos_class
      p.wclass = ExtractWClass(pod)
      if p.wclass == 'BE' and p.qosclass != 'BestEffort':
        print "WARNING: Pod %s in namespace %s is not BestEffort in K8S" %(p.name, p.namespace)
      if p.wclass == 'BE':
        stats.be_pods += 1
      else:
        stats.hp_pods += 1
      active_pods_rlock.acquire()
      active_pods[pod_key] = p
      active_pods_rlock.release()
      if verbose:
        print "ADDED pod %s (%s, %s)" %(pod_key, p.qosclass, p.wclass)

    # modify pod
    if modify_pod:
      p = active_pods[pod_key]
      # set with containers in event
      new_set = set()
      for cont in pod.status.container_statuses:
        if not cont.container_id:
          continue
        cid = cont.container_id[len('docker://'):]
        new_set.add(cid)
      added_cont = new_set.difference(p.container_ids)
      deleted_cont = p.container_ids.difference(new_set)
      # process all added containers
      for _ in added_cont:
        c = Container()
        c.docker_id = _
        c.k8s_pod_name = pod.metadata.name
        c.k8s_namespace = pod.metadata.namespace
        c.ipaddress = pod.status.pod_ip
        try:
          c.docker = node.denv.containers.get(_)
        except (docker.errors.NotFound, docker.errors.APIError):
          print "WARNING: Cannot find containers %s for pod %s" %(_, pod_key)
          continue
        c.docker_name = c.docker.name
        c.quota = c.docker.attrs['HostConfig']['CpuQuota']
        c.period = c.docker.attrs['HostConfig']['CpuPeriod']
        if p.wclass == 'BE' and not p.period == 100000:
          c.period = 100000
          c.docker.update(cpu_period=100000)
        if p.wclass == 'BE' and (c.quota < min_be_quota or c.quota > max_be_quota):
          c.quota = min_be_quota
          c.docker.update(cpu_quota=c.quota)
        active_pods_rlock.acquire()
        p.container_ids.add(_)
        p.containers[_] = c
        active_pods_rlock.release()
      # process all deleted containers
      active_pods_rlock.acquire()
      for _ in deleted_cont:
        p.containers.pop(_)
        p.container_ids.remove(_)
      active_pods_rlock.release()
      if verbose:
        print "UPDATED pod %s (%s, %s)" %(pod_key, p.qosclass, p.wclass)

    # remove a pod
    if delete_pod:
      p = active_pods[pod_key]
      if p.wclass == 'BE':
        stats.be_pods -= 1
      else:
        stats.hp_pods -= 1
      active_pods_rlock.acquire()
      p.containers.clear()
      p.container_ids.clear()
      active_pods.pop(pod_key)
      active_pods_rlock.release()
      if verbose:
        print "DELETED pod %s" %(pod.metadata.name)


  #not watching K8S anymore
  print "ERROR: cannot watch K8S pods stream anynore, terminating"
  sys.exit(-1)
