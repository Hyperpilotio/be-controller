"""
Dynamic CPU shares controller based on the Heracles design

Current pitfalls:
- when shrinking, we penalize all BE containers instead of killing 1-2 of them

TODO
- validate CPU usage measurements

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

# standard
import time
from datetime import datetime as dt
import sys
import json
import argparse
import os.path
import os
from io import BytesIO
import subprocess
import threading
import pycurl
import docker
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# hyperpilot imports
import settings as st
import netcontrol as net


def ActiveContainers():
  """ Identifies active containers in a docker environment.
  """
  min_shares = st.params['min_shares']
  active_containers = {}
  stats = st.ControllerStats()

  # read container list from docker
  try:
    containers = st.node.denv.containers.list()
  except docker.errors.APIError:
    print "Cannot communicate with docker daemon, terminating."
    sys.exit(-1)

  for cont in containers:
    try:
      _ = st.Container()
      _.docker_id = cont.id
      _.docker_name = cont.name
      _.docker = cont
      # check container shares
      _.shares = cont.attrs['HostConfig']['CpuShares']
      if _.shares < min_shares:
        _.shares = min_shares
        cont.update(cpu_shares=_.shares)
      # check container class
      if 'hyperpilot.io/wclass' in cont.attrs['Config']['Labels']:
        _.wclass = cont.attrs['Config']['Labels']['hyperpilot.io/wclass']
      if _.wclass == 'HP':
        stats.hp_cont += 1
        stats.hp_shares += _.shares
      else:
        stats.be_cont += 1
        stats.be_shares += _.shares
      # append to dictionary of active containers
      active_containers[_.docker_id] = _
    except docker.errors.APIError:
      print "Problem with docker container"

  # Check container class in K8S
  if st.k8sOn:
    # get all best effort pods
    label_selector = 'hyperpilot.io/wclass = BE'
    try:
      pods = st.node.kenv.list_pod_for_all_namespaces(watch=False,\
                                            label_selector=label_selector)
      for pod in pods.items:
        if pod.spec.node_name == st.node.name:
          for cont in pod.status.container_statuses:
            cid = cont.container_id[len('docker://'):]
            if cid in active_containers:
              if active_containers[cid].wclass == 'HP':
                active_containers[cid].wclass = 'BE'
                stats.be_cont += 1
                stats.be_shares += active_containers[cid].shares
                stats.hp_cont -= 1
                stats.hp_shares -= active_containers[cid].shares
              active_containers[cid].k8s_pod_name = pod.metadata.name
              active_containers[cid].k8s_namespace = pod.metadata.namespace
              active_containers[cid].ipaddress = pod.status.pod_ip
    except (ApiException, TypeError, ValueError):
      print "Cannot talk to K8S API server, labels unknown."
    # get first qos tracked workload on this node, if it exists
    label_selector = 'hyperpilot.io/qos=true'
    try:
      pods = st.node.kenv.list_pod_for_all_namespaces(watch=False,\
                                            label_selector=label_selector)
      if len(pods.items) > 1:
        print "Multiple QoS tracked workloads, ignoring all but first"

      st.node.qos_app = pods.items[0].status.container_statuses[0].name

      #for pod in pods.items:
       # if pod.spec.node_name == st.node.name:
        #  break
    except (ApiException, TypeError, ValueError, IndexError):
      print "Cannot find QoS service name"

  return active_containers, stats


def CpuStatsDocker():
  """Calculates CPU usage for the node using container statistics from Docker APIs
  """
  cpu_usage = 0.0
  for _, cont in st.active_containers.items():
    try:
      percent = 0.0
      new_stats = cont.docker.stats(stream=False, decode=True)
      new_cpu_stats = new_stats['cpu_stats']
      past_cpu_stats = new_stats['precpu_stats']
      cpu_delta = float(new_cpu_stats['cpu_usage']['total_usage']) - \
                  float(past_cpu_stats['cpu_usage']['total_usage'])
      system_delta = float(new_cpu_stats['system_cpu_usage']) - \
                     float(past_cpu_stats['system_cpu_usage'])
      # The percentages are system-wide, not scaled per core
      if (system_delta > 0.0) and (cpu_delta > 0.0):
        percent = (cpu_delta / system_delta) * 100.0
      cont.cpu_percent = percent
      cpu_usage += percent
    except docker.errors.APIError:
      print "Problem with docker container %s" % cont.docker_name
  return cpu_usage


def CpuStatsK8S():
  """Calculates CPU usage for the node using statistics from K8S APIs, in percentage value
  """
  try:
    _ = pycurl.Curl()
    data = BytesIO()
    _.setopt(_.URL, st.node.name + ':10255/stats/summary')
    _.setopt(_.WRITEFUNCTION, data.write)
    _.perform()
    output = json.loads(data.getvalue())
    usage_nano_cores = output['node']['cpu']['usageNanoCores']
    cpu_usage = usage_nano_cores / (st.node.cpu * 1E9) * 100.0
    return cpu_usage
  except (ValueError, pycurl.error)  as e:
    print "Problem calculating CpuStatsK8S ", e
    return 100.0

def CpuStats():
  """ Calculates CPU usage statistics
  """
  if st.k8sOn:
    return CpuStatsK8S()
  else:
    return CpuStatsDocker()


def SloSlackFile():
  """ Read SLO slack from local file
  """
  with open('slo_slack.txt') as _:
    array = [[float(x) for x in line.split()] for line in _]
  return array[0][0]

def ControllerEnabled():
  try:
    _ = pycurl.Curl()
    data = BytesIO()
    _.setopt(_.URL, 'qos-data-store:7781/v1/switch')
    _.setopt(_.WRITEFUNCTION, data.write)
    _.perform()
    output = json.loads(data.getvalue())
    return output['data']
  except (ValueError, pycurl.error) as e:
    print "Problem accessing QoS data store:", e
    return st.enabled


def SloSlackQoSDS(name):
  """ Read SLO slack from QoS data store
  """
  print "  Getting slack value for", name, "from QoS data store"
  try:
    _ = pycurl.Curl()
    data = BytesIO()
    _.setopt(_.URL, 'qos-data-store:7781/v1/apps/metrics')
    _.setopt(_.WRITEFUNCTION, data.write)
    _.perform()
    output = json.loads(data.getvalue())
    if output['error']:
      print "Problem accessing QoS data store: " + output['data']
      return 0.0
    if name not in output['data']:
      print "QoS datastore does not track workload", name
      return 0.0
    elif 'metrics' not in output['data'][name] or \
       'slack' not in output['data'][name]['metrics']:
      return 0.0
    else:
      return float(output['data'][name]['metrics']['slack'])
  except (ValueError, pycurl.error) as e:
    print "Problem accessing QoS data store ", e
    return 0.0

def SloSlack(name):
  """ Read SLO slack
  """
  return SloSlackQoSDS(name)
#  return SloSlackFile()


def EnableBE():
  """ enables BE workloads, locally
  """
  if st.k8sOn:
    command = 'kubectl label --overwrite nodes ' + st.node.name + ' hyperpilot.io/be-enabled=true'
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, \
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
      print "Failed to enable BE on k8s: %s" % stderr


def DisableBE():
  """ kills all BE workloads
  """
  if st.k8sOn:
    body = client.V1DeleteOptions()
  # kill BE containers
  for _, cont in st.active_containers.items():
    if cont.wclass == 'BE':
      # K8s delete pod
      if st.k8sOn:
        try:
          _ = st.node.kenv.delete_namespaced_pod(cont.k8s_pod_name, \
                  cont.k8s_namespace, body, grace_period_seconds=0, \
                  orphan_dependents=True)
        except ApiException as e:
          print "Cannot kill K8S BE pod: %s\n" % e
      else:
      # docker kill container
        try:
          cont.docker.kill()
        except docker.errors.APIError:
          print "Cannot kill container %s" % cont.name

  # taint local node
  if st.k8sOn:
    command = 'kubectl label --overwrite nodes ' + st.node.name + ' hyperpilot.io/be-enabled=false'
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, \
                                 stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
      print "Failed to disable BE on k8s: %s" % stderr


def GrowBE():
  """ grows number of shares for all BE workloads by be_growth_rate
      assumption: non 0 shares
  """
  be_growth_rate = st.params['BE_growth_rate']
  for _, cont in st.active_containers.items():
    if cont.wclass == 'BE':
      old_shares = cont.shares
      new_shares = int(be_growth_rate*cont.shares)
      # if initial shares is very small, boost quickly
      if new_shares == cont.shares:
        new_shares = 2 * cont.shares
      cont.shares = new_shares
      if new_shares == old_shares:
        print "Skip growing CPU shares as new shares remains unchanged", str(new_shares)
        continue

      try:
        cont.docker.update(cpu_shares=cont.shares)
        print "Grow CPU shares of BE container from %d to %d" % (old_shares, new_shares)
      except docker.errors.APIError as e:
        print "Cannot update shares for container %s: %s" % (str(cont), e)


def ShrinkBE():
  """ shrinks number of shares for all BE workloads by be_shrink_rate
      warning: it does not work if shares are 0 to begin with
  """
  be_shrink_rate = st.params['BE_shrink_rate']
  min_shares = st.params['min_shares']
  for _, cont in st.active_containers.items():
    if cont.wclass == 'BE':
      old_shares = cont.shares
      new_shares = int(be_shrink_rate*cont.shares)
      if new_shares == cont.shares:
        new_shares = int(cont.shares/2)
      if new_shares < min_shares:
        new_shares = min_shares
      cont.shares = new_shares

      if new_shares == old_shares:
        print "Skip shrinking CPU shares as new shares remains unchanged", str(new_shares)
        continue

      try:
        cont.docker.update(cpu_shares=cont.shares)
        print "Shrink CPU shares of BE container from %d to %d" % (old_shares, new_shares)
      except docker.errors.APIError as e:
        print "Cannot update shares for container %s: %s" % (str(cont), e)


def ParseArgs():
  """ parse arguments and print config
  """
  # argument parsing
  parser = argparse.ArgumentParser()
  parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
  parser.add_argument("-c", "--config", type=str, required=False, default="config.json",
                      help="configuration file (JSON)")
  args = parser.parse_args()
  if args.verbose:
    st.verbose = True

  # read configuration file
  if os.path.isfile(args.config):
    with open(args.config, 'r') as json_data_file:
      try:
        params = json.load(json_data_file)
      except ValueError as e:
        print "Error in reading configuration file %s: %s" % (args.config, e)
        sys.exit(-1)
  else:
    print "Cannot read configuration file ", args.config
    sys.exit(-1)

  # frequently used parameters
  st.k8sOn = (params['mode'] == 'k8s')

  # k8s setup
  if 'ctlloc' not in params:
    params['ctlloc'] = 'in'

  # print configuration parameters
  print "Configuration:"
  for _ in params:
    print "  ", _, params[_]
  print

  return params


def configDocker():
  """ configure Docker environment
      current version does not record node capacity
  """
  # always initialize docker
  try:
    st.node.denv = docker.from_env()
    print "Docker API initialized."
  except docker.errors.APIError:
    print "Cannot communicate with docker daemon, terminating."
    sys.exit(-1)


def configK8S():
  """ configure K8S environment
  """
  if st.k8sOn:
    try:
      if st.params['ctlloc'] == 'in':
        config.load_incluster_config()
      else:
        config.load_kube_config()
      st.node.kenv = client.CoreV1Api()
      print "K8S API initialized."
    except config.ConfigException as e:
      print "Cannot initialize K8S environment, terminating:", e
      sys.exit(-1)
    st.node.name = os.getenv('MY_NODE_NAME')
    if st.node.name is None:
      print "Cannot get node name in K8S, terminating."
      sys.exit(-1)
    # read node stats
    try:
      _ = st.node.kenv.read_node(st.node.name)
    except ApiException as e:
      print "Exception when calling CoreV1Api->read_node: %s\n" % e
      sys.exit(-1)
    st.node.cpu = int(_.status.capacity['cpu'])
    EnableBE()


def __init__():
  """ Main function of shares controller
  """
  # parse arguments
  st.params = ParseArgs()

  # initialize environment
  configDocker()
  configK8S()

  # simpler parameters
  slack_threshold_disable = st.params['slack_threshold_disable']
  slack_threshold_shrink = st.params['slack_threshold_shrink']
  load_threshold_shrink = st.params['load_threshold_shrink']
  slack_threshold_grow = st.params['slack_threshold_grow']
  load_threshold_grow = st.params['load_threshold_grow']
  period = st.params['period']

  # launch other controllers
  if st.verbose:
    print "Starting network controller"
  try:
    _ = threading.Thread(name='NetControll', target=net.NetControll)
    _.setDaemon(True)
    _.start()
  except threading.ThreadError:
    print "Cannot start network controller; continuing without it"


  # control loop
  cycle = 0
  while 1:
    st.enabled = ControllerEnabled()

    if not st.enabled:
      print "BE Controller is disabled, skipping main control"
      time.sleep(period)
      continue

    if st.get_param('shared_controller_disabled', False) is True:
      print "Shares Controller is disabled"
      time.sleep(period)
      continue

    # check SLO slack from file
    slo_slack = SloSlack(st.node.qos_app)

    # get active containers and their class
    st.active_containers, stats = ActiveContainers()
    # get CPU stats
    cpu_usage = CpuStats()

    # grow, shrink or disable control
    if slo_slack < slack_threshold_disable:
      if st.verbose:
        print " Disabling BE phase"
      DisableBE()
    elif slo_slack < slack_threshold_shrink or \
         cpu_usage > load_threshold_shrink:
      if st.verbose:
        print " Shrinking BE phase"
      ShrinkBE()
    elif slo_slack > slack_threshold_grow and \
         cpu_usage < load_threshold_grow:
      if st.verbose:
        print " Enabling and Growing BE phase"
      EnableBE()
      GrowBE()
    else:
      if st.verbose:
        print " Enabling BE phase"
      EnableBE()

    if st.verbose:
      print "Shares controller cycle", cycle, "at", dt.now().strftime('%H:%M:%S')
      print " Qos app", st.node.qos_app, ", slack", slo_slack, ", CPU utilization", cpu_usage
      print " HP (%d): %d shares" % (stats.hp_cont, stats.hp_shares)
      print " BE (%d): %d shares" % (stats.be_cont, stats.be_shares)
    cycle += 1
    time.sleep(period)

__init__()
