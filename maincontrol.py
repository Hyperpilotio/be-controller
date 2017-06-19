"""
Dynamic CPU controller based on the Heracles design

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
import store

# hyperpilot imports
import settings as st
import netcontrol as net
import blkiocontrol as blkio


def CpuStatsDocker():
  """Calculates CPU usage for the node using container statistics from Docker APIs
  """
  cpu_usage = 0.0
  for _, pod in st.active.pods.items():
    for _, cont in pod.containers().items():
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
        print "Main:WARNING: Problem with docker container %s" % cont.docker_name
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
    print "Main:WARNING: Problem calculating CpuStatsK8S ", e
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
    print "Main:WARNING:: Problem accessing QoS data store:", e
    return st.enabled


def SloSlackQoSDS(name):
  """ Read SLO slack from QoS data store
  """
  print "Main: Getting slack value for", name, "from QoS data store"
  try:
    _ = pycurl.Curl()
    data = BytesIO()
    _.setopt(_.URL, 'qos-data-store:7781/v1/apps/metrics')
    _.setopt(_.WRITEFUNCTION, data.write)
    _.perform()
    output = json.loads(data.getvalue())
    if output['error']:
      print "Main:WARNING: Problem accessing QoS data store: " + output['data']
      return 0.0, 0.0
    if name not in output['data']:
      print "Main:WARNING: QoS datastore does not track workload", name
      return 0.0, 0.0
    elif 'metrics' not in output['data'][name] or \
       'slack' not in output['data'][name]['metrics']:
      return 0.0, 0.0
    else:
      metrics = output['data'][name]['metrics']
      if 'latency' not in metrics:
        metrics['latency'] = 0.0
      return float(metrics['slack']), float(metrics['latency'])
  except (ValueError, pycurl.error) as e:
    print "Main:WARNING: Problem accessing QoS data store ", e
    return 0.0, 0.0

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
    _, stderr = process.communicate()
    if process.returncode != 0:
      print "Main:ERROR: Failed to enable BE on k8s: %s" % stderr


def DisableBE():
  """ kills all BE workloads
  """
  if st.k8sOn:
    body = client.V1DeleteOptions()
  # kill BE pods
  for _, pod in st.active.pods.items():
    if pod.wclass == 'BE':
      # K8s delete pod
      if st.k8sOn:
        try:
          _ = st.node.kenv.delete_namespaced_pod(pod.name, \
                  pod.namespace, body, grace_period_seconds=0, \
                  orphan_dependents=True)
        except ApiException as e:
          print "Main:WARNING: Cannot kill K8S BE pod: %s\n" % e
      else:
      # docker kill container
        try:
          for _, cont in pod.containers.items():
            cont.docker.kill()
        except docker.errors.APIError:
          print "Main:WARNING: Cannot kill container %s" % cont.name

  # taint local node
  if st.k8sOn:
    command = 'kubectl label --overwrite nodes ' + st.node.name + ' hyperpilot.io/be-enabled=false'
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, \
                                 stderr=subprocess.PIPE)
    _, stderr = process.communicate()
    if process.returncode != 0:
      print "Main:ERROR: Failed to disable BE on k8s: %s" % stderr

def SetQuotaBE(quota):
  """ allows all BE workloads to run at max quota
  """

  for _, pod in st.active.pods.items():
    for _, cont in pod.containers.items():
      if pod.wclass == 'BE':
        cont.quota = quota
        try:
          cont.docker.update(cpu_quota=cont.quota)
          print "Main: CPU quota of BE container set to %d" % (cont.quota)
        except docker.errors.APIError as e:
          print "Main:WARNING: Cannot update quota for container %s: %s" % (str(cont), e)
        # special case for disabling quota
        if quota == 0:
          cont.period = 0
          try:
            cont.docker.update(cpu_period=cont.period)
            print "Main: CPU quota of BE container set to %d" % (cont.period)
          except docker.errors.APIError as e:
            print "Main:WARNING: Cannot update quota for container %s: %s" % (str(cont), e)
          


def ResetBE():
  """ resets quota for all BE workloads to min_be_quota
  """
  min_be_quota = int(st.node.cpu * 100000 * st.params["quota_controller"]['min_be_quota'])

  for _, pod in st.active.pods.items():
    for _, cont in pod.containers.items():
      if pod.wclass == 'BE':
        old_quota = cont.quota
        cont.quota = min_be_quota
        try:
          cont.docker.update(cpu_quota=cont.quota)
          print "Main: Reset CPU quota of BE container from %d to %d" % (old_quota, cont.quota)
        except docker.errors.APIError as e:
          print "Main:WARNING: Cannot update quota for container %s: %s" % (str(cont), e)


def GrowBE(slack):
  """ grows quotas for all BE workloads by be_growth_rate
      assumption: non 0 quotas to begin with
  """
  be_growth_ratio = st.params['quota_controller']['BE_growth_ratio']
  be_growth_rate = 1 + be_growth_ratio * slack
  max_be_quota = int(st.node.cpu * 100000 * st.params['quota_controller']['max_be_quota'])
  min_be_quota = int(st.node.cpu * 100000 * st.params['quota_controller']['min_be_quota'])

  aggregate_be_quota = 0
  for _, pod in st.active.pods.items():
    for _, cont in pod.containers.items():
      if pod.wclass == 'BE':
        if not cont.period == 100000:
          cont.period = 100000
          cont.docker.update(cpu_period=100000)
        old_quota = cont.quota
        cont.quota = int(be_growth_rate * cont.quota)
        # We limit each BE container to a max quota
        if cont.quota > max_be_quota:
          cont.quota = max_be_quota
        if cont.quota < min_be_quota:
          cont.quota = min_be_quota
        try:
          cont.docker.update(cpu_quota=cont.quota)
          print "Main: Grow CPU quota of BE container in pod %s from %d to %d" % (pod.name, old_quota, cont.quota)
        except docker.errors.APIError as e:
          print "Main:WARNING: Cannot update quota for container %s: %s" % (str(cont), e)
        aggregate_be_quota += cont.quota

  st.node.be_quota = aggregate_be_quota


def ShrinkBE(slack):
  """ shrinks quota for all BE workloads by be_shrink_rate
  """
  be_shrink_ratio = st.params['quota_controller']['BE_shrink_ratio']
  be_shrink_rate = 1 + be_shrink_ratio * slack
  min_be_quota = int(st.node.cpu * 100000 * st.params['quota_controller']['min_be_quota'])
  max_be_quota = int(st.node.cpu * 100000 * st.params['quota_controller']['max_be_quota'])

  aggregate_be_quota = 0
  for _, pod in st.active.pods.items():
    for _, cont in pod.containers.items():
      if pod.wclass == 'BE':
        if not cont.period == 100000:
          pod.period = 100000
          cont.docker.update(cpu_period=100000)
        old_quota = cont.quota
        cont.quota = int(be_shrink_rate * cont.quota)
        if cont.quota < min_be_quota:
          cont.quota = min_be_quota
        if cont.quota > max_be_quota:
          cont.quota = max_be_quota
        try:
          cont.docker.update(cpu_quota=cont.quota)
          print "Main: Shrink CPU quota of BE container in pod %s from %d to %d" % (pod.name, old_quota, cont.quota)
        except docker.errors.APIError as e:
          print "Main:WARNING: Cannot update quota for container %s: %s" % (str(cont), e)
        aggregate_be_quota += cont.quota

  st.node.be_quota = aggregate_be_quota


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
        print "Main:ERROR: Error in reading configuration file %s: %s" % (args.config, e)
        sys.exit(-1)
  else:
    print "Main:ERROR: Cannot read configuration file ", args.config
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
    print "Main: Docker API initialized."
  except docker.errors.APIError:
    print "Main:ERROR: Cannot communicate with docker daemon, terminating."
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
      print "Main: K8S API initialized."
    except config.ConfigException as e:
      print "Main:ERROR: Cannot initialize K8S environment, terminating:", e
      sys.exit(-1)
    st.node.name = os.getenv('MY_NODE_NAME')
    if st.node.name is None:
      print "Main:ERROR: Cannot get node name in K8S, terminating."
      sys.exit(-1)
    # read node stats
    try:
      _ = st.node.kenv.read_node(st.node.name)
    except ApiException as e:
      print "Main:ERROR: Exception when calling CoreV1Api->read_node: %s\n" % e
      sys.exit(-1)
    st.node.cpu = int(_.status.capacity['cpu'])


def __init__():
  """ Main function of CPU controller
  """

  # parse arguments
  st.params = ParseArgs()
  st.stats_writer = store.InfluxWriter()

  if st.get_param("write_metrics", None, False) is True:
    # flatten the setting params
    stored_params = {}
    for key, val in st.params.items():
      if isinstance(val, dict):
        for ctrl_param, param_val in val.items():
          stored_params["{}.{}".format(key, ctrl_param)] = param_val
      else:
        stored_params[key] = val
    st.stats_writer.write(dt.now(), os.getenv("MY_NODE_NAME"),
                          "settings", stored_params)

  # initialize environment
  configDocker()
  configK8S()
  EnableBE()

  # simpler parameters
  slack_threshold_disable = st.params['quota_controller']['slack_threshold_disable']
  slack_threshold_reset = st.params['quota_controller']['slack_threshold_reset']
  slack_threshold_shrink = st.params['quota_controller']['slack_threshold_shrink']
  load_threshold_shrink = st.params['quota_controller']['load_threshold_shrink']
  slack_threshold_grow = st.params['quota_controller']['slack_threshold_grow']
  load_threshold_grow = st.params['quota_controller']['load_threshold_grow']
  period = st.params['quota_controller']['period']
  max_be_quota = int(st.node.cpu * 100000 * st.params["quota_controller"]['max_be_quota'])
  min_be_quota = int(st.node.cpu * 100000 * st.params["quota_controller"]['min_be_quota'])

  # launch watcher for active containers and pods
  if st.verbose:
    print "Main: Starting K8S watcher"
  try:
    _ = threading.Thread(name='K8SWatch', target=st.K8SWatch)
    _.setDaemon(True)
    _.start()
  except threading.ThreadError:
    print "Main:ERROR: Cannot start K8S watcher; terminating"
    sys.exit(-1)
  # launch other controllers
  if st.verbose:
    print "Main: Starting network controller"
  try:
    _ = threading.Thread(name='NetControll', target=net.NetControll)
    _.setDaemon(True)
    _.start()
  except threading.ThreadError:
    print "Main:WARNING: Cannot start network controller; continuing without it"
  if st.verbose:
    print "Main: Starting blkio controller"
  try:
    _ = threading.Thread(name='BlkioControll', target=blkio.BlkioControll)
    _.setDaemon(True)
    _.start()
  except threading.ThreadError:
    print "Main:WARNING: Cannot start blkio controller; continuing without it"


  # control loop
  cycle = 0
  while 1:

    old_enabled = st.enabled
    st.enabled = ControllerEnabled()

    # reset max quota  if the controller is turned off
    if old_enabled and not st.enabled:
      print "Main: Controller off, turning off quota"
      SetQuotaBE(0)
    # set min quota if the controller is tuned on
    if st.enabled and not old_enabled:
      print "Main: Controller off, setting quota to min"
      SetQuotaBE(min_be_quota)

    if not st.enabled:
      print "Main:WARNING: BE Controller is disabled, skipping main control"
      time.sleep(period)
      continue

    if st.get_param('disabled', 'quota_controller', False) is True:
      print "Main:WARNING: CPU controller is disabled"
      time.sleep(period)
      continue

    # check SLO slack from file
    slo_slack, latency = SloSlack(st.node.qos_app)

    # get CPU stats
    cpu_usage = CpuStats()

    at = dt.now().strftime('%H:%M:%S')

    quota_cycle_data = {
        "cycle": cycle,
        "qos_app": st.node.qos_app,
        "slack": slo_slack,
        "latency": latency,
        "cpu_usage": cpu_usage,
        "hp_pods": st.active.hp_pods,
        "be_pods": st.active.be_pods,
        "be_quota": st.node.be_quota
    }

    if st.verbose:
      print "Main: Quota controller cycle", cycle, "at", dt.now().strftime('%H:%M:%S')
      print "Main: Current state:"
      print "Main:   Qos app", st.node.qos_app, " SLO slack", slo_slack, " CPU utilization", cpu_usage
      print "Main:   HP (%d)" % (st.active.hp_pods)
      print "Main:   BE (%d): %d quota" % (st.active.be_pods, st.node.be_quota)

    # grow, shrink or disable control
    # Disable
    if slo_slack < slack_threshold_disable and st.active.be_pods:
      quota_cycle_data["action"] = "disable_be"
      if st.verbose:
        print "Main:Action: Disabling BE"
      DisableBE()
    # Reset to minimum
    elif slo_slack < slack_threshold_reset and st.active.be_pods:
      quota_cycle_data["action"] = "reset_be"
      if st.verbose:
        print "Main:Action: Resetting BE"
      ResetBE()
    # Shrink quota due to slack
    elif slo_slack < slack_threshold_shrink and st.active.be_pods:
      quota_cycle_data["action"] = "shrink_be"
      if st.verbose:
        print "Main:Action: Shrinking BE"
      ShrinkBE(slo_slack-slack_threshold_shrink)
    # Shrink quota due to high utilization
    elif cpu_usage > load_threshold_shrink and st.active.be_pods:
      quota_cycle_data["action"] = "shrink_be"
      if st.verbose:
        print "Main:Action: Shrinking BE"
      ShrinkBE((load_threshold_shrink - cpu_usage)/100.0)
    # Enable best effort
    elif slo_slack > slack_threshold_grow and \
         cpu_usage < load_threshold_grow and not st.active.be_pods:
      quota_cycle_data["action"] = "enable_be"
      if st.verbose:
        print "Main:Action: Enabling BE"
      EnableBE()
    # Grow best effort
    elif slo_slack > slack_threshold_grow and \
      cpu_usage < load_threshold_grow and st.active.be_pods:
      quota_cycle_data["action"] = "grow_be"
      if st.verbose:
        print "Main:Action: Growing BE"
      GrowBE(slo_slack)
    # Default
    else:
      quota_cycle_data["action"] = "none"
      if st.verbose:
        print "Main:Action: No change"

    if st.get_param('write_metrics', 'quota_controller', False) is True:
      st.stats_writer.write(at, st.node.name, "cpu_quota", quota_cycle_data)

    cycle += 1
    time.sleep(period)

__init__()
