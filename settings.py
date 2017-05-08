"""
Global settings the best-effort workload controller

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

import store

class Container(object):
  """ A class for tracking active containers
  """
  def __init__(self):
    self.docker_name = ''
    self.k8s_pod_name = ''
    self.k8s_namespace = ''
    self.docker_id = 0
    self.wclass = 'HP'
    self.shares = 0
    self.period = 0
    self.quota = 0
    self.docker = None
    self.cpu_percent = 0
    self.ipaddress = ''

  def __repr__(self):
    return "<Container:%s pod:%s class:%s>" \
           % (self.docker_name, self.k8s_pod_name, self.wclass)

  def __str__(self):
    return "<Container:%s pod:%s class:%s>" \
           % (self.docker_name, self.k8s_pod_name, self.wclass)


class ControllerStats(object):
  """ A class for tracking controller stats
  """
  def __init__(self):
    self.hp_cont = 0
    self.be_cont = 0
    self.hp_shares = 0
    self.be_shares = 0
    self.be_quota = 0
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
    self.docker = None

# globals
# controller parameters
params = {}
verbose = False
k8sOn = True
# all active containers
active_containers = {}
# 1 -> BE grow, -1 -> BE disable, 0 -> no action
status = 0
# node info
node = NodeInfo()
enabled = False

def get_param(name, section=None, default=None):
  keys = params
  if section and section in params:
    keys = keys[section]
  elif section:
    return default

  if name in keys:
    return keys[name]

  return default

stats_writer = store.InfluxWriter()
