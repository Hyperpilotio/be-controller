"""
Network utilies class

Current assumptions:
- Manual entry of max throughput possible
- Each BE container has their own IP address
- Not managing bursts for now

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

# standard
import time
from datetime import datetime as dt

# hyperpilot imports
import settings as st
import netclass as netclass

def NetControll():
  """ Network controller
  """
  # initialize controller
  netst = st.params['net_controller']
  if st.verbose:
    print "Net: Starting NetControl (%s, %s, %f, %f)" \
           % (netst['iface_ext'], netst['iface_cont'], netst['max_bw_mbps'], netst['link_bw_mbps'])
  net = netclass.NetClass(netst['iface_ext'], netst['iface_cont'], \
                          netst['max_bw_mbps'], netst['link_bw_mbps'], \
                          st.params['ctlloc'])
  period = netst['period']
  cycle = 0
  was_enabled = False

  # initialize ingress stats
  old_be_ingress_mbit, old_total_ingress_mbit = net.getIngressBytesStats()
  old_time = dt.now()

  # control loop
  while 1:

    # reset limits if the controller is turned off
    if was_enabled and not st.enabled:
      for _, pod in st.active.pods.items():
        if pod.wclass == 'BE':
          net.removeIPfromFilter(pod.ipaddress)

    if not st.enabled:
      print "Net:WARNING: BE Controller is disabled, skipping net control"
      was_enabled = False
      time.sleep(period)
      continue

    if st.get_param('disabled', 'net_controller', False) is True:
      print "Net:WARNING: Net Controller is disabled"
      was_enabled = False
      time.sleep(period)
      continue

    was_enabled = True

    # get IP of all active BE containers
    active_be_ips = set()
    st.active.lock.acquire_read()
    for _, pod in st.active.pods.items():
      if pod.wclass == 'BE':
        active_be_ips.add(pod.ipaddress)
    st.active.lock.release_read()
    # track BW usage of new containers
    new_ips = active_be_ips.difference(net.cont_ips)
    for _ in new_ips:
      net.addIPtoFilter(_)
    old_ips = net.cont_ips.difference(active_be_ips)
    for _ in old_ips:
      net.removeIPfromFilter(_)

    # actual controller

    # ingress bw usage
    new_be_ingress_mbit, new_total_ingress_mbit = net.getIngressBytesStats()
    new_time = dt.now()
    elapsed_time = (new_time - old_time).total_seconds()
    total_ingress_bw = int((new_total_ingress_mbit - old_total_ingress_mbit)/elapsed_time)
    be_ingress_bw = int((new_be_ingress_mbit - old_be_ingress_mbit)/elapsed_time)
    hp_ingress_bw = total_ingress_bw - be_ingress_bw
    be_ingress_limit = net.max_bw_mbps - hp_ingress_bw - max(0.05*net.max_bw_mbps, 0.10*hp_ingress_bw)
    if be_ingress_limit < 0.0:
      be_ingress_limit = 0.0
    net.setIngressBwLimit(int(be_ingress_limit))
    old_time = new_time
    old_be_ingress_mbit = new_be_ingress_mbit
    old_total_ingress_mbit = new_total_ingress_mbit

    # egress
    egress_bw_usage = net.getEgressBwStats()
    if 1 in egress_bw_usage and 10 in egress_bw_usage:
      total_egress_bw = egress_bw_usage[1]
      be_egress_bw = egress_bw_usage[10]
      hp_egress_bw = egress_bw_usage[1] - be_egress_bw
      if hp_egress_bw < 0.0:
        hp_egress_bw = 0.0
      be_egress_limit = net.max_bw_mbps - hp_egress_bw - max(0.05*net.max_bw_mbps, 0.10*hp_egress_bw)
      if be_egress_limit < 0.0:
        be_egress_limit = 0.0
      net.setEgressBwLimit(int(be_egress_limit))
    elif st.verbose:
      print "Net:WARNING: Net stats lost, bw_usage: " + str(egress_bw_usage)

    net_cycle_data = {
        "cycle": cycle,
        "total_egress_bw": total_egress_bw,
        "hp_egress_bw": hp_egress_bw,
        "be_egress_bw": be_egress_bw,
        "be_egress_limit": be_egress_limit,
        "total_ingress_bw": total_ingress_bw,
        "hp_ingress_bw": hp_ingress_bw,
        "be_ingress_bw": be_ingress_bw,
        "be_ingress_limit": be_ingress_limit,
    }

    at = new_time.strftime('%H:%M:%S')

    # loop
    if st.verbose:
      print "Net: Net controller cycle", cycle, "at", at,
      print "Net:   Egress BW: %f (Total used) %f (HP used), %f (BE alloc)" %(total_egress_bw, hp_egress_bw, be_egress_bw)

    if st.get_param('write_metrics', 'net_controller', False) is True:
      st.stats_writer.write(at, st.node.name, "net", net_cycle_data)

    cycle += 1
    time.sleep(period)
