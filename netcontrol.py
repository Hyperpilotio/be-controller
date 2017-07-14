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
                          netst['default_limit_mbps'], st.params['ctlloc'])
  period = netst['period']
  cycle = 0
  was_enabled = False

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

    # get stats, calculate new limits, do sanity checks
    ingress_total_mbps, ingress_be_mbps, egress_total_mbps, egress_be_mbps = \
      net.currentStats()
    ingress_hp_mbps = ingress_total_mbps - ingress_be_mbps
    egress_hp_mbps = egress_total_mbps - egress_be_mbps

    be_ingress_limit = net.max_bw_mbps - ingress_hp_mbps - \
      max(0.10*net.max_bw_mbps, 0.10*ingress_hp_mbps)
    be_egress_limit = net.max_bw_mbps - egress_hp_mbps - \
      max(0.10*net.max_bw_mbps, 0.10*egress_hp_mbps)
    if be_ingress_limit < netst['default_limit_mbps']:
      be_ingress_limit = netst['default_limit_mbps']
    if be_egress_limit < netst['default_limit_mbps']:
      be_egress_limit = netst['default_limit_mbps']

    # enforce limits
    net.setEgressBwLimit(int(be_egress_limit))
    net.setIngressBwLimit(int(be_ingress_limit))

    net_cycle_data = {
        "cycle": cycle,
        "total_egress_bw": int(egress_total_mbps),
        "hp_egress_bw": int(egress_hp_mbps),
        "be_egress_bw": int(egress_be_mbps),
        "be_egress_limit": int(be_egress_limit),
        "total_ingress_bw": int(ingress_total_mbps),
        "hp_ingress_bw": int(ingress_hp_mbps),
        "be_ingress_bw": int(ingress_be_mbps),
        "be_ingress_limit": int(be_ingress_limit),
    }

    at = dt.now().strftime('%H:%M:%S')

    # loop
    if st.verbose:
      print "Net: Net controller cycle", cycle, "at", at
      print "Net:   Egress  BW: %.2f (Total) %.2f (HP), %.2f (BE), %.2f (BE alloc)" \
        %(egress_total_mbps, egress_hp_mbps, egress_be_mbps, be_egress_limit)
      print "Net:   Ingress BW: %.2f (Total) %.2f (HP), %.2f (BE), %.2f (BE alloc)" \
        %(ingress_total_mbps, ingress_hp_mbps, ingress_be_mbps, be_ingress_limit)

    if st.get_param('write_metrics', 'net_controller', False) is True:
      st.stats_writer.write(at, st.node.name, "net", net_cycle_data)

    cycle += 1
    time.sleep(period)
