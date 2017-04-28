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
  if st.verbose:
    print "Starting NetControl (%s, %s, %f, %f)" \
           % (st.params['iface_ext'], st.params['iface_cont'], st.params['max_bw_mbps'], st.params['link_bw_mbps'])
  net = netclass.NetClass(st.params['iface_ext'], st.params['iface_cont'], \
                          st.params['max_bw_mbps'], st.params['link_bw_mbps'], \
                          st.params['ctlloc'])
  period = st.params['net_period']
  cycle = 0
  # control loop
  while 1:

    if not st.enabled:
      print "BE Controller is disabled, skipping net control"
      time.sleep(period)
      continue

    if st.get_param('net_controller_disabled', False) is True:
      print "Net Controller is disabled"
      time.sleep(period)
      continue

    # get IP of all active BE containers
    active_be_ips = set()
    for _, cont in st.active_containers.items():
      if cont.wclass == 'BE':
        active_be_ips.add(cont.ipaddress)
    # track BW usage of new containers
    new_ips = active_be_ips.difference(net.cont_ips)
    for _ in new_ips:
      net.addIPtoFilter(_)
    old_ips = net.cont_ips.difference(active_be_ips)
    for _ in old_ips:
      net.removeIPfromFilter(_)

    # actual controller
    bw_usage = net.getBwStats()
    if 1 in bw_usage and 10 in bw_usage:
      total_bw = bw_usage[1]
      hp_bw = bw_usage[1] - bw_usage[10]
      if hp_bw < 0.0:
        hp_bw = 0.0
      be_bw = net.max_bw_mbps - hp_bw - max(0.05*net.max_bw_mbps, 0.10*hp_bw)
      if be_bw < 0.0:
        be_bw = 0.0
      net.setBwLimit(be_bw)
    elif st.verbose:
      print "Net stats lost, bw_usage: " + str(bw_usage)

    net_cycle_data = {
      "cycle": cycle,
      "total_bw": total_bw,
      "hp_bw": hp_bw,
      "be_bw": be_bw
    }

    at = dt.now().strftime('%H:%M:%S')

    # loop
    if st.verbose:
      print "Net controller cycle", cycle, "at", at,
      print " BW: %f (Total used) %f (HP used), %f (BE alloc)" %(total_bw, hp_bw, be_bw)

    st.stats_writer.write(at, st.node.name, "net", net_cycle_data)

    cycle += 1
    time.sleep(period)
