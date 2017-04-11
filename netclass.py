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

import subprocess
import re
import time
import datetime as dt
import command_client as cc

class NetClass(object):
  """This class performs network bandwidth isolation using HTB qdisc and ipfilters.

     Useful documents and examples:
      - Creating multiple htb service classes:
        http://luxik.cdi.cz/~devik/qos/htb/manual/userg.htm
      - Classifying packets with filters
        http://lartc.org/howto/lartc.qdisc.filters.html
      - Common iptables commands
        http://www.thegeekstuff.com/2011/06/iptables-rules-examples
  """
  def __init__(self, iface_ext, iface_cont, max_bw_mbps, link_bw_mbps, ctlloc):
    self.iface_ext = iface_ext
    self.iface_cont = iface_cont
    self.max_bw_mbps = max_bw_mbps
    self.link_bw_mbps = link_bw_mbps
    self.cont_ips = set()
    self.cc = cc.CommandClient(ctlloc)
    self.mark = 6

    # reset IP tables
    out, err = self.cc.run_command('iptables -t mangle -F')
    if err:
      raise Exception('Could not reset iptables: ' + err)

    # make sure HTB is in a reasonable state to begin with
    self.cc.run_command('tc qdisc del dev %s root' % self.iface_ext)

    # replace root qdisc with HTB
    # need to disable/enable HTB to get the stats working

    success = self.cc.run_commands([
      'tc qdisc add dev %s root handle 1: htb default 1' % self.iface_ext,
      'echo 1 > /sys/module/sch_htb/parameters/htb_rate_est',
      'tc qdisc del dev %s root' % self.iface_ext,
      'tc qdisc add dev %s root handle 1: htb default 1' % self.iface_ext,
      'tc class add dev %s parent 1: classid 1:1 htb rate %dmbit ceil %dmbit' \
                               % (self.iface_ext, self.link_bw_mbps, self.link_bw_mbps),
      'tc class add dev %s parent 1: classid 1:10 htb rate %dmbit ceil %dmbit' \
                               % (self.iface_ext, self.max_bw_mbps, self.max_bw_mbps),
      'tc filter add dev %s parent 1: protocol all prio 10 handle %d fw flowid 1:10' \
                               % (self.iface_ext, self.mark)
      ])

    if not success:
      raise Exception('Could not setup htb qdisc')


  def addIPtoFilter(self, cont_ip):
    """ Adds the IP of a container to the IPtables filter
    """
    if cont_ip in self.cont_ips:
      raise Exception('Duplicate filter for IP %s' % cont_ip)
    self.cont_ips.add(cont_ip)

    out, err = self.cc.run_command('iptables -t mangle -A PREROUTING -i %s -s %s -j MARK --set-mark %d' \
                               % (self.iface_cont, cont_ip, self.mark))
    if err:
      raise Exception('Could not add iptable filter for %s: %s' % (cont_ip, err))


  def removeIPfromFilter(self, cont_ip):
    """ Adds the IP of a container to the IPtables filter
    """
    if cont_ip not in self.cont_ips:
      raise Exception('Not existing filter for %s' % cont_ip)
    self.cont_ips.remove(cont_ip)

    out, err = self.cc.run_command('iptables -t mangle -D PREROUTING -i %s -s %s -j MARK --set-mark %d' \
                               % (self.iface_cont, cont_ip, self.mark))
    if err:
      raise Exception('Could not add iptable filter for %s: %s' % (cont_ip, err))


  def setBwLimit(self, bw_mbps):
    # replace always work for tc filter

    out, err = self.cc.run_command('tc class replace dev %s parent 1: classid 1:10 htb rate %dmbit ceil %dmbit' \
                               % (self.iface_ext, bw_mbps, bw_mbps))
    if err:
      raise Exception('Could not change htb class rate: ' + err)


  def getBwStatsBlocking(self):
    """Performs a blocking read to get one second averaged bandwidth statistics
    """
    # helper method to get stats from tc
    def read_tc_stats():
      text, err = self.cc.run_command('tc -s class show dev %s' % self.iface_ext)
      if err:
        raise Exception("Unable to get tc stats for %s: %s" % (self.iface_ext, err))

      """Example format to parse. For some reason rate and pps are always 0...
      class htb 1:1 root prio 0 rate 10000Mbit ceil 10000Mbit burst 0b cburst 0b
       Sent 108 bytes 2 pkt (dropped 0, overlimits 0 requeues 0)
       rate 0bit 0pps backlog 0b 0p requeues 0
       lended: 2 borrowed: 0 giants: 0
       tokens: 14 ctokens: 14

      class htb 1:2 root prio 0 rate 1000Mbit ceil 1000Mbit burst 1375b cburst 1375b
       Sent 1253014380 bytes 827622 pkt (dropped 0, overlimits 0 requeues 0)
       rate 0bit 0pps backlog 0b 0p requeues 0
       lended: 18460 borrowed: 0 giants: 0
       tokens: -47 ctokens: -47
      """
      results = {}
      for _ in re.finditer('class htb 1:(?P<cls>\d+).*?\n.*?Sent (?P<bytes>\d+) bytes', text, re.DOTALL):
        cls = int(_.group('cls'))
        bytes = int(_.group('bytes'))
        results[cls] = 8.0*bytes/1000/1000 # convert to mbps

      return results

    # read stats from tc
    starting_value = read_tc_stats()
    starting_time = dt.datetime.now()
    time.sleep(1)
    ending_value = read_tc_stats()
    ending_time = dt.datetime.now()

    # take the difference to find the average
    elapsed_time = (ending_time - starting_time).total_seconds()
    results = {}
    for _ in dict.iterkeys():
      results[_] = float(ending_value[_] - starting_value[_]/elapsed_time)
    return results


  def getBwStats(self):
    """Performs a non-blocking read averaged bandwidth statistics
    """
    text, err = self.cc.run_command('tc -s class show dev %s' % self.iface_ext)
    if err:
      raise Exception("Unable to get Bw stats for %s: %s" % (self.iface_ext, err))

    """
    Example format to parse. Rate and pps are assumed to be valid
    class htb 1:1 root prio 0 rate 10Gbit ceil 10Gbit burst 0b cburst 0b
      Sent 3552621 bytes 22143 pkt (dropped 0, overlimits 0 requeues 0)
      rate 59400bit 50pps backlog 0b 0p requeues 0
      lended: 22143 borrowed: 0 giants: 0
      tokens: 13 ctokens: 13

    class htb 1:2 root prio 0 rate 1000Mbit ceil 1000Mbit burst 1375b cburst 1375b
      Sent 1253014380 bytes 827622 pkt (dropped 0, overlimits 0 requeues 0)
      rate 59400bit 50pps backlog 0b 0p requeues 0
      lended: 18460 borrowed: 0 giants: 0
      tokens: -47 ctokens: -47
    """
    results = {}
    for _ in re.finditer('class htb 1:(?P<cls>\d+).*?\n.*?rate (?P<rate>\d+)bit', text, re.DOTALL):
      cls = int(_.group('cls'))
      rate = int(_.group('rate'))
      results[cls] = float(rate / (1000000.0)) # convert to mbps
    return results
