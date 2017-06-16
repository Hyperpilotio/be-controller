"""
Network utilies class

Current assumptions:
- Manual entry of max throughput
- Each BE container has their own IP address
- Not managing bursts for now
- Using tc (htb) + iptables for outgoing traffic
- Using tc (cbq) for incoming traffic

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

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
      - http://lartc.org/howto/lartc.ratelimit.single.html
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
    _, err = self.cc.run_command('iptables -t mangle -F')
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
                        % (self.iface_ext, self.mark)])
    if not success:
      raise Exception('Could not setup htb qdisc')

    # make sure CBQ is in a reasonable state to begin with
    self.cc.run_command('tc qdisc del dev %s root' % self.iface_cont)
    # replace root qdisc with CBQ
    success = self.cc.run_commands([
        'tc qdisc replace dev %s root handle 2: cbq avpkt 1000 bandwidth %dmbit' \
            % (self.iface_cont, self.link_bw_mbps),
	    'tc class replace dev %s parent 2: classid 2:1 cbq rate %dmbit allot 1500 prio 5 bounded isolated' \
            % (self.iface_cont, self.ingress_limit_mbps)])
    if not success:
      raise Exception('Could not setup cbq qdisc')


  def addIPtoFilter(self, cont_ip):
    """ Adds the IP of a container to the IPtables filter
    """
    if cont_ip in self.cont_ips:
      raise Exception('Duplicate filter for IP %s' % cont_ip)
    self.cont_ips.add(cont_ip)

    # egress
    _, err = self.cc.run_command('iptables -t mangle -A PREROUTING -i %s -s %s -j MARK --set-mark %d' \
                               % (self.iface_cont, cont_ip, self.mark))
    if err:
      raise Exception('Could not add iptable filter for %s: %s' % (cont_ip, err))
    # ingress
    _, err = self.cc.run_command('tc filter add dev %s parent 2: protocol ip prio 16 u32 match ip dst %s flowid 2:1' \
                               % (self.iface_cont, cont_ip))
    if err:
      raise Exception('Could not add cbq filter for %s: %s' % (cont_ip, err))


  def removeIPfromFilter(self, cont_ip):
    """ Adds the IP of a container to the IPtables filter
    """
    if cont_ip not in self.cont_ips:
      raise Exception('Not existing filter for %s' % cont_ip)
    self.cont_ips.remove(cont_ip)

    #egress
    _, err = self.cc.run_command('iptables -t mangle -D PREROUTING -i %s -s %s -j MARK --set-mark %d' \
                               % (self.iface_cont, cont_ip, self.mark))
    if err:
      raise Exception('Could not remove iptable filter for %s: %s' % (cont_ip, err))
    #ingress
    _, err = self.cc.run_command('tc filter del dev %s prio 16' % (self.iface_cont))
    if err:
      raise Exception('Could not remove cbq filter for %s: %s' % (cont_ip, err))


  def setEgressBwLimit(self, bw_mbps):
    # replace always work for tc filter
    _, err = self.cc.run_command('tc class replace dev %s parent 1: classid 1:10 htb rate %dmbit ceil %dmbit' \
                               % (self.iface_ext, bw_mbps, bw_mbps))
    if err:
      raise Exception('Could not change htb class rate: ' + err)

  def setIngressBwLimit(self, bw_mbps):
    # ingress
    _, err = self.cc.run_command('tc class replace dev %s parent 2: classid 2:1 cbq rate %dmbit \
	                                 allot 1500 prio 5 bounded isolated ' \
                               % (self.iface_cont, bw_mbps))
    if err:
      raise Exception('Could not change cbq class rate: ' + err)


  @staticmethod
  def parseBwStats(text):
    results = {}
    for _ in re.finditer('class htb 1:(?P<cls>\d+).*?\n.*?rate (?P<rate>\d+[K|M]?)bit', text, re.DOTALL):
      cls = int(_.group('cls'))
      rate = _.group('rate')
      if rate[-1] == "K":
        rate = rate[:-1] + "000"
      elif rate[-1] == "M":
        rate = rate[:-1] + "000000"

      results[cls] = float(int(rate) / (1000000.0)) # convert to mbps
    return results


  def getEgressBwStats(self):
    """Performs a non-blocking read for averaged bandwidth statistics
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
    return NetClass.parseBwStats(text)


  def getIngressBytesStats(self):
    """Performs a non blocking read to bytes statistics for ingress
    """
    # helper method to get stats from tc
    def read_tc_stats():
      text, err = self.cc.run_command('tc -s class show dev %s' % self.iface_cont)
      if err:
        raise Exception("Unable to get tc stats for %s: %s" % (self.iface_cont, err))

      """Example format to parse.
         class cbq 2: root rate 1Gbit (bounded,isolated) prio no-transmit
          Sent 71472933340 bytes 11208948 pkt (dropped 0, overlimits 0 requeues 0) 
          backlog 0b 0p requeues 0 
           borrowed 0 overactions 0 avgidle 125 undertime 0
         class cbq 2:1 parent 2: rate 50Mbit (bounded,isolated) prio 5
          Sent 28586400957 bytes 7576847 pkt (dropped 2309, overlimits 15156382 requeues 0) 
          backlog 0b 0p requeues 0 
           borrowed 0 overactions 6378134 avgidle -5382 undertime 1.22595e+09      """
      be_bytes = 0
      total_bytes = 0
      for _ in re.finditer('class cbq 2:(?P<cls>\d+).*?\n.*?Sent (?P<bytes>\d+) bytes', text, re.DOTALL):
        if _.group('cls') == '10':
          be_bytes = int(_.group('bytes'))
        else:
          total_bytes = int(_.group('bytes'))
      # convert to mbit before returning
      return (8.0*be_bytes/1000/1000, 8.0*total_bytes/1000/1000)
