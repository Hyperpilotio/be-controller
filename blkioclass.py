"""
Blkio utilies class

Current assumptions:
 - Blkio is enabled in cgroups
 - Single block device throttld for now
 - Symmetric read/write throttling for now

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

import os

class BlkioClass(object):
  """This class performs IO bandwidth isolation using blkio I/O throttling.

     Useful documents and examples:
      - blkio background
        https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/ch-Subsystems_and_Tunable_Parameters.html#blkio-throttling
      - blkio examples
        https://fritshoogland.wordpress.com/2012/12/15/throttling-io-with-linux/

  """
  def __init__(self, block_dev, max_rd_iops, max_wr_iops):
    self.block_dev = block_dev
    self.max_rd_iops = max_rd_iops
    self.max_wr_iops = max_wr_iops
    self.keys = set()

    # check if blockio is active
    if not os.path.isdir('/sys/fs/cgroup/blkio'):
      raise Exception('Blkio not configured for K8S')


  def addBeCont(self, cont_key):
    """ Adds the long ID of a container to the list of BE containers throttled
    """
    if cont_key in self.keys:
      raise Exception('Duplicate blkio throttling request %s' % cont_key)
    # check if blockio is active
    directory = '/sys/fs/cgroup/blkio/' + cont_key  
    if not os.path.isdir(directory):
      print 'Blkio:WARNING: Blkio not setup correctly for container (add): '+ cont_key
    self.keys.add(cont_key)


  def removeBeCont(self, cont_key):
    """ Removes the long ID of a container from the list of BE containers throttled
    """
    if cont_key not in self.keys:
      print 'Blkio:WARNING: Cannot remove from blkio non existing container %s' % cont_key
    else:
      self.keys.remove(cont_key)


  def setIopsLimit(self, riops, wiops):
    """ Sets rad/write IOPS limit for BE containers
    """
    if len(self.keys) == 0:
      return

    if riops > self.max_rd_iops:
      raise Exception('Blkio rd limit ' + riops + ' is higher than max iops ' + self.max_rd_iops)
    if wiops > self.max_wr_iops:
      raise Exception('Blkio wr limit ' + riops + ' is higher than max iops ' + self.max_wr_iops)

    # heuristic: assuming N BE containers, allow each to BE job to use up to 1/N IOPS
    # a hierarchical cgroup would be better
    rlimit = (int)(riops/len(self.keys))
    wlimit = (int)(wiops/len(self.keys))

    # set the limit for every container
    for cont in self.keys:
      directory = '/sys/fs/cgroup/blkio/' + cont
      rfile = directory + '/blkio.throttle.read_iops_device'
      wfile = directory + '/blkio.throttle.write_iops_device'
      if not os.path.isdir(directory) or \
         not os.path.isfile(rfile) or \
         not os.path.isfile(wfile):
        print 'Blkio:WARNING: Blkio not setup correctly for container (limit):'+ cont
        continue
      # throttle string
      rcmd = self.block_dev + ' ' + str(rlimit)
      wcmd = self.block_dev + ' ' + str(wlimit)
      # read limit
      try:
        with open(rfile, "w") as _:
          _.write(rcmd)
        with open(wfile, "w") as _:
          _.write(wcmd)
      except EnvironmentError as e:
        print 'Blkio:WARNING: cannot not setup correctly for container (limit): '+ e
        continue


  def getIopUsed(self, cont_key):
    """ Find IOPS used for an active container
    """
    rpattern = self.block_dev + ' Read'
    wpattern = self.block_dev + ' Write'

    # check if directory and stats file exists
    directory = '/sys/fs/cgroup/blkio/' + cont_key
    if not os.path.isdir(directory):
      print 'Blkio:WARNING: Blkio not configured for container %s' %(cont_key)
      return 0, 0
    stats_file = directory + '/blkio.throttle.io_serviced'
    if not os.path.isfile(stats_file):
      print 'Blkio:WARNING: Blkio not configured for container %s' %(cont_key)
      return 0, 0
    # read and parse iops
    with open(stats_file) as _:
      lines = _.readlines()
    riop = 0
    wiop = 0
    for _ in lines:
      if _.startswith(rpattern):
        riop = int(_.split()[2])
      if _.startswith(wpattern):
        wiop = int(_.split()[2])
      if riop and wiop:
        break

    return riop, wiop

  def clearIopsLimit(self):
    """ Clears rad/write IOPS limit for BE containers
    """
    # set the limit for every container
    for cont in self.keys:
      directory = '/sys/fs/cgroup/blkio/' + cont
      rfile = directory + '/blkio.throttle.read_iops_device'
      wfile = directory + '/blkio.throttle.write_iops_device'
      if not os.path.isdir(directory) or \
         not os.path.isfile(rfile) or \
         not os.path.isfile(wfile):
        print 'Blkio:WARNING: Blkio not setup correctly for container (limit): '+ cont
        continue
      # throttle string
      cmd = ''
      try:
        with open(rfile, "w") as _:
          _.write(cmd)
        with open(wfile, "w") as _:
          _.write(cmd)
      except EnvironmentError as e:
        print 'Blkio:WARNING: cannot not clear correctly for container (limit): '+ e
        continue
