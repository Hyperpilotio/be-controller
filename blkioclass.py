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
import command_client as cc

class BlkioClass(object):
  """This class performs IO bandwidth isolation using blkio I/O throttling.

     Useful documents and examples:
      - blkio background
        https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/ch-Subsystems_and_Tunable_Parameters.html#blkio-throttling
      - blkio examples
        https://fritshoogland.wordpress.com/2012/12/15/throttling-io-with-linux/

  """
  def __init__(self, block_dev, max_iops, ctlloc):
    self.block_dev = block_dev
    self.max_iops = max_iops
    self.cont_ids = set()
    self.cc = cc.CommandClient(ctlloc)

    # check if blockio is active
    if not os.path.isdir('/sys/fs/cgroup/blkio/docker'):
      raise Exception('Blkio not configured for docker')


  def addBeCont(self, cont_id):
    """ Adds the long ID of a container to the list of BE containers throttled
    """
    if cont_id in self.cont_ids:
      raise Exception('Duplicate blkio throttling request %s' % cont_id)
    # check if blockio is active
    directory = '/sys/fs/cgroup/blkio/docker/' + cont_id
    if not os.path.isdir(directory):
      print 'Blkio not setup correctly for container: '+ cont_id
    self.cont_ids.add(cont_id)


  def removeBeCont(self, cont_id):
    """ Removes the long ID of a container from the list of BE containers throttled
    """
    if cont_id not in self.cont_ids:
      print 'Cannot remove from blkio non existing container %s' % cont_id
    else:
      self.cont_ids.remove(cont_id)


  def setIopsLimit(self, iops):
    """ Sets rad/write IOPS limit for BE containers
        Symmetric rd/write limit
    """
    if len(self.cont_ids) == 0:
      return

    if iops >= self.max_iops:
      raise Exception('Blkio limit ' + iops + ' is higher than max iops ' + self.max_iops)

    # heuristic: assuming N BE containers, allow each to BE job to use up to 1/N IOPS
    # a hierarchical cgroup would be better
    limit = (int)(iops/len(self.cont_ids))

    # set the limit for every container
    for cont in self.cont_ids:
      directory = '/sys/fs/cgroup/blkio/docker/' + cont
      rfile = directory + '/blkio.throttle.read_iops_device'
      wfile = directory + '/blkio.throttle.write_iops_device'
      if not os.path.isdir(directory) or \
         not os.path.isfile(rfile) or \
         not os.path.isfile(wfile):
        print 'Blkio not setup correctly for container: '+ cont
        continue
      # throttle string
      cmd = "\"" + self.block_dev + ' ' + str(limit) + "\""
      # read limit
      try:
        with open(rfile, "w") as _:
          _.write(cmd)
        with open(wfile, "w") as _:
          _.write(cmd)
      except EnvironmentError:
        print 'Blkio not setup correctly for container: '+ cont
        continue


  def getIopUsed(self, container):
    """ Find IOPS used for an active containers
    """
    pattern = self.block_dev + ' Total'

    # check if directory and stats file exists
    directory = '/sys/fs/cgroup/blkio/docker/' + str(container.docker_id)
    if not os.path.isdir(directory):
      print 'Blkio not configured for container %s' %(container.docker_id)
      return 0
    stats_file = directory + '/blkio.throttle.io_serviced'
    if not os.path.isfile(stats_file):
      print 'Blkio not configured for container %s' %(container.docker_id)
      return 0
    # read and parse iops
    with open(stats_file) as _:
      lines = _.readlines()
    for _ in lines:
      if _.startswith(pattern):
        iops = int(_.split()[2])
        break

    return iops
