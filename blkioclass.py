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

import math
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
    out, _ = self.cc.run_command('ls /sys/fs/cgroup/blkio/docker/')
    if 'No such file or directory' in out:
      raise Exception('Blkio not available: ' + out)


  def addBeCont(self, cont_id):
    """ Adds the long ID of a container to the list of BE containers throttled
    """
    if cont_id in self.cont_ids:
      raise Exception('Duplicate blkio throttling request %s' % cont_id)
    # check if blockio is active
    directory = '/sys/fs/cgroup/blkio/docker/' + cont_id
    out, _ = self.cc.run_command('ls ' + directory)
    if "No such file or directory" in out:
      raise Exception('Blkio not setup correctly for container: ' + cont_id)
    self.cont_ids.add(cont_id)


  def removeBeCont(self, cont_id):
    """ Removes the long ID of a container to the list of BE containers throttled
    """
    if cont_id not in self.cont_ids:
      raise Exception('Not existing container' % cont_id)
    self.cont_ids.remove(cont_id)


  def setIopsLimit(self, iops):
    # replace always work for tc filter

    if iops >= self.max_iops:
      raise Exception('Blkio limit ' + iops + ' is higher than max iops ' + self.max_iops)

    # heuristic: assuming N BE containers, allow each to BE job to use up to 1/sqrt(N) IOPS
    # a hierarchical cgroup would be better
    limit = (int)(iops/math.sqrt(len(self.cont_ids)))

    # set the limit for every container
    for cont in self.cont_ids:
      directory = '/sys/fs/cgroup/blkio/docker/' + cont
      cmd = self.block_dev + ' ' + limit
      # set read limit
      rfile = directory + 'blkio.throttle.read_iops_device'
      _, err = self.cc.run_command('echo ' + cmd + ' > ' + rfile)
      if err:
        raise Exception('Could not set blkio limit: ' + err)
      # set write limit
      wfile = directory + 'blkio.throttle.write_iops_device'
      _, err = self.cc.run_command('echo ' + cmd + ' > ' + wfile)
      if err:
        raise Exception('Could not set blkio limit: ' + err)
