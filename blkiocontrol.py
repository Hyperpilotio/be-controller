"""
Blkio controller

Current assumptions:
 - Blkio is enabled in cgroups
 - Single block device throttld for now
 - Symmetric read/write throttling for now

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

# standard
import time
import datetime as dt

# hyperpilot imports
import settings as st
import blkioclass as blkioclass

def BlkioControll():
  """ Blkio controller
  """
  # initialize controller
  if st.verbose:
    print "Starting BlkioControl (%s, %d, %s)" \
           % (st.params['block_dev'], st.params['max_iops'], st.params['ctlloc'])
  blkio = blkioclass.BlkioClass(st.params['block_dev'], st.params['max_iops'], \
                          st.params['ctlloc'])
  period = st.params['blkio_period']
  cycle = 0
  start_iop_stats = {}
  start_time = dt.datetime.now()

  # control loop
  while 1:

    if not st.enabled:
      print "BE Controller is disabled, skipping blkio control"
      time.sleep(period)
      continue

    if st.get_param('blkio_controller_disabled', False) is True:
      print "Blkio Controller is disabled"
      time.sleep(period)
      continue

    # get IOPS usage statistics
    # and get IP of all active BE containers
    active_be_ids = set()
    end_iop_stats = blkio.getIopUsed(st.active_containers)
    end_time = dt.datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    be_iop = 0
    hp_iop = 0

    for _, cont in st.active_containers.items():
      if cont.docker_id in start_iop_stats:
        start = start_iop_stats[cont.docker_id]
      else:
        start = 0
      if cont.docker_id in end_iop_stats:
        end = end_iop_stats[cont.docker_id]
      else:
        continue
      iop = end - start
      if cont.wclass == 'BE':
        be_iop += iop
        active_be_ids.add(cont.docker_id)
      else:
        hp_iop += iop

    hp_iops = int(hp_iop/elapsed_time)
    be_iops = int(be_iop/elapsed_time)
    total_iops = hp_iops + be_iops
    start_time = end_time
    start_iop_stats = end_iop_stats

    # track BW usage of new containers
    new_ids = active_be_ids.difference(blkio.cont_ids)
    for _ in new_ids:
      blkio.addBeCont(_)
    old_ids = blkio.cont_ids.difference(active_be_ids)
    for _ in old_ids:
      blkio.removeBeCont(_)

    # actual controller
    be_limit = blkio.max_iops - hp_iops - max(0.05*blkio.max_iops, 0.10*hp_iops)
    if be_limit < 0.0:
      be_limit = 0.0
    blkio.setIopsLimit(be_limit)

    blkio_cycle_data = {
        "cycle": cycle,
        "max_iops": blkio.max_iops,
        "total_iops": total_iops,
        "hp_iops": hp_iops,
        "be_iops": be_iops,
        "be_limit": be_limit
    }

    at = dt.datetime.now().strftime('%H:%M:%S')

    # loop
    if st.verbose:
      print "Blkio controller cycle", cycle, "at", at,
      print " IOPS: %d (Total used) %d (HP used), %d (BE alloc)" %(total_iops, hp_iops, be_iops)

    if st.get_param('write_metrics', False) is True:
      st.stats_writer.write(at, st.node.name, "blkio", blkio_cycle_data)

    cycle += 1
    time.sleep(period)
