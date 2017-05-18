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
  netst = st.params['blkio_controller']
  if st.verbose:
    print "Starting BlkioControl (%s, %d, %s)" \
           % (netst['block_dev'], netst['max_rd_iops'], netst['max_wr_iops'])
  blkio = blkioclass.BlkioClass(netst['block_dev'], netst['max_rd_iops'], netst['max_wr_iops'])
  period = netst['blkio_period']
  cycle = 0
  start_iop_stats = {}
  start_time = dt.datetime.now()

  # control loop
  while 1:

    if not st.enabled:
      print "BE Controller is disabled, skipping blkio control"
      time.sleep(period)
      continue

    if st.get_param('disabled', 'blkio_controller', False) is True:
      print "Blkio Controller is disabled"
      time.sleep(period)
      continue

    #Get IDS of all active containers
    active_ids = set()
    active_be_ids = set()
    st.active.lock.acquire_read()
    for _, pod in st.active.pods.items():
      if pod.qosclass == 'guaranteed':
        root = 'kubepods/' + 'pod' + pod.uid + '/'
      else:
        root = 'kubepods/' + pod.qosclass.lower() + '/pod' + pod.uid + '/'
      for cont in pod.container_ids:
        key = root + cont
        active_ids.add(key)
        if pod.wclass == 'BE':
          active_be_ids.add(key)
    st.active.lock.release_read()

    # get IOPS usage statistics
    end_iop_stats = {}
    be_riop = 0
    be_wiop = 0
    hp_riop = 0
    hp_wiop = 0
    for key in active_ids:
      rend, wend = blkio.getIopUsed(key)
      end_iop_stats[key] = [rend, wend]
      if key in start_iop_stats:
        [rstart, wstart] = start_iop_stats[key]
      else:
        rstart = 0
        wstart = 0
      riop = rend - rstart
      wiop = wend - wstart
      if key in active_be_ids:
        be_riop += riop
        be_wiop += wiop
      else:
        hp_riop += riop
        hp_wiop += wiop

    end_time = dt.datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    hp_riops = int(hp_riop/elapsed_time)
    be_riops = int(be_riop/elapsed_time)
    hp_wiops = int(hp_wiop/elapsed_time)
    be_wiops = int(be_wiop/elapsed_time)
    total_riops = hp_riops + be_riops
    total_wiops = hp_wiops + be_wiops

    # reset stats for next cycle
    start_time = end_time
    start_iop_stats = end_iop_stats

    # track BW usage of new containers
    new_ids = active_be_ids.difference(blkio.keys)
    for _ in new_ids:
      blkio.addBeCont(_)
    old_ids = blkio.keys.difference(active_be_ids)
    for _ in old_ids:
      blkio.removeBeCont(_)

    # actual controller
    be_rlimit = blkio.max_rd_iops - hp_riops - max(0.05*blkio.max_rd_iops, 0.10*hp_riops)
    be_wlimit = blkio.max_wr_iops - hp_wiops - max(0.05*blkio.max_wr_iops, 0.10*hp_wiops)
    if be_rlimit < 0.0:
      be_rlimit = 0.0
    if be_wlimit < 0.0:
      be_wlimit = 0.0
    blkio.setIopsLimit(be_rlimit, be_wlimit)

    blkio_cycle_data = {
        "cycle": cycle,
        "max_rd_iops": blkio.max_rd_iops,
        "max_wr_iops": blkio.max_wr_iops,
        "total_riops": total_riops,
        "total_wiops": total_wiops,
        "hp_rd_iops": hp_riops,
        "hp_wr_iops": hp_wiops,
        "be_rd_iops": be_riops,
        "be_wr_iops": be_wiops,
        "be_rd_limit": be_rlimit,
        "be_wr_limit": be_wlimit
    }

    at = dt.datetime.now().strftime('%H:%M:%S')

    # loop
    if st.verbose:
      print "Blkio controller cycle", cycle, "at", at,
      print " IOPS: %d (Total used) %d (HP used), %d (BE alloc)" \
            %(total_riops + total_wiops, hp_riops + hp_wiops, be_riops + be_wiops)

    if st.get_param('write_metrics', 'blkio_controller', False) is True:
      st.stats_writer.write(at, st.node.name, "blkio", blkio_cycle_data)

    cycle += 1
    time.sleep(period)
