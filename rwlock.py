"""
Simple reader writer lock based on the Python Cookbook

"""

__author__ = "Christos Kozyrakis"
__email__ = "christos@hyperpilot.io"
__copyright__ = "Copyright 2017, HyperPilot Inc"

import threading

class ReadWriteLock(object):
  """ A lock object that allows many readers but just one writer
  """
  def __init__(self):
    self._read_ready = threading.Condition(threading.Lock())
    self._readers = 0

  def acquire_read(self):
    self._read_ready.acquire()
    try:
      self._readers += 1
    finally:
      self._read_ready.release()

  def release_read(self):
    self._read_ready.acquire()
    try:
      self._readers -= 1
      if not self._readers:
        self._read_ready.notifyAll()
    finally:
      self._read_ready.release()

  def acquire_write(self):
    self._read_ready.acquire()
    while self._readers > 0:
      self._read_ready.wait()

  def release_write(self):
    self._read_ready.release()
