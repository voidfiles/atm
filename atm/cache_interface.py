#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import os
from hashlib import sha1
from datetime import datetime


class CacheInterface(object):
  """
  An base class for declaring the cache interface
  """
  def __init__(self, uri, format='txt', interval=None):
    self.uri = uri
    self.format = format.lower()
    self.interval = interval

  def withdraw(self, filepath):
    """ Retrieves a file from the cache"""
    pass

  def liquidate(self):
    """ Retrieve all files from the cache. Returns a generator"""
    pass

  def default(self):
    """ Delete all files from the cache"""
    pass

  def statement(self):
    """ List all files in the cache """
    pass

  def transaction(self, url, timestamp = None):
    """ get the filepath for the contents of a url in the cache"""
    if timestamp:
      timestamp = str(self._round_timestamp_to_interval(timestamp))

    return self._url_to_filepath(url, timestamp)

  def _resp_from_request(self, url):
    response = requests.get(url)

    status_code = response.status_code

    if response.status_code != 200:
      return status_code, None

    # fetch
    if self.format == "json":
      return status_code, response.json()

    elif self.format == "txt":
      return status_code, response.content

  def _url_to_filepath(self, url, interval_string):
    """ Make a url into a file name, using SHA1 hashes. """

    # use a sha1 hash to convert the url into a unique filepath
    hash_file = "%s.%s" % (sha1(url).hexdigest(), self.format)
    if interval_string:
      hash_file = "%s-%s" % (interval_string, hash_file)

    return os.path.join(self.cache_dir, hash_file)

  def _gen_interval_string(self):
    """Generate a timestamp string that will be used to update the cache at a set interval"""
    now = int(datetime.now().strftime("%s"))
    if self.interval:
      return self._round_timestamp_to_interval(now)
    else:
      return None

  def _round_timestamp_to_interval(self, ts):
    """Generate a timestamp string that will be used to update the cache at a set interval"""
    return int(ts) - int(ts % int(self.interval))
