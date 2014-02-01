#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json

from cache_interface import CacheInterface
from responses import ATM_Response


def store_local(local_path, content, format):
  """ Save a local copy of the file. """
  # Save to disk.
  with open(local_path, 'wb') as f:

    if format == 'json':
      f.write(json.dumps(content))

    elif format == "txt":
      return f.write(content)


def load_local(local_path, format):
  """ Read a local copy of a url. """

  if not os.path.exists(local_path):
    return None

  with open(local_path, 'rb') as f:
    if format == 'json':
      try:
        content = json.load(f)
      except ValueError:
        raise ValueError("JSON file is corrupted!")
      else:
        return content

    elif format == "txt":
      return f.read()


class LocalCacheInterface(CacheInterface):
  """
  An interface for caching files locally
  """

  @staticmethod
  def accepts_uri(uri):
    # This one seems to be the fallback
    return True

  def __init__(self, *args, **kwargs):
      super(LocalCacheInterface, self).__init__(*args, **kwargs)
      self.cache_dir = self.uri
      # If the cache directory does not exist, make one.
      if not os.path.isdir(self.cache_dir):
        os.makedirs(self.cache_dir)

  def get_cache(self, url):
    """ Wrap requests.get() """
    # create a filepath
    interval_string = self._gen_interval_string()
    filepath = self._url_to_filepath(url, interval_string)

    content = load_local(filepath, self.format)
    status_code = None
    # if it doesen't exist, fetch the url and cache it.
    if content is None:
      status_code, content = self._resp_from_request(url)
      store_local(filepath, content, self.format)

    return ATM_Response(
      content = content,
      url = url,
      is_s3= False,
      filepath = filepath,
      cache_dir = self.cache_dir,
      bucket_name = None,
      status_code = status_code,
      source = "cache" if status_code is None else "url",
      timestamp = int(interval_string) if interval_string else None
    )

  def withdraw(self, filepath):
    """ Retrieves a file from the cache"""
    return load_local(filepath, self.format)

  def liquidate(self):
    """ Retrieve all files from the cache. Returns a generator"""
    for filepath in self.statement():
      yield load_local(filepath, self.format)

  def default(self):
    """ Delete all files from the cache"""
    for filepath in self.statement():
      os.remove(filepath)

  def statement(self):
    """ List all files in the cache """
    return [os.path.join(self.cache_dir, f) for f in os.listdir(self.cache_dir)]

