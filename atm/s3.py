#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import boto
import boto.s3
from boto.s3.key import Key
import json
from urlparse import urlparse

from cache_interface import CacheInterface
from responses import ATM_Response

def is_s3_uri(uri):
  """Return True if *uri* can be parsed into an S3 URI, False otherwise."""
  try:
    parse_s3_uri(uri)
    return True
  except ValueError:
    return False

def parse_s3_uri(uri):
  """Parse an S3 URI into (bucket, key)

  >>> parse_s3_uri('s3://walrus/tmp/')
  ('walrus', 'tmp/')

  If ``uri`` is not an S3 URI, raise a ValueError
  """
  if not uri.endswith('/'):
    uri += '/'

  components = urlparse(uri)
  if (components.scheme not in ('s3', 's3n')
          or '/' not in components.path):
    raise ValueError('Invalid S3 URI: %s' % uri)

  return components.netloc, components.path[1:]

class S3(object):
  """ A class for connecting to a s3 bucket and uploading/downloading files"""
  def __init__(self, s3_uri):
      self.bucket_name, self.cache_dir = parse_s3_uri(s3_uri)
      self.bucket = self._connect_to_bucket(self.bucket_name)

  def _connect_to_bucket(self, bucket_name):
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    for i in conn.get_all_buckets():
      if bucket_name == i.name:
          return i

  def upload(self, filepath, data, format):
    k = Key(self.bucket)
    k.key = filepath
    if format == "txt":
      k.set_contents_from_string(data)
    elif format == "json":
      k.set_contents_from_string(json.dumps(data))

  def download(self, filepath, format):
    k = Key(self.bucket)
    k.key = filepath
    if k.exists():
      if format == "txt":
        return k.get_contents_as_string()
      elif format == "json":
        return json.loads(k.get_contents_as_string())
    else:
        return None

  def delete(self, filepath):
    k = Key(self.bucket)
    k.key = filepath
    self.bucket.delete_key(k)


class S3CacheInterface(CacheInterface):
  """
  A class for intelligently caching / retrieving data fetched from urls
  """

  @staticmethod
  def accepts_uri(uri):
    if not uri.endswith('/'):
      uri += '/'

    components = urlparse(uri)
    if (components.scheme not in ('s3', 's3n')
            or '/' not in components.path):
      return False

    return True

  def __init__(self, *args, **kwargs):
    super(S3CacheInterface, self).__init__(*args, **kwargs)
    self.is_s3 = True
    self.s3 = S3(self.uri)
    self.cache_dir = self.s3.cache_dir
    self.bucket_name = self.s3.bucket_name

  def get_cache(self, url):
    """ Wrap requests.get() """
    # create a filepath
    interval_string = self._gen_interval_string()
    filepath = self._url_to_filepath(url, interval_string)
    content = self.s3.download(filepath, self.format)

    status_code = None
    # if it doesen't exist, fetch the url and cache it.
    if content is None:
      status_code, content = self._resp_from_request(url)
      self.s3.upload(filepath, content, self.format)

    return ATM_Response(
      content = content,
      url = url,
      filepath = filepath,
      cache_dir = self.cache_dir,
      bucket_name = self.bucket_name,
      is_s3 = self.is_s3,
      status_code = status_code,
      source = "cache" if status_code is None else "url",
      timestamp = int(interval_string) if interval_string else None
    )

  def withdraw(self, filepath):
    """ Retrieves a file from the cache"""
    return self.s3.download(filepath, self.format)

  def liquidate(self):
    """ Retrieve all files from the cache. Returns a generator"""
    for filepath in self.statement():
      yield self.s3.download(filepath, self.format)

  def default(self):
    """ Delete all files from the cache"""
    for filepath in self.statement():
      self.s3.delete(filepath)


  def statement(self):
    """ List all files in the cache """
    return [k.key for k in self.s3.bucket.list(self.cache_dir)]


  def transaction(self, url, timestamp = None):
    """ get the filepath for the contents of a url in the cache"""
    if timestamp:
      timestamp = str(self._round_timestamp_to_interval(timestamp))

    return self._url_to_filepath(url, timestamp)
