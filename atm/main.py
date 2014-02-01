#!/usr/bin/env python
# -*- coding: utf-8 -*-
from s3 import S3CacheInterface

from local import LocalCacheInterface

interfaces = (S3CacheInterface, LocalCacheInterface)


class ATM_Error(Exception):
  pass


class ATM(object):
  """
  A class for intelligently caching / retrieving data fetched from urls
  """
  def __init__(self, *args, **kwargs):
    # This could be done cleaner if we could change the interface.
    # Something like ATM.from_s3_url() and such, for for now I will just
    # leave this as is
    return None

  def __new__(cls, cache_url, *args, **kwargs):
    print '%s, %s, %s' % (cache_url, args, kwargs)
    for interface in interfaces:
      if interface.accepts_uri(cache_url):
        print 'Found interface'
        return interface(cache_url, *args, **kwargs)
      else:
        print 'No one wants it'
