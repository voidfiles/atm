

class ATM_Response(object):
  """a return object for ATM.get_cache"""
  def __init__(self, content,  url, filepath, cache_dir, bucket_name, is_s3, status_code, source, timestamp):
    self.content = content
    self.url = url
    self.filepath = filepath
    self.cache_dir = cache_dir
    self.bucket_name =  bucket_name
    self.is_s3 = is_s3
    self.status_code = status_code
    self.source = source
    self.timestamp = timestamp
