from contextlib import contextmanager
from plumbum.path import LocalPath
from tempfile import NamedTemporaryFile
import os

from cachetools.func import ttl_cache
import boto3
import yaml

def load_config(path):
    config_f = open(path, 'r')
    return yaml.safe_load(config_f)

_multipart_chunksize = 20 * 1024 * 1024

@contextmanager
def s3_upload_future(infile, client, bucket, key, extra_args=None):
    config = TransferConfig(multipart_chunksize=_multipart_chunksize)
    with TransferManager(client, config) as manager:
        future = manager.upload(
            fileobj=infile,
            bucket=bucket,
            key=key,
            extra_args=extra_args
        )
        yield future

@contextmanager
def s3_download_future(outfile, client, bucket, key):
    config = TransferConfig(multipart_chunksize=_multipart_chunksize)
    with TransferManager(client, config) as manager:
        future = manager.download(
            fileobj=outfile,
            bucket=bucket,
            key=key,
            extra_args=extra_args
        )
        yield future

def timed_touch(path, mtime):
    try:
        os.utime(path, ns=(mtime, mtime))
    except FileNotFoundError:
        with NamedTemporaryFile(dir=path.dirname) as f:
            os.utime(f.fileno(), ns=(mtime, mtime))
            os.link(f.name, str(path))       

@ttl_cache(ttl=1800)
def get_creds_dict():
    creds = boto3.Session().get_credentials()
    creds_dict = dict(
        AWS_ACCESS_KEY_ID=creds.access_key,
        AWS_SECRET_ACCESS_KEY=creds.secret_key,
    )
    if creds.token is not None:
        creds_dict['AWS_SECURITY_TOKEN'] = creds.token
    return creds_dict

def compose(f):
    def wrapper(g):
        @functools.wraps(g)
        def wrapped(*args, **kwargs):
            return f(g(*args, **kwargs))
        return wrapped
    return wrapper


