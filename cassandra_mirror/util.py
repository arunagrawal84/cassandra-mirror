from contextlib import contextmanager

from plumbum.path import LocalPath
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory
import os

from cachetools.func import ttl_cache
import boto3
import json
import yaml

# This is incorporated into the encryption context, representing the program that uploaded the
# file. The goal is to disambiguate the files produced by this utility from files produced by some
# other utility. It is called a continuity code because it will remain the same for the life of
# the project.
continuity_code = 'dcb4246e-f8ac-400e-b005-61c751a75134'

def load_config(path):
    config_f = open(path, 'r')
    return yaml.safe_load(config_f)

_multipart_chunksize = 20 * 1024 * 1024

def timed_touch(path, mtime):
    """Sets the time on `path` to `mtime`, atomically. If `path` does not
    exist, it is created. If we create `path`, we guarantee that it will either
    exist with the correct mtime or not exist at all.
    """
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

def moving_temporary_file(destination):
    ret = NamedTemporaryFile(
        prefix=destination.name + '.',
        dir=destination.up(),
    )
    def finalize():
        os.link(ret.name, str(destination))

    ret.finalize = finalize
    return ret

class MovingTemporaryDirectory(TemporaryDirectory):
    """Represents a directory that may have partial or incorrect data in it.

    The idea is that a process will create the directory, fill it with data,
    and then move it to its final location. Up until the final move, the
    directory is "temporary": it will be deleted on exit from its
    contextmanager. Once the final move is done, the directory is permanent.
    """

    def __init__(self, destination):
        self.destination = str(destination)
        super().__init__(
            prefix=destination.name + '.',
            dir=destination.up(),
        )

    def __enter__(self):
        return self

    @property
    def path(self):
        return LocalPath(self.name)

    def finalize(self):
        # plumbum's rename is based on shutil.move, and will nest directories
        # So you'll get data/data.XXXXXX, if the destination data directory
        # already existed.
        os.rename(self.name, self.destination)
        try:
            self.cleanup()
        except FileNotFoundError:
            # We expect to hit this in 100% of cases, because we renamed the
            # directory.
            # Making this call sets the flag indicating the file has deleted,
            # so that __exit__ does not complain
            pass


def compute_top_prefix(config, identity):
    prefix = config['s3']['prefix_format'].format(**config['context'])
    return '/'.join((
        prefix,
        'v1',
        identity
    ))

def serialize_context(o):
    return json.dumps(o, sort_keys=True).encode('ascii')