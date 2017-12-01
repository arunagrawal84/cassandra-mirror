from collections import namedtuple
from contextlib import contextmanager

from plumbum.path import LocalPath
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory
import os

import pkg_resources
import boto3
import json
import yaml

from plumbum.machines.local import LocalCommand
from plumbum.commands.processes import CommandNotFound
gof3r = LocalCommand(pkg_resources.resource_filename(__name__, "gof3r"))

s3 = boto3.resource('s3')

# despite being class-like, s3.Object is not a class
def S3Path(bucket_name, key):
    self = s3.Object(bucket_name, key)
    def _with_components(*components):
        new_key = '/'.join((self.key,) + components)
        return S3Path(self.bucket_name, new_key)

    def _read_utf8():
        return self.get()["Body"].read().decode("utf-8")

    def _descendants(start=None):
        args = dict(Prefix=key + '/')
        if start is not None:
            args['Marker'] = '{}/{}'.format(key, start)

        return s3.Bucket(bucket_name).objects.filter(**args)


    self.with_components = _with_components
    self.read_utf8 = _read_utf8
    self.descendants = _descendants

    return self

def reverse_format_nanoseconds(ns):
    return '{:016x}'.format((1 << 64) - int(ns))

# This is incorporated into the encryption context, representing the program that uploaded the
# file. The goal is to disambiguate the files produced by this utility from files produced by some
# other utility. It is called a continuity code because it will remain the same for the life of
# the project.
continuity_code = 'dcb4246e-f8ac-400e-b005-61c751a75134'

def _get_config_path():
    config_filename = os.getenv('CASSANDRA_CONF', '/etc/cassandra')
    return LocalPath(config_filename) / 'mirroring.yaml'

def _load_config_from_path(path):
    config_f = path.open()
    return yaml.safe_load(config_f)

class Locations(namedtuple('Locations', 'data_dir sstables_dir state_dir links_dir')):
    pass

def _make_locations(config):
    data_dir = LocalPath(config.get('data_dir', '/var/lib/cassandra'))
    state_dir = config.get('state_dir')
    state_dir = (
        LocalPath(state_dir) if state_dir is not None
        else data_dir / 'mirroring'
    )
    return Locations(data_dir, data_dir / 'data', state_dir,
	    state_dir / 'links')

def load_config():
    path = _get_config_path()
    config = _load_config_from_path(path)
    locs = _make_locations(config)
    return config, locs

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

def compute_top_prefix(config):
    prefix = config['s3']['prefix_format'].format(**config['context'])
    key = '/'.join((
        prefix,
        'v2',
        config['context']['identity']
    ))
    return S3Path(config['s3']['bucket'], key)

def serialize_context(o):
    return json.dumps(o, sort_keys=True).encode('ascii')
