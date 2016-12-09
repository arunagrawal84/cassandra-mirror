from collections import namedtuple
from contextlib import contextmanager
import argparse
import functools
import os
import time

from plumbum import BG
from plumbum import LocalPath
from boto3.s3.transfer import TransferConfig
from boto3.s3.transfer import TransferManager
import boto3
import keypipe
import yaml

from plumbum.cmd import dd
from plumbum.cmd import tar
from plumbum.cmd import lz4

def apply(f):
    def wrap(g):
        @functools.wraps(g)
        def wrapped(*args, **kwargs):
            return f(g(*args, **kwargs))
        return wrapped
    return wrap

def is_sstable_toc(path):
    split = path.basename.split('-', 3)
    is_tmp = len(split) >= 2 and split in ('tmp', 'tmplink')
    return (path.endswith('-TOC.txt')
        and not is_tmp
        and path.isfile() # not a link or a directory
    )

# A recursive directory walker, with a goal of returning files
# matching the predicate `test`
def find(path, depth, test):
    if depth == 0:
        if test(path):
            yield path
        return

    for name in path.list():
        yield from find(name, depth - 1, test)

def get_sstables(path):
    data_dir = path / 'data'
    return list(find(data_dir, 3, is_sstable_toc))

class SSTable(namedtuple('SSTable', 'path dirname keyspace cf_name cf_uuid version generation')):
    def __new__(cls, toc_path):
        ks, cf, name = toc_path.split()[-3:]
        cf_name, cf_uuid = cf.split('-')
        version, generation = name.split('-')[2:4]
        dirname = toc_path.dirname
        return super().__new__(cls, toc_path, dirname, ks, cf_name, cf_uuid, version, generation)

    def get_files(self):
        def construct_path(toc_entry):
            name = '-'.join((
                self.keyspace,
                self.cf_name,
                self.version,
                self.generation,
                toc_entry.rstrip(),
            ))
            return self.dirname / name

        return tuple(map(construct_path, self.path.open(mode='r')))


def compute_destination_key(config, sstable):
    return '/'.join((
        config['s3']['prefix'],
        sstable.keyspace,
        sstable.cf_name + '-' + sstable.cf_uuid,
        sstable.generation,
    ))

def sstable_data_stream(sstable):
    files = sstable.get_files()

    # This has the side-effect of ensuring that all files present in the TOC
    # actually exist. Don't lose this property.
    size = sum(map(lambda f: f.stat().st_size, files))
    filenames = tuple(map(lambda f: f.basename, files))

    tar_args = ('-b', 128, '-C', sstable.path.dirname, '-c', '--') + filenames
    cmd = tar.__getitem__(tar_args)
    return cmd, size

def load_config(path):
    config_f = open(path, 'r')
    return yaml.safe_load(config_f)

@contextmanager
def s3_upload_future(infile, client, bucket, key):
    config = TransferConfig()
    with TransferManager(client, config) as manager:
        future = manager.upload(fileobj=infile, bucket=bucket, key=key)
        yield future

class ClosableFD(object):
    closed = False

    def __init__(self, fd):
        self.fd = fd

    def close():
        # Our basic purpose, which is to prevent the same FD from being closed
        # twice
        if not self.closed:
            self.closed = True
            os.close(fd)

    def __enter__():
        pass

    def __exit__():
        self.close()

def actual_backup_sstable(config, sstable):
    # Client creation isn't thread-safe, so we do it in advance
    s3 = boto3.client('s3')

    start_time = time.time()
    uncompressed_stream, size = sstable_data_stream(sstable)

    (encrypt_r, compress_w) = os.pipe()
    (s3_r, encrypt_w) = os.pipe()
    compressed_stream_f = (uncompressed_stream | lz4 > compress_w) & BG
    os.close(compress_w)

    encryption_thread = keypipe.seal_thread(
        config['encryption']['provider'],
        config['encryption']['args'],
        encrypt_r,
        encrypt_w,
    )

    #TODO: maybe use s3gof3r instead or in-addition
    s3_key = compute_destination_key(config, sstable)
    s3_fileobj = os.fdopen(s3_r, 'rb')
    with s3_upload_future(s3_fileobj, s3, config['s3']['bucket'], s3_key) as s3_future:
        compressed_stream_f.wait()

        encryption_thread.join()
        os.close(encrypt_r)
        os.close(encrypt_w)

        s3_future.result()
    os.close(s3_r)

    finish_time = time.time()

    elapsed = finish_time - start_time
    rate_MBps = size / 1024. / 1024. / elapsed
    print('Took {} seconds to stream {} bytes. Speed: {} MB/s'.format(elapsed, size, rate_MBps))

def backup_sstable(config, toc_path):
    sstable = SSTable(toc_path)
    actual_backup_sstable(config, sstable)

def backup_all_sstables(config):
    data_dir = LocalPath(config['data_dir'])
    state_dir = config.get('state_dir', data_dir / 'mirroring')

    sstables = get_sstables(config['data_dir'])
    for t in sstables:
        backup_sstable(config, state_dir, t)

def initial_upload():
    config_filename = os.path.expanduser('~/backups.yaml')
    config = load_config(config_filename)
    backup_all_sstables(config)
