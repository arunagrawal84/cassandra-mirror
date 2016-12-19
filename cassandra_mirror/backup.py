from collections import namedtuple
from contextlib import contextmanager
from operator import attrgetter
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp
import argparse
import functools
import itertools
import logging
import os
import os.path
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

from .util import load_config

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

# A recursive directory walker
def find(path, depth):
    if depth == 0:
        yield path
        return

    for name in path.list():
        yield from find(name, depth - 1)

def get_columnfamilies(path):
    return find(path / 'data', 2)

def get_sstables_for_columnfamily(path):
    prev_filenames = None
    filenames = None
    attempts = 0

    """
    getdents contains no atomicity guarantee, and we need to be sure we get
    all SSTables for a given columnfamily. If we fail, we will miss SSTables
    with data in them!
    The race we wish to avoid is:
      backup process: opendir()
      backup process: readdir() part of the directory
      cassandra: create new compacted SSTable
      cassandra: delete ancestor SSTables
      backup process: readdir() the rest of the directory

    It is entirely possible for readdir to not see either the new compacted
    SSTable or the ancestor SSTables that were just deleted.

    We can/must use sstableutil for Cassandra 3.x
    """
    while True:
        filenames = path.list()
        if filenames == prev_filenames:
            break

        prev_filenames = filenames
        attempts += 1
        if attempts > 10:
            # WTF. How could this even...?
            raise RuntimeError("Couldn't get consistent list of SSTables")

    sstables = [
        SSTable.from_cassandra_toc(f)
        for f in filenames
        if is_sstable_toc(f)
    ]

    return sorted(sstables, key=attrgetter('generation'))

class SSTable(namedtuple('SSTable', 'path dirname keyspace cf_name cf_uuid version generation')):
    @classmethod
    def from_cassandra_toc(cls, toc_path):
        ks, cf, name = toc_path.split()[-3:]
        cf_name, cf_uuid = cf.split('-')
        version, generation = name.split('-')[2:4]
        generation = int(generation)
        dirname = toc_path.dirname
        return cls(toc_path, dirname, ks, cf_name, cf_uuid, version, generation)

    @classmethod
    def from_copied_toc(cls, toc_path):
        ks, cf, generation, _, filename = toc_path.split()[-5:]
        generation = int(generation)
        cf_name, cf_uuid = cf.split('-')
        version = filename.split('-')[2]
        dirname = toc_path.dirname
        return cls(toc_path, dirname, ks, cf_name, cf_uuid, version, generation)

    def __new__(cls, *args, **kwargs):
        ret = super().__new__(cls, *args, **kwargs)
        if ret.version != 'ka':
            #TODO: be more compatible
            raise RuntimeError('unknown SSTable version')
        return ret

    def construct_path(self, toc_entry):
        name = '-'.join((
            self.keyspace,
            self.cf_name,
            self.version,
            str(self.generation),
            toc_entry.rstrip(),
        ))
        return self.dirname / name

    def get_immutable_files(self):
        return [
            self.construct_path(f) for f
            in self.path.open(mode='r')
            if f != 'Statistics.db'
        ]

    def get_mutable_files(self):
        return [self.construct_path('Statistics.db')]

def compute_destination_prefix(config, cf_path):
    return '/'.join((
        config['s3']['prefix'],
        #TODO: just read the uuid out of Cassandra
        config['identity'],
        cf_path.dirname.name,
        cf_path.name,
    ))


def tar_stream_command(dirname, files):
    size = sum(map(lambda f: f.stat().st_size, files))
    max_mtime = max(f.stat().st_mtime_ns for f in files)
    filenames = tuple(map(lambda f: f.basename, files))

    tar_args = ('-b', 128, '-C', dirname, '-c', '--') + filenames
    cmd = tar.__getitem__(tar_args)
    return cmd, size, max_mtime

class ClosableFD(int):
    closed = False

    def close(self):
        # Our basic purpose, which is to prevent the same FD from being closed
        # twice
        if not self.closed:
            self.closed = True
            os.close(self)

    def __enter__():
        pass

    def __exit__():
        self.close()

def closeable_pipe():
    return map(ClosableFD, os.pipe())

@contextmanager
def s3_upload_future(infile, client, bucket, key, extra_args=None):
    config = TransferConfig(multipart_chunksize=20 * 1024 * 1024)
    with TransferManager(client, config) as manager:
        future = manager.upload(
            fileobj=infile,
            bucket=bucket,
            key=key,
            extra_args=extra_args
        )
        yield future



def upload_pipe(config, s3_key, data_cmd, mtime):
    """Pipes the output of data_cmd into S3.

    Dataflow: data_cmd | lz4 | keypipe | s3
    """

    # Client creation isn't thread-safe, so we do it in advance
    s3 = boto3.client('s3')

    (encrypt_r, compress_w) = closeable_pipe()
    (s3_r, encrypt_w) = closeable_pipe()
    compressed_stream_f = (data_cmd | lz4 > compress_w) & BG
    compress_w.close()

    encryption_thread = keypipe.seal_thread(
        config['encryption']['provider'],
        config['encryption']['args'],
        encrypt_r,
        encrypt_w,
    )

    #TODO: maybe use s3gof3r in lieu or addition
    s3_fileobj = os.fdopen(s3_r, 'rb')
    with s3_upload_future(
        s3_fileobj,
        s3,
        config['s3']['bucket'],
        s3_key,
        extra_args=dict(Metadata={'Max-File-MTime': str(mtime)}),
    ) as s3_future:
        compressed_stream_f.wait()

        encryption_thread.join()
        encrypt_r.close()
        encrypt_w.close()

        s3_future.result()
    s3_r.close()


def timed_upload_pipe(*args, **kwargs):
    start_time = time.time()
    upload_pipe(*args, **kwargs)
    finish_time = time.time()
    elapsed = finish_time - start_time
    return elapsed

def upload_sstable(config, upload_prefix, sstable):
    marker_dir = sstable.path.dirname.dirname / 'uploaded'
    marker_dir.mkdir()

    # the S3 prefix to be used by all uploaded components
    sstable_prefix = '/'.join((
        upload_prefix,
        'data',
        '{:010}'.format(sstable.generation),
    ))

    immutable_marker = marker_dir / 'immutable'
    if not immutable_marker.exists():
        immutable_files = sstable.get_immutable_files()
        upload_key = '/'.join((sstable_prefix, 'immutable'))
        cmd, size, mtime = tar_stream_command(
            sstable.path.dirname,
            immutable_files,
        )

        timed_upload_pipe(config, upload_key, cmd, mtime)
        immutable_marker.touch()

    mutable_marker = marker_dir / 'mutable'
    mutable_files = sstable.get_mutable_files()
    cmd, size, mtime = tar_stream_command(
        sstable.path.dirname,
        mutable_files,
    )
    if (
        not mutable_marker.exists() or
        mtime > mutable_marker.stat().st_mtime_ns
    ):
        reversed_mtime = (1 << 64) - mtime
        upload_key = '/'.join((
            sstable_prefix,
            'mutable',
            '{:020}'.format(reversed_mtime)
        ))

        timed_upload_pipe(config, upload_key, cmd, mtime)
        with NamedTemporaryFile(dir=mutable_marker.dirname) as f:
            fd = f.fileno()
            os.utime(f.name, ns=(mtime, mtime))
            os.link(f.name, str(mutable_marker))

    return sstable.generation, mtime

def actual_hardlink_sstable(sstable, state_path):
    state_path.dirname.mkdir()
    temp_state_path = LocalPath(mkdtemp(
        suffix='.',
        dir=state_path.dirname,
    ))
    temp_state_path.chmod(0o750)

    files = sstable.get_immutable_files()

    # This has the side-effect of ensuring that all files present in the TOC
    # actually exist. Don't lose this property.
    for f in files:
        f.link(temp_state_path / f.name)

    temp_state_path.move(state_path)

def hardlink_sstable(state_dir, sstable):
    state_path = (
        state_dir / sstable.keyspace /
        '{}-{}'.format(sstable.cf_name, sstable.cf_uuid) /
        '{:010}'.format(sstable.generation) /
        'data'
    )
    if not state_path.exists():
        actual_hardlink_sstable(sstable, state_path)

    for f in sstable.get_mutable_files():
        dest = state_path / f.name

        # If the inodes no longer match, delete and replace the file.
        if dest.stat().st_ino != f.stat().st_ino:
            dest.unlink()
            f.link(dest)

    return SSTable.from_copied_toc(state_path / sstable.path.name)


def actual_upload_manifest(bucket, prefix, sstable, tables):
    # S3 can only scan ascending. Since we usually want to start with the
    # highest generation and work downward, we'll format the string so that
    # the highest generation sorts lexicographically lowest.
    generation_string = '{:010}-{:010}'.format(
        # the maximum generation is actually 2**31 - 1
        (1 << 32) - sstable.generation,
        sstable.generation,
    )

    s3_key = '/'.join((
        prefix,
        'manifest',
        generation_string,
    ))

    body = ''.join([
        '{:010} {}\n'.format(generation, mtime)
        for (generation, mtime)
        in tables
    ]).encode('utf8')

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body,
    )

def upload_manifest(bucket, dest_prefix, sstable, tables):
    marker = sstable.path.dirname.dirname / 'manifest_uploaded'
    if not marker.exists():
        actual_upload_manifest(bucket, dest_prefix, sstable, tables)

    marker.touch()

def backup_columnfamily(state_dir, config, cf_path):
    """Performs backup of a single columnfamily."""

    linked_sstables = [
        hardlink_sstable(state_dir, t)
        for t in get_sstables_for_columnfamily(cf_path)
    ]

    """Invariant: all SSTables in linked_sstables are now hardlinked.

    If we didn't hard link the SSTables, Cassandra could delete files we were
    planning to upload, which will cause upload_sstable() to fail (rightly).
    In the worst case, upload_sstable() would fail over and over again on
    successive runs, and we would never upload a manifest file.
    """

    upload_prefix = compute_destination_prefix(config, cf_path)

    # uploaded_sstables is a list of (generation, mtime) pairs that
    # upload_manifest can format into a manifest.
    uploaded_sstables = [
        upload_sstable(config, upload_prefix, t)
        for t in linked_sstables
    ]

    """Invariant: all SSTables in linked_sstables have been uploaded.

    It is vitally important that we have actually uploaded all SSTables that
    will be mentioned in a manifest file. Otherwise the manifest file will
    mention SSTable(s) that haven't been uploaded.
    """

    if len(linked_sstables) > 0:
        upload_manifest(
            config['s3']['bucket'],
            upload_prefix,
            linked_sstables[-1],
            uploaded_sstables
        )

def backup_all_sstables(config):
    data_dir = LocalPath(config['data_dir'])
    state_dir = config.get('state_dir')
    state_dir = (
        LocalPath(state_dir) if state_dir is not None
        else data_dir / 'mirroring'
    )

    for cf_path in get_columnfamilies(data_dir):
        backup_columnfamily(state_dir, config, cf_path)

def do_backup():
    config_filename = os.path.expanduser('~/backups.yaml')
    config = load_config(config_filename)
    backup_all_sstables(config)
