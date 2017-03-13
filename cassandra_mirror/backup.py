from collections import namedtuple
from operator import attrgetter
import functools
import logging
import os.path
import time
import sys

# This import patches plumbum's BaseCommand.
from keypipe.plumbum_helpers import ThreadCommand
from plumbum import FG
from plumbum import LocalPath
import boto3
import keypipe

from plumbum.cmd import lz4
from plumbum.cmd import tar
from plumbum.cmd import s3gof3r

from .identity import get_identity

from .backup_helpers import is_sstable_toc
from .backup_helpers import stat_helper

from .obsoletion import cleanup_obsoleted
from .obsoletion import mark_obsoleted

from .util import MovingTemporaryDirectory
from .util import compute_top_prefix
from .util import continuity_code
from .util import load_config
from .util import serialize_context
from .util import timed_touch

logger = logging.getLogger(__name__)

s3 = boto3.resource('s3')

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
    getdents provides no atomicity guarantee, and we need to be sure we get
    all SSTables for a given columnfamily. If we fail, we will miss SSTables
    with data in them!
    The race we wish to avoid is:
      backup process: opendir()
      backup process: readdir() part of the directory
      cassandra: create new compacted SSTable
      cassandra: delete ancestor SSTables
      backup process: readdir() the rest of the directory

    It is entirely possible for readdir to see neither the new compacted
    SSTable nor the ancestor SSTables that were just deleted.

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
        parts = str(self.path).split('-')[:-1]
        parts.append(toc_entry)
        return LocalPath('-'.join(parts))

    def get_immutable_files(self):
        return [
            self.construct_path(f) for f
            in self.toc_items()
            if f != 'Statistics.db'
        ]

    def toc_items(self):
        return map(
            lambda i: i.rstrip(),
            self.path.open(mode='r')
        )

    def get_mutable_files(self):
        return [self.construct_path('Statistics.db')]

    def get_context(self):
        return dict(
            keyspace=self.keyspace,
            columnfamily='{}-{}'.format(self.cf_name, self.cf_uuid),
            generation=self.generation,
            continuity=continuity_code,
        )

def compute_destination_prefix(config, identity, cf_path):
    return '/'.join((
        compute_top_prefix(config, identity),
        cf_path.dirname.name,
        cf_path.name,
    ))

def tar_stream_command(dirname, files):
    # By this point the files should be safely hard linked, so mtime and size
    # can't change from under us.
    size = sum(map(lambda f: f.stat().st_size, files))
    max_mtime = max(f.stat().st_mtime_ns for f in files)
    filenames = tuple(map(lambda f: f.basename, files))

    tar_args = ('-b', 128, '-C', dirname, '-c', '--') + filenames
    cmd = tar.__getitem__(tar_args)
    return cmd, size, max_mtime

def upload_pipe(data_cmd, s3_object, encryption_context, encryption_config):
    """Pipes the output of data_cmd into S3.

    Dataflow: data_cmd | lz4 | keypipe | s3
    """

    gof3r_cmd = s3gof3r[
        'put',
        '--no-md5',
        '-b', s3_object.bucket_name,
        '-k', s3_object.key,
    ]

    context = serialize_context(encryption_context)
    logger.debug("Invoking keypipe with context %s", context)

    keypipe_partial = functools.partial(
        keypipe.seal,
        encryption_config['provider'],
        encryption_config['args'],
        context,
    )

    (data_cmd | lz4 / keypipe_partial | gof3r_cmd) & FG

def upload_sstable_component(
    marker_info,
    size,
    data_cmd,
    s3_object,
    encryption_context,
    encryption_config
):
    start_time = time.time()
    elapsed = upload_pipe(
        data_cmd,
        s3_object,
        encryption_context,
        encryption_config,
    )
    finish_time = time.time()
    elapsed = finish_time - start_time
    speed = size / elapsed / 1024
    logger.info('Uploaded to %s. %s bytes in %.3f seconds: %s KB/s',
        s3_object.key,
        size,
        elapsed,
        "{:,.2f}".format(speed)
    )
    timed_touch(*marker_info)

def merge_dicts(*args):
    ret = {}
    for d in args:
        ret.update(d)
    return ret

def upload_sstable_immutable(config, identity, sstable, prefix, context, marker_dir):
    marker = marker_dir / 'immutable'
    if not marker.exists():
        destination_key = '/'.join((prefix, 'immutable'))
        destination = s3.Object(config['s3']['bucket'], destination_key)

        files = sstable.get_immutable_files()
        cmd, size, mtime = tar_stream_command(
            sstable.path.dirname,
            files,
        )

        upload_sstable_component(
            (marker, mtime),
            size,
            cmd,
            destination,
            merge_dicts(context, dict(component='immutable')),
            config['encryption']
        )

def upload_sstable_mutable(config, identity, sstable, prefix, context, marker_dir):
    marker = marker_dir / 'mutable'

    files = sstable.get_mutable_files()
    cmd, size, mtime = tar_stream_command(
        sstable.path.dirname,
        files,
    )

    if (
        not marker.exists() or
        mtime > marker.stat().st_mtime_ns
    ):
        reversed_mtime = (1 << 64) - mtime
        destination_key = '/'.join((
            prefix,
            'mutable',
            '{:020}'.format(reversed_mtime)
        ))
        destination = s3.Object(config['s3']['bucket'], destination_key)

        upload_sstable_component(
            (marker, mtime),
            size,
            cmd,
            destination,
            merge_dicts(context, dict(component='mutable', timestamp=mtime)),
            config['encryption'],
        )

    return mtime


def upload_sstable(config, identity, upload_prefix, sstable):
    marker_dir = sstable.path.dirname.dirname / 'uploaded'
    marker_dir.mkdir()

    context = dict(config['context'])
    context.update(sstable.get_context())
    context['identity'] = identity

    # the S3 prefix to be used by all uploaded components
    sstable_prefix = '/'.join((
        upload_prefix,
        'data',
        '{:010}'.format(sstable.generation),
    ))

    upload_sstable_immutable(config, identity, sstable, sstable_prefix, context, marker_dir)
    mtime = upload_sstable_mutable(config, identity, sstable, sstable_prefix, context, marker_dir)

    return sstable.generation, mtime

def actual_hardlink_sstable(sstable, state_path):
    state_path.dirname.mkdir()
    with MovingTemporaryDirectory(state_path) as d:
        files = sstable.get_immutable_files()

        # This has the side-effect of ensuring that all files present in the TOC
        # actually exist. Don't lose this property.
        for f in files:
            f.link(d.path / f.name)

        d.path.chmod(0o750)
        d.finalize()

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
        stat = stat_helper(dest)

        # If the inodes no longer match, delete and replace the file.
        if stat is None or stat.st_ino != f.stat().st_ino:
            try:
                dest.unlink()
            except FileNotFoundError:
                pass
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

def upload_manifest(bucket, prefix, sstable, tables):
    marker = sstable.path.dirname.dirname / 'uploaded' / 'manifest'
    if not marker.exists():
        actual_upload_manifest(bucket, prefix, sstable, tables)
        marker.touch()

def backup_columnfamily(links_dir, config, identity, cf_path):
    """Performs backup of a single columnfamily."""

    linked_sstables = [
        hardlink_sstable(links_dir, t)
        for t in get_sstables_for_columnfamily(cf_path)
    ]

    """Invariant: all SSTables in linked_sstables are now hardlinked.

    If we didn't hard link the SSTables, Cassandra could delete files we were
    planning to upload, which will cause upload_sstable() to fail (rightly).
    In the worst case, upload_sstable() would fail over and over again on
    successive runs, and we would never upload a manifest file.
    """

    upload_prefix = compute_destination_prefix(config, identity, cf_path)

    # uploaded_sstables is a list of (generation, mtime) pairs that
    # upload_manifest can format into a manifest.
    uploaded_sstables = [
        upload_sstable(config, identity, upload_prefix, t)
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

def backup_all_sstables(config, locs):
    identity = get_identity(locs.state_dir)
    for cf_path in get_columnfamilies(locs.data_dir):
        backup_columnfamily(locs.links_dir, config, identity, cf_path)

class Locations(namedtuple('Locations', 'data_dir sstables_dir state_dir links_dir')):
    pass

def do_backup():
    logging.basicConfig(stream=sys.stderr)
    logger.setLevel(logging.DEBUG)

    config = load_config()
    data_dir = LocalPath(config.get('data_dir', '/var/lib/cassandra'))
    state_dir = config.get('state_dir')
    state_dir = (
        LocalPath(state_dir) if state_dir is not None
        else data_dir / 'mirroring'
    )
    state_dir.mkdir()

    locs = Locations(data_dir, data_dir / 'data', state_dir, state_dir / 'links')
    backup_all_sstables(config, locs)
    mark_obsoleted(locs)
    cleanup_obsoleted(locs, 0)
