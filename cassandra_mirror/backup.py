from collections import namedtuple
from fcntl import flock
from fcntl import LOCK_EX
from fcntl import LOCK_NB
from io import BytesIO
from operator import attrgetter
import functools
import logging
import os.path
import time
import sys

from plumbum import BG
from plumbum import FG
from plumbum import LocalPath
import boto3

from .identity import get_identity
from .backup_helpers import is_sstable_toc
from .backup_helpers import stat_helper
from .obsoletion import cleanup_obsoleted
from .obsoletion import mark_obsoleted
from .prune import prune
from .util import MovingTemporaryDirectory
from .util import compute_top_prefix
from .util import continuity_code
from .util import gof3r
from .util import load_config
from .util import reverse_format_nanoseconds
from .util import serialize_context
from .util import timed_touch

from plumbum.cmd import lz4
from plumbum.cmd import tar
logger = logging.getLogger(__name__)

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

    def format_generation(self):
        return '{:010}'.format(self.generation)


def tar_stream_command(dirname, files):
    size = sum(map(lambda f: f.stat().st_size, files))
    max_mtime = max(f.stat().st_mtime_ns for f in files)
    filenames = tuple(map(lambda f: f.basename, files))

    tar_args = ('-b', 128, '-C', dirname, '-c', '--') + filenames
    cmd = tar.__getitem__(tar_args)
    return cmd, size, max_mtime

def upload_s3(cmd, s3_object):
    if gof3r:
        gof3r_cmd = gof3r[
            'put',
            '--no-md5',
            '-b', s3_object.bucket_name,
            '-k', s3_object.key,
        ]

        cmd | gof3r_cmd & FG
    else:
        with cmd.bgrun() as proc:
            s3_object.upload_fileobj(proc.stdout)

def wrap_with_keypipe(cmd, context, config):
    # This import patches plumbum's BaseCommand.
    from keypipe.plumbum_helpers import ThreadCommand
    import keypipe

    keypipe_partial = functools.partial(
        keypipe.seal,
        config['provider'],
        config['args'],
        context,
    )
    cmd = cmd / keypipe_partial

def upload_pipe(data_cmd, s3_object, encryption_context, encryption_config):
    """Pipes the output of data_cmd into S3.

    Dataflow: data_cmd | lz4 | keypipe | s3
    """

    context = serialize_context(encryption_context)
    logger.debug("Processing SSTable with context %s", context.decode('ascii'))
    if encryption_config:
        cmd = wrap_with_keypipe(cmd, context, encryption_config)

    cmd = data_cmd | lz4

    upload_s3(cmd, s3_object)

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

def upload_sstable_immutable(sstable, destination, encryption_config,
    context, marker_dir):

    marker = marker_dir / 'immutable'
    if not marker.exists():
        files = sstable.get_immutable_files()
        cmd, size, mtime = tar_stream_command(
            sstable.path.dirname,
            files,
        )

        upload_sstable_component(
            (marker, mtime),
            size,
            cmd,
            destination.with_components('immutable'),
            merge_dicts(context, dict(component='immutable')),
            encryption_config,
        )

def upload_sstable_mutable(sstable, destination, encryption_config,
    context, marker_dir):

    files = sstable.get_mutable_files()
    cmd, size, mtime = tar_stream_command(
        sstable.path.dirname,
        files,
    )

    marker = marker_dir / 'mutable'
    if (
        not marker.exists() or
        mtime > marker.stat().st_mtime_ns
    ):
        # We namespace the mutable components by their mtime
        # If we did not, a point-in-time recovery would get incorrect
        # repairedAt times, and we would not repair these SSTables.
        destination = destination.with_components(
            'mutable',
            reverse_format_nanoseconds(mtime)
        )

        upload_sstable_component(
            (marker, mtime),
            size,
            cmd,
            destination,
            merge_dicts(context, dict(component='mutable', timestamp=mtime)),
            encryption_config,
        )

    return mtime

def upload_sstable(config, destination, sstable):
    marker_dir = sstable.path.dirname.dirname / 'uploaded'
    marker_dir.mkdir()

    context = dict(config['context'])
    context.update(sstable.get_context())

    # the S3 prefix to be used by all uploaded components
    destination = destination.with_components(sstable.format_generation())

    encryption_config = config.get('encryption')
    upload_sstable_immutable(sstable, destination, encryption_config,
        context, marker_dir)
    mtime = upload_sstable_mutable(sstable, destination, encryption_config,
        context, marker_dir)

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
        sstable.format_generation() /
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

def actual_upload_cf_manifest(destination, sstable, tables):
    destination = destination.with_components(
        sstable.format_generation(),
        'manifest',
    )

    body = BytesIO(''.join([
        '{:010} {}\n'.format(generation, mtime)
        for (generation, mtime)
        in tables
    ]).encode('utf8'))
    destination.upload_fileobj(body)

def upload_cf_manifest(destination, sstable, tables):
    marker = sstable.path.dirname.dirname / 'uploaded' / 'manifest'
    if not marker.exists():
        actual_upload_cf_manifest(destination, sstable, tables)
        marker.touch()

def backup_columnfamily(cf_path, links_dir, destination, config):
    """Performs backup of a single columnfamily."""
    ks_dir, cf = cf_path.dirname, cf_path.basename
    ks = ks_dir.basename
    descriptor = '{}/{}'.format(ks, cf)

    linked_sstables = [
        hardlink_sstable(links_dir, t)
        for t in get_sstables_for_columnfamily(cf_path)
    ]

    if len(linked_sstables) == 0:
        return None

    """Invariant: all SSTables in linked_sstables are now hardlinked.

    If we didn't hard link the SSTables, Cassandra could delete files we were
    planning to upload, which will cause upload_sstable() to fail (rightly).
    In the worst case, upload_sstable() would fail over and over again on
    successive runs, and we would never upload a manifest file.
    """

    destination = destination.with_components('data', ks, cf)

    # seen_sstables is a list of (generation, mtime) pairs that
    # upload_manifest can format into a manifest.
    seen_sstables = [
        upload_sstable(config, destination, t)
        for t in linked_sstables
    ]

    """Invariant: all SSTables in linked_sstables have been uploaded.

    It is vitally important that we have actually uploaded all SSTables that
    will be mentioned in a manifest file. Otherwise the manifest file will
    mention SSTable(s) that haven't been uploaded.
    """

    """
    get_sstables_for_columnfamily returned us SSTables from lowest to highest,
    so to get the highest generation, we just take the last one.
    """
    last_sstable = linked_sstables[-1]

    upload_cf_manifest(
        destination,
        last_sstable,
        seen_sstables,
    )

    return descriptor, last_sstable.format_generation()

def transform_cf_for_manifest(cfs):
    buf = BytesIO()

    # filter out cfs with no SSTables
    for generation, path in filter(None, cfs):
        buf.write('{} {}\n'.format(path, generation).encode('utf-8'))
    buf.seek(0)
    return buf

def upload_global_manifest(columnfamilies, destination, marker_dir):
    # S3 can only scan ascending. Since we usually want to start with the
    # newest backup and work backward, we'll format the string so that
    # the newest backup sorts lexicographically lowest.
    t = time.time()
    t_string = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t))
    ns_since_epoch = time.time() * 1e9
    label = '{} {}'.format(reverse_format_nanoseconds(ns_since_epoch), t_string)

    # Unlike every other marker file, we touch this one _ahead_ of actually
    # completing the operation. That's because this marker file doesn't
    # actually inhibit an upload. Instead it serves to tell the prune script
    # that this backup deserves to be/remain in S3.
    (marker_dir / label).touch()

    destination = destination.with_components('manifests', label)
    body = transform_cf_for_manifest(columnfamilies)

    destination.upload_fileobj(body)
    return label

def fix_identity(config, locs):
    identity = config['context'].get('identity')
    if identity is None:
        config['context']['identity'] = get_identity(locs.state_dir)

def backup_all_sstables(config, locs, destination):
    cf_paths = list(get_columnfamilies(locs.data_dir))
    return [
        backup_columnfamily(p, locs.links_dir, destination, config)
        for p in cf_paths
    ]

def do_backup():
    logging.basicConfig(stream=sys.stderr)
    logger.setLevel(logging.DEBUG)

    config, locs = load_config()

    # fix_identity is only relevant for backup. For other code paths there is
    # no state directory to rely upon.
    fix_identity(config, locs)

    if not locs.data_dir.exists():
        raise RuntimeException('data_dir does not exist')

    locs.state_dir.mkdir()
    lock_fh = (locs.state_dir / 'lock').open('w')
    flock(lock_fh.fileno(), LOCK_EX | LOCK_NB)

    destination = compute_top_prefix(config)
    cf_specs = backup_all_sstables(config, locs, destination)

    marker_dir = (locs.state_dir / 'manifests')
    marker_dir.mkdir()
    label = upload_global_manifest(cf_specs, destination, marker_dir)
    print(label)
    prune(destination, config['ttl'], marker_dir, 8)

    mark_obsoleted(locs)
    cleanup_obsoleted(locs, 0)
