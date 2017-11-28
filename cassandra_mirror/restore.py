from collections import namedtuple
from io import BytesIO
from subprocess import PIPE
from tempfile import TemporaryDirectory
import argparse
import functools
import json
import logging
import os
import os.path
import sys
import time

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from plumbum import BG
from plumbum import FG
from plumbum import LocalPath
from plumbum.commands.modifiers import PIPE
import boto3

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

def get_common_prefixes(bucket, prefix):
    cut = len(prefix)
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')
    result = paginator.paginate(
        Bucket=bucket.name,
        Prefix=prefix,
        Delimiter='/'
    )
    for i in result.search('CommonPrefixes'):
        yield i['Prefix'][cut:-1]

def keypipe_cmd(provider_args, context):
    import keypipe
    from keypipe.plumbum_helpers import ThreadCommand
    keypipe_partial = functools.partial(
        keypipe.unseal,
        provider_args,
        context,
    )

    return ThreadCommand(keypipe_partial)

def pipe():
    (r, w) = os.pipe()
    return BytesIO(r), BytesIO(w)


def download_s3(cmd, s3_object):
    if gof3r:
        gof3r_cmd = s3gof3r[
            'get',
            '--no-md5',
            '-b', s3_object.bucket_name,
            '-k', s3_object.key,
        ]
        gof3r_cmd | cmd & FG
    else:
        with cmd.bgrun(stdin=PIPE) as future:
            s3_object.download_fileobj(future.stdin)


def download_to_path(
    marker_path,
    s3_object,
    destination,
    provider_args,
    encryption_context
):
    context = serialize_context(encryption_context)
    logger.debug("Invoking keypipe with context %s", context)

    prefix = marker_path.name + '.'

    with TemporaryDirectory(prefix=prefix, dir=destination.up()) as d:
        temp_destination = LocalPath(d)
        cmd = lz4['-d'] | tar['-C', temp_destination, '-x']
        if provider_args:
            cmd = keypipe_cmd(provider_args, context) | cmd

        start_time = time.time()
        download_s3(cmd, s3_object)
        finish_time = time.time()
        elapsed = finish_time - start_time

        max_mtime = max(f.stat().st_mtime_ns for f in temp_destination)
        size = sum(f.stat().st_size for f in temp_destination)

        speed = size / elapsed / 1024
        logger.info('Downloaded from %s. %s bytes in %.3f seconds: %s KB/s',
            s3_object.key,
            size,
            elapsed,
            "{:,.2f}".format(speed),
        )

        timed_touch(marker_path, max_mtime)

        """Invariant: marker path exists before files are moved into the
        final data_dir.

        This prevents files that we just downloaded from being re-uploaded.
        Note that, unlike when we upload, the presence of this marker file does
        not inhibit downloads.
        """

        for i in temp_destination:
            i.link(destination / i.name)


def download_sstable_to_path(
    config,
    dest,
    sstable,
):
    ks, cf, gen, mtime = sstable
    dest = dest / ks / cf / gen

    sstable_context = dict(config['context'])
    sstable_context['keyspace'] = ks
    sstable_context['columnfamily'] = cf
    sstable_context['generation'] = int(gen)

    if (dest / 'data').exists():
        """Note that we use the existence of a directory named 'data' to
        inhibit downloads. This stands in contrast to uploads, which we inhibit
        with separate marker files.

        The practical upshot is that, if we are going to put a directory called
        'data' in place, it had better contain valid data.
        """
        return

    provider_args = None
    if 'encryption' in config:
        provider_args = {
            config['encryption']['provider']: config['encryption']['args']
        }

    uploaded_dir = dest / 'uploaded'
    uploaded_dir.mkdir()

    reversed_mtime = reverse_format_nanoseconds(mtime)

    objects = (
        ('mutable', source.with_components('mutable', reversed_mtime)),
        ('immutable', source.with_components('immutable')),
    )

    with MovingTemporaryDirectory(dest / 'data') as temp:
        for (marker_name, s3_object) in objects:
            context = dict(sstable_context)
            context['component'] = marker_name
            context['continuity'] = continuity_code
            if marker_name == 'mutable':
                context['timestamp'] = mtime

            marker_path = uploaded_dir / marker_name
            download_to_path(
                marker_path,
                s3_object,
                temp.path,
                provider_args,
                context,
            )

        temp.finalize()

def create_manifest_markers(sources, dest):
    """Creates the files indicating that a manifest was uploaded for this
    generation. By doing so, we inhibit future manifests for being uploaded for
    this generation."""

    for ks, cf, gen in sources:
        uploaded_dir = dest / ks / cf / gen / 'uploaded'
        uploaded_dir.mkdir()
        (uploaded_dir / 'manifest').touch()

"""
Hierarchical Manifest Layout

A global manifest points to individual columnfamilies, each with a maximum
generation.  Each generation points to its own data files. Too, its manifest
references zero or more other generations. The CF manifests are not transitive:
we do not consult the manifest of any generation other than the maximum one.

Manifest (manifests/<label>)
|_ CF Manifests (data/<ks>/<cf>/<gen>/manifest)
   |_ Generations (data/<ks>/<cf>/<gen>)
      |_ Immutable component (data/<ks>/<cf>/<gen>/immutable)
      |_ Mutable component (data/<ks>/<cf>/<gen>/mutable/<mtime>)
"""

def _get_global_manifest_entries(source, label):
    source = source.with_components('manifests', label)
    for i in source.read_utf8().splitlines(keepends=False):
        max_generation, ks_cf = i.split()
        ks, cf = ks_cf.split('/')
        yield ks, cf, max_generation

def _spider_global_manifest_entries(source, entries):
    source = source.with_components('data')
    for ks, cf, max_gen in entries:
        manifest = source.with_components(ks, cf, max_gen, 'manifest')
        for l in manifest.read_utf8().splitlines(keepends=False):
            gen, mtime = l.split()
            yield ks, cf, gen, mtime

def get_generations_referenced_by_manifest(source, label):
    entries = _get_global_manifest_entries(source, label)
    return _spider_global_manifest_entries(source, entries)

def compute_cf_dirs(base, sstables):
    for ks, cf, entries in sstables:
        yield base / ks / cf, ks, cf, entries

def copy_single_sstable(ks, cf, src, dst):
    for i in src:
        i.link(dst / ks / cf / i.name)

def copy_back(src, dst):
    for ks_dir in src:
        ks = ks_dir.name
        for cf_dir in ks_dir:
            cf = cf_dir.name
            (dst / ks / cf).mkdir()
            for generation_dir in cf_dir:
                copy_single_sstable(
                    ks_dir.name,
                    cf_dir.name,
                    generation_dir / 'data',
                    dst
                )

def restore(identity, manifest_label, workers):
    config = load_config()

    s3 = boto3.resource('s3')
    source = compute_top_prefix(config)
    manifest_entries = list(_get_global_manifest_entries(source, label))

    dest = LocalPath('mirrored')
    create_manifest_markers(manifest_entries, dest)

    sstables = _spider_global_manifest_entries(source, manifest_entries)

    with futures.ThreadPoolExecutor(workers) as executor:
        fs = [
            executor.submit(download_sstable_to_path, config, dest, sstable)
            for sstable in sstables
        ]
        for f in futures.as_completed(fs):
            try:
                # Raise any exceptions from the executor
                f.result()
            except:
                # If there is an exception, cancel the inflight futures
                # No point throwing good work after bad
                for inflight in fs:
                    inflight.cancel()
                raise

    with MovingTemporaryDirectory(LocalPath('data')) as d:
        copy_back(LocalPath('mirrored'), d.path)
        d.finalize()

def do_restore():
    parser = argparse.ArgumentParser(
        description='Restore a Cassandra backup into the current working directory.'
    )
    parser.add_argument('--workers', '-w', type=int, default=32)
    parser.add_argument('source_identity',
        help='The identity (typically UUID) of the node whose backup should be restored'
    )
    parser.add_argument('manifest_label', nargs='?')

    args = parser.parse_args()

    logging.basicConfig(stream=sys.stderr)
    logger.setLevel(logging.INFO)

    if LocalPath('data').exists():
        logger.info('Skipping restoration because a data directory already exists')
        return

    restore(args.source_identity, args.manifest_label, args.workers)

if __name__ == '__main__':
    sys.exit(do_restore())
