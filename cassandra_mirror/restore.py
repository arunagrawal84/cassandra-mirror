from collections import namedtuple
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

import keypipe
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from keypipe.plumbum_helpers import ThreadCommand
from plumbum import FG
from plumbum import LocalPath
import boto3

from .util import MovingTemporaryDirectory
from .util import compute_top_prefix
from .util import continuity_code
from .util import get_creds_dict
from .util import load_config
from .util import serialize_context
from .util import timed_touch

from plumbum.cmd import s3gof3r
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

def get_columnfamilies(bucket, prefix):
    for ks in get_common_prefixes(bucket, '{}/'.format(prefix)):
        for cf in get_common_prefixes(bucket, '{}/{}/'.format(prefix, ks)):
            yield ks, cf

def download_to_path(
    marker_path,
    s3_object,
    destination,
    provider_args,
    encryption_context
):
    context = serialize_context(encryption_context)
    logger.debug("Invoking keypipe with context %s", context)

    keypipe_partial = functools.partial(
        keypipe.unseal,
        provider_args,
        context,
    )

    credentials = get_creds_dict()

    gof3r_cmd = s3gof3r[
        'get',
        '--no-md5',
        '-b', s3_object.bucket_name,
        '-k', s3_object.key,
    ].with_env(**credentials)

    prefix = marker_path.name + '.'

    with TemporaryDirectory(prefix=prefix, dir=destination.up()) as d:
        temp_destination = LocalPath(d)
        start_time = time.time()
        (
            gof3r_cmd /
            keypipe_partial |
            lz4['-d'] |
            tar['-C', temp_destination, '-x']
        ) & FG
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
    path,
    objects,
    sstable_context,
    mutable_mtime
):
    if (path / 'data').exists():
        """Note that we use the existence of a directory named 'data' to
        inhibit downloads. This stands in contrast to uploads, which we inhibit
        with separate marker files.

        The practical upshot is that, if we are going to put a directory called
        'data' in place, it had better contain valid data.
        """
        return

    provider_args = {
        config['encryption']['provider']: config['encryption']['args']
    }

    uploaded_dir = path / 'uploaded'
    uploaded_dir.mkdir()

    with MovingTemporaryDirectory(path / 'data') as temp:
        for (marker_name, s3_object) in objects:
            context = dict(sstable_context)
            context.update(config['context'])
            context['component'] = marker_name
            context['continuity'] = continuity_code
            if marker_name == 'mutable':
                context['timestamp'] = mutable_mtime

            marker_path = uploaded_dir / marker_name
            download_to_path(
                marker_path,
                s3_object,
                temp.path,
                provider_args,
                context,
            )

        temp.finalize()

ManifestEntry = namedtuple('ManifestEntry', 'generation, mtime')

def get_sstables_to_download_for_cf(config, bucket, prefix, ks, cf):
    for i in bucket.objects.filter(
        Prefix='{}/{}/{}/manifest/'.format(prefix, ks, cf),
    ).limit(1):
        manifest_lines = i.get()['Body'].read().decode('utf8').splitlines()

        return [
            ManifestEntry(*line.split())
            for line in manifest_lines
        ]

def create_metadata_directories(sstables):
    for cf_dir, ks, cf, manifest_entries in sstables:
        last_entry = manifest_entries[-1]
        uploaded_dir = (cf_dir / last_entry.generation / 'uploaded')
        uploaded_dir.mkdir()
        (uploaded_dir / 'manifest').mkdir()

def _get_download_instructions_for_cf(
    identity,
    bucket,
    prefix,
    sstable
):
    cf_dir, ks, cf, manifest_entries = sstable
    for entry in manifest_entries:
        generation, mutable_mtime = entry
        generation_dir = cf_dir / generation
        sstable_prefix = '{}/{}/{}/data/{}'.format(prefix, ks, cf, generation)

        mutable_mtime = int(mutable_mtime)
        reversed_mtime = (1<<64) - mutable_mtime

        context = dict(
            identity=identity,
            keyspace=ks,
            columnfamily=cf,
            generation=int(generation),
        )

        objects = (
            ('mutable', bucket.Object('{}/mutable/{:020}'.format(
                sstable_prefix,
                reversed_mtime
            ))),
            ('immutable', bucket.Object('{}/immutable'.format(
                sstable_prefix
            ))),
        )

        yield generation_dir, objects, context, mutable_mtime

def get_download_instructions(identity, bucket, prefix, sstables):
    for i in sstables:
        yield from _get_download_instructions_for_cf(identity, bucket, prefix, i)

def get_sstables_to_download(config, bucket, prefix):
    for ks, cf in get_columnfamilies(bucket, prefix):
        tables = get_sstables_to_download_for_cf(config, bucket, prefix, ks, cf)
        yield ks, cf, tables

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

def restore(identity):
    config_filename = os.path.expanduser('~/backups.yaml')
    config = load_config(config_filename)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(config['s3']['bucket'])
    prefix = compute_top_prefix(config, identity)

    i = get_sstables_to_download(config, bucket, prefix)
    sstables = list(compute_cf_dirs(LocalPath('mirrored'), i))

    create_metadata_directories(sstables)
    instructions = get_download_instructions(identity, bucket, prefix, sstables)

    # It is assumed we'll be disk-bound, so I've chosen a typical disk queue depth.
    with futures.ThreadPoolExecutor(max_workers=32) as executor:
        fs = [
            executor.submit(download_sstable_to_path, config, *i)
            for i in instructions
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
    parser.add_argument('source_identity',
        help='The identity (typically UUID) of the node whose backup should be restored'
    )
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stderr)
    logger.setLevel(logging.INFO)

    if LocalPath('data').exists():
        logger.info('Skipping restoration because a data directory already exists')
        return

    restore(args.source_identity)

if __name__ == '__main__':
    sys.exit(do_restore())
