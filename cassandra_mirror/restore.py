from collections import namedtuple
from subprocess import PIPE
from tempfile import TemporaryDirectory
import functools
import logging
import os
import os.path

import keypipe
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

from keypipe.plumbum_helpers import ThreadCommand
from plumbum import FG
from plumbum import LocalPath
import boto3

from .util import load_config
from .util import timed_touch
from .util import get_creds_dict

from plumbum.cmd import s3gof3r
from plumbum.cmd import lz4
from plumbum.cmd import tar

boto3.set_stream_logger('botocore.vendored.requests.packages.urllib3', logging.DEBUG)

def compute_source_prefix(config):
    return '/'.join((
        config['s3']['prefix'],
        config['identity'],
    ))

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

def download_to_path(marker_path, s3_object, destination, provider_args):
    keypipe_partial = functools.partial(
        keypipe.unseal,
        provider_args,
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
        (
            gof3r_cmd /
            keypipe_partial |
            lz4['-d'] |
            tar['-C', temp_destination, '-x']
        ) & FG

        max_mtime = max(f.stat().st_mtime_ns for f in temp_destination)
        timed_touch(marker_path, max_mtime)

        """Invariant: marker path exists before files are moved into the
        final data_dir.

        This prevents files that we just downloaded from being re-uploaded.
        Note that, unlike when we upload, the presence of this marker file does
        not inhibit downloads.
        """

        for i in temp_destination:
            i.link(destination / i.name)

class MovingTemporaryDirectory(TemporaryDirectory):
    def __init__(self, destination):
        self.destination = str(destination)
        super().__init__(
            prefix=destination.name + '.', 
            dir=destination.up(),
        )

    def __enter__(self):
        return self

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


def download_sstable_to_path(config, path, objects):
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
            marker_path = uploaded_dir / marker_name
            download_to_path(
                marker_path,
                s3_object,
                temp.path(),
                provider_args,
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
    bucket,
    prefix,
    cf_dir,
    ks,
    cf,
    manifest_entries,
):
    for entry in manifest_entries:
        generation, mtime = entry
        generation_dir = cf_dir / generation
        sstable_prefix = '{}/{}/{}/data/{}'.format(prefix, ks, cf, generation)
       
        reversed_mtime = (1<<64) - int(mtime)
        objects = (
            ('mutable', bucket.Object('{}/mutable/{:020}'.format(
                sstable_prefix, 
                reversed_mtime
            ))),
            ('immutable', bucket.Object('{}/immutable'.format(
                sstable_prefix
            ))),
        )

        yield generation_dir, objects

def get_download_instructions(bucket, prefix, sstables):
    for i in sstables:
        yield from _get_download_instructions_for_cf(bucket, prefix, *i)

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

def actual_restore():
    config_filename = os.path.expanduser('~/backups.yaml')
    config = load_config(config_filename)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(config['s3']['bucket'])
    prefix = compute_source_prefix(config) 

    i = get_sstables_to_download(config, bucket, prefix)
    sstables = list(compute_cf_dirs(LocalPath('mirrored'), i))

    create_metadata_directories(sstables)
    instructions = get_download_instructions(bucket, prefix, sstables)

    # It is assumed we'll be disk-bound, so I've chosen a typical disk queue depth. 
    with futures.ThreadPoolExecutor(max_workers=32) as executor:
        fs = [
            executor.submit(download_sstable_to_path, config, *i)
            for i in instructions
        ]
        for f in futures.as_completed(fs):
            # Raise any exceptions from the executor
            f.result()

    with MovingTemporaryDirectory(LocalPath('data')) as d:
        copy_back(LocalPath('mirrored'), d.path())
        d.finalize()

def restore():
    if LocalPath('data').exists():
        return

    actual_restore()

if __name__ == '__main__':
    sys.exit(restore())
