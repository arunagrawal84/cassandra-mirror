from subprocess import PIPE
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

from plumbum.cmd import s3gof3r
from plumbum.cmd import lz4
from plumbum.cmd import tar

#boto3.set_stream_logger('botocore.vendored.requests.packages.urllib3', logging.DEBUG)

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

def download_to_path(config, s3_object, path):
    provider_args = {
        config['encryption']['provider']: config['encryption']['args']
    }

    keypipe_partial = functools.partial(
        keypipe.unseal,
        provider_args,
    )

    gof3r_cmd = s3gof3r[
        'get',
        '--no-md5',
        '-b', s3_object.bucket_name,
        '-k', s3_object.key,
    ].with_env(**config['s3']['credentials'])
   
    path.mkdir() 

    basename = s3_object.key.split('/')[-1]

    (
        gof3r_cmd /
        keypipe_partial |
        lz4['-d'] |
        tar['-C', path, '-x']
    ) & FG
    
def get_objects_to_download_for_cf(config, bucket, prefix, ks, cf):
    for i in bucket.objects.filter(
        Prefix='{}/{}/{}/manifest/'.format(prefix, ks, cf),
        MaxKeys=1,
    ):
        cf_dir = LocalPath(ks) / cf
        cf_dir.mkdir()

        manifest_lines = i.get()['Body'].read().decode('utf8').splitlines()
        for line in manifest_lines:
            generation, mtime = line.split()
            generation_dir = cf_dir / generation
            sstable_prefix = '{}/{}/{}/data/{}'.format(prefix, ks, cf, generation)
           
            reversed_mtime = (1<<64) - int(mtime)
            s3_objects = map(bucket.Object, (
                '{}/mutable/{:020}'.format(sstable_prefix, reversed_mtime),
                '{}/immutable'.format(sstable_prefix),
            ))

            for o in s3_objects:
                yield (o, generation_dir)

def get_objects_to_download(config, bucket, prefix):
    for ks, cf in get_columnfamilies(bucket, prefix):
        yield from get_objects_to_download_for_cf(config, bucket, prefix, ks, cf)

def get_creds_dict():
    creds = boto3.Session().get_credentials()
    creds_dict = dict(
        AWS_ACCESS_KEY_ID=creds.access_key,
        AWS_SECRET_ACCESS_KEY=creds.secret_key,
    )
    if creds.token is not None:
        creds_dict['AWS_SECURITY_TOKEN'] = creds.token
    return creds_dict
        
def restore():
    config_filename = os.path.expanduser('~/backups.yaml')
    config = load_config(config_filename)

    #TODO: it seems improper to modify the config dict this way.
    config['s3']['credentials'] = get_creds_dict()

    bucket = config['s3']['bucket']
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    prefix = compute_source_prefix(config) 

    objects = get_objects_to_download(config, bucket, prefix)
    def download(args):
        return download_to_path(config, *args)

    # It is assumed we'll be disk-bound, so I've chosen a typical disk queue depth. 
    with futures.ThreadPoolExecutor(max_workers=32) as executor:
        results = executor.map(download, objects)

if __name__ == '__main__':
    sys.exit(restore())
