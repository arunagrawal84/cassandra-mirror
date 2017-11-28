import argparse
import logging
import sys
from time import mktime
from time import time

import boto3

from .restore import get_generations_referenced_by_manifest
from .util import compute_top_prefix
from .util import load_config
from .util import reverse_format_nanoseconds

s3 = boto3.resource('s3')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def to_timestamp(t):
    return mktime(t.timetuple())

def delete_manifests(source, labels_to_keep, grace_after):
    source = source.with_components('manifests')

    trim_length = len(source.key) + 1
    start_label = reverse_format_nanoseconds(grace_after * 1e9)
    for o in source.descendants(start=start_label):
        if o.key[trim_length:] in labels_to_keep:
            continue

        logger.info('Deleting %s', o.key)
        o.delete()

def delete_data(source, generations_to_keep, grace_after):
    source = source.with_components('data')

    trim_length = len(source.key) + 1
    for o in source.descendants():
        if to_timestamp(o.last_modified) > grace_after:
            continue

        # Trim the key to remove `source` as a prefix
        trimmed_key = o.key[trim_length:]
        ks, cf, gen, _ = trimmed_key.split('/', 3)
        if (ks, cf, gen) in generations_to_keep:
            continue

        logger.info('Deleting %s', o.key)
        o.delete()

def verify_labels_is_comprehensive(source, labels_to_keep, grace_after):
    source = source.with_components('manifests')
    stop_at = reverse_format_nanoseconds(grace_after * 1e9)

    trim_length = len(source.key) + 1
    for o in source.descendants():
        label = o.key[trim_length:]
        print((label, stop_at))
        if label > stop_at:
            break

        if label not in labels_to_keep:
            raise RuntimeError('List of labels to keep does not cover the grace period')


def prune(ttl, labels_to_keep):
    config = load_config()

    grace_after = time() - ttl * 60 * 60

    source = compute_top_prefix(config)

    labels_to_keep = set(labels_to_keep)
    generations_to_keep = set()


    verify_labels_is_comprehensive(source, labels_to_keep, grace_after)

    for label in labels_to_keep:
        for i in get_generations_referenced_by_manifest(source, label):
            ks, cf, gen, _ = i
            generations_to_keep.add((ks, cf, gen))

    delete_manifests(source, labels_to_keep, grace_after)
    delete_data(source, generations_to_keep, grace_after)

def do_prune():
    parser = argparse.ArgumentParser(
        description='Prune a cassandra-mirror backup directory of old backups',
    )

    parser.add_argument('ttl', type=int, help='TTL, in hours')
    parser.add_argument('label', nargs='+',
        help='Backup labels to retain'
    )

    args = parser.parse_args()

    logging.basicConfig(stream=sys.stderr)

    prune(args.ttl, args.label)
