import argparse
import logging
import os
import sys
from time import mktime
from time import time

import boto3

from .restore import get_sstables_for_labels
from .util import compute_top_prefix
from .util import load_config
from .util import reverse_format_nanoseconds

s3 = boto3.resource('s3')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def to_timestamp(t):
    return mktime(t.timetuple())

def delete_manifests(source, labels_to_keep, prune_before):
    source = source.with_components('manifests')

    trim_length = len(source.key) + 1
    start_label = reverse_format_nanoseconds(prune_before * 1e9)
    for o in source.descendants(start=start_label):
        if o.key[trim_length:] in labels_to_keep:
            continue

        logger.info('Deleting %s', o.key)
        o.delete()

def delete_data(source, generations_to_keep):
    source = source.with_components('data')

    trim_length = len(source.key) + 1
    for o in source.descendants():
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

def prune(source, ttl, marker_dir, label_threshold):
    label_files = marker_dir.list()

    labels_to_keep = set()
    label_files_to_delete = []

    prune_before = time() - ttl * 60 * 60
    for label_file in label_files:
        if label_file.stat().st_mtime < prune_before:
            label_files_to_delete.append(label_file)
        else:
            labels_to_keep.add(label_file.name)

    if len(label_files_to_delete) < label_threshold:
        return

    generations_to_keep = set()

    for ks, cf, gen, _ in get_sstables_for_labels(source, labels_to_keep):
        generations_to_keep.add((ks, cf, gen))

    delete_manifests(source, labels_to_keep, prune_before)
    delete_data(source, generations_to_keep)

    for label_file in label_files_to_delete:
        label_file.delete()
