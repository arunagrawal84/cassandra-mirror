import argparse
import logging
import sys

from botocore.exceptions import ClientError
import boto3

from .restore import get_sstable_objects
from .restore import get_sstables_for_labels

from .util import compute_top_prefix
from .util import load_config

logger = logging.getLogger(__name__)

def object_exists(path):
    try:
        path.load()
    except ClientError as e:
        if e.response['Error']['Code'] != "404":
            raise

        return False
    return True

def check(labels):
    config, locs = load_config()

    if len(labels) == 0:
        marker_dir = (locs.state_dir / 'manifests')
        labels = [i.name for i in marker_dir.list()]

    s3 = boto3.resource('s3')
    source = compute_top_prefix(config)

    sstables = get_sstables_for_labels(source, labels)

    objects = set()
    for s in sstables:
        objects.update(get_sstable_objects(source, s))

    corrupt_objects = set()
    for type_, path in objects:
        logger.info('Checking object: %s', path.key)
        if not object_exists(path):
            corrupt_objects.add(path)

    for o in corrupt_objects:
        print(o.key)

    if len(corrupt_objects):
        # An uncaught exception returns a 1, so we'll distinguish ourselves
        # from that.
        return 2

def do_check():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    parser = argparse.ArgumentParser(
        description='Check the objects associated with one or more Cassandra backups.'
    )

    # Hmm, we could add an --only-latest flag.
    # There are situations where a backup is irreparably corrupted, and the
    # only recourse is to upload a new backup. So --only-latest would allow
    # any monitoring based on this script to turn OK after a new backup is
    # finished.
    # Or we could add a --valid-within flag, which would check whether there
    # is *any* backup valid within (say) the last 8 hours. As opposed to this
    # script which checks all backups.

    parser.add_argument('label', nargs='*',
        help=('Backup labels to check. If unspecified, all labels known to '
        'the local machine will be checked')
    )

    args = parser.parse_args()

    return check(args.label)
