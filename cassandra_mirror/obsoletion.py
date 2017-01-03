from os import removedirs
from shutil import rmtree
import time

from .backup_helpers import is_sstable_toc
from .backup_helpers import stat_helper

def mark_obsoleted(locs):
    for ks in locs.links_dir:
        orig_ks = locs.sstables_dir / ks.name
        for cf_dir in ks:
            orig_cf = orig_ks / cf_dir.name
            for generation in cf_dir:
                tocs = list(filter(is_sstable_toc, generation / 'data'))
                if len(tocs) != 1:
                    raise RuntimeError('Found {} TOCs in {}.'.format(len(tocs), str(cf_dir)))
                toc = tocs[0]
                cassandra_toc = orig_cf / toc.name
                if stat_helper(cassandra_toc) is None:
                    (generation / 'obsolete').touch()

def maybe_delete_sstable_links(sstable_path, threshold):
    s = stat_helper(sstable_path / 'obsolete')
    if s is None:
        return

    if s.st_mtime < threshold:
        rmtree(str(sstable_path))

        # If we've removed the last entry in this columnfamily, recursively
        # clean up.
        if sum(1 for i in sstable_path.up()) == 0:
            removedirs(str(sstable_path.up()))

def cleanup_obsoleted(locs, max_age):
    threshold = time.time() - max_age

    for ks in locs.links_dir:
        for cf_dir in ks:
            for generation in cf_dir:
                maybe_delete_sstable_links(generation, threshold)
