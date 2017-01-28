from collections import namedtuple

def is_sstable_toc(path):
    split = path.basename.split('-', 3)
    is_tmp = len(split) >= 2 and split in ('tmp', 'tmplink')
    return (path.endswith('-TOC.txt')
        and not is_tmp
        and path.isfile() # not a link or a directory
    )

def stat_helper(path):
    """os.path.exists will return None for PermissionError (or any other
    exception) , leading us to believe a file is not present when it, in fact,
    is. This is behavior is awful, so stat_helper preserves any exception
    other than FileNotFoundError.
    """

    try:
        return path.stat()
    except FileNotFoundError:
        return None
