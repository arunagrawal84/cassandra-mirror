from multiprocessing import Pipe
from multiprocessing import Process
import os

from .util import moving_temporary_file

def get_cassandra_classpath():
    cassandra_conf = os.environ.get('CASSANDRA_CONF', '/etc/cassandra')
    cassandra_home = os.environ.get('CASSANDRA_HOME', '/usr/share/cassandra')
    return [
        cassandra_conf,
        '{}/*'.format(cassandra_home),
        '{}/lib/*'.format(cassandra_home),
    ]

def _get_jnius():
    import jnius_config
    jnius_config.add_options(
        '-ea',
        '-Xrs',
        '-Xmx256M',
        '-Dlogback.configurationFile=logback-tools.xml',
    )
    jnius_config.set_classpath(*get_cassandra_classpath())
    import jnius
    return jnius

def return_to_pipe(f, p):
    def wrapper(*args, **kwargs):
        try:
            ret = f(*args, **kwargs)
            p.send(ret)
        finally:
            p.close()
    return wrapper

def in_another_process(f):
    def wrapper(*args, **kwargs):
        parent_conn, child_conn = Pipe()
        wrapped_f = return_to_pipe(f, child_conn)
        p = Process(target=wrapped_f, args=args, kwargs=kwargs)
        p.start()
        child_conn.close()
        ret = parent_conn.recv()
        p.join()
        return ret
    return wrapper

@in_another_process
def _get_identity():
    jnius = _get_jnius()
    NodeProbe = jnius.autoclass('org.apache.cassandra.tools.NodeProbe')
    probe = NodeProbe('127.0.0.1', 7199)
    host_id = probe.getLocalHostId()
    return host_id

def _get_identity_miss(identity_file):
    ret = _get_identity()
    with moving_temporary_file(identity_file) as f:
        f.write((ret + '\n').encode('ascii'))
        f.finalize()

def _get_identity_hit(identity_file):
    return identity_file.read().rstrip()

def get_identity(state_dir):
    identity_file = state_dir / 'identity'
    try:
        return _get_identity_hit(identity_file)
    except FileNotFoundError:
        pass

    _get_identity_miss(identity_file)
    return _get_identity_hit(identity_file)
