from simcity_client.util import Config
from simcity_client.database import CouchDB
from simcity_client.submit import SSHSubmitter, OsmiumSubmitter
from simcity_client.document import Token
from ConfigParser import NoSectionError
import os

database = None
config = None
_has_init = False
job_id = None

def init(configfile=None, job=None):
    global job_id, _has_init, database, config
    
    if job is not None:
        job_id = job
    elif 'SIMCITY_JOBID' in os.environ:
        job_id = os.environ['SIMCITY_JOBID']
    
    try:
        config = Config(configfile)
        couch_cfg = config.section('CouchDB')
        database = CouchDB(
                        url      = couch_cfg['url'],
                        db       = couch_cfg['database'],
                        username = couch_cfg['username'],
                        password = couch_cfg['password'])
    except NoSectionError:
        raise ValueError("Configuration file " + config.filename + " does not contain CouchDB section")
    except IOError as ex:
        raise IOError("Cannot establish connection with CouchDB <" + couch_cfg['url'] + ">: " + str(ex))

    _has_init = True
    
def _check_init():
    if not _has_init:
        raise EnvironmentError("simcity_client.init() not called yet")
    
def start_job_if_needed(hostname, max_jobs):
    _check_init()
    
    num = overview_total()

    num_jobs = num['active_jobs'] + num['pending_jobs']
    if num_jobs <= num['todo'] and num_jobs < max_jobs:
        return _start_job(hostname)
    else:
        return None

def _start_job(hostname):
    _check_init()
    
    host = hostname + '-host'
    try:
        host_cfg = config.section(host)
    except:
        raise ValueError(hostname + ' not configured under ' + host + 'section')
    
    try:
        if host_cfg['method'] == 'ssh':
            submitter = SSHSubmitter(
                            database = database,
                            host     = host_cfg['host'],
                            jobdir   = host_cfg['path'],
                            prefix   = hostname + '-')
        elif host_cfg['method'] == 'osmium':
            submitter = OsmiumSubmitter(
                            database = database,
                            port     = host_cfg['port'],
                            jobdir   = host_cfg['path'],
                            prefix   = hostname + '-')
        else:
            raise EnvironmentError('Connection method for ' + hostname + ' unknown')
        
        script = [host_cfg['script']]
    except KeyError:
        raise EnvironmentError('Connection method for ' + hostname + ' not well configured')

    return submitter.submit(script)
    
def add_token(properties):
    _check_init()
    
    t = Token(properties)
    return database.save(t)

def overview_total():
    _check_init()
    
    overview = database.view('overview_total', group=True)

    views = ['todo', 'locked', 'error', 'done', 'finished_jobs', 'active_jobs', 'pending_jobs']
    num = {view: 0 for view in views}

    for view in overview:
        num[view.key] = view.value

    return num
