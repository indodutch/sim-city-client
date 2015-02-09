from simcity_client import util
from simcity_client import database
from simcity_client import submit
from simcity_client.document import Token
from ConfigParser import NoSectionError
import os

def init(configfile=None):
    try:
        result = {}

        if 'SIMCITY_JOBID' in os.environ:
            result['job_id'] = os.environ['SIMCITY_JOBID']
        
        result['config'] = util.Config(configfile)
        couch_cfg = result['config'].section('CouchDB')
        result['database'] = database.CouchDB(
                                        url      = couch_cfg['url'],
                                        db       = couch_cfg['database'],
                                        username = couch_cfg['username'],
                                        password = couch_cfg['password'])
        return result
    except NoSectionError:
        raise ValueError("Configuration file " + result['config'].filename + "does not contain CouchDB section")
    except IOError as ex:
        raise IOError("Cannot establish connection with CouchDB <" + couch_cfg['url'] + ">: " + str(ex))

def start_job(hostname, database, config):
    host = hostname + '-host'
    host_cfg = config.section(host)
    if host_cfg['method'] == 'ssh':
        submitter = submit.SSHSubmitter(
                            database = database,
                            host     = host_cfg['host'],
                            jobdir   = host_cfg['path'],
                            prefix   = hostname + '-')
    elif host_cfg['method'] == 'osmium':
        submitter = submit.OsmiumSubmitter(
                            database = database,
                            port     = host_cfg['port'],
                            jobdir   = host_cfg['path'],
                            prefix   = hostname + '-')
    else:
        raise ValueError('Host ' + hostname + ' not configured under ' + host + 'section')
    
    return submitter.submit([host_cfg['script']])

def add_token(properties, database):
    t = Token(properties)
    return database.save(t)

def overview_total(database):
    overview = database.view('overview_total', group=True)

    views = ['todo', 'locked', 'error', 'done', 'finished_jobs', 'active_jobs', 'pending_jobs']
    num = {view: 0 for view in views}
    
    for view in overview:
        num[view.key] = view.value

    return num
