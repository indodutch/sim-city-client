from simcity.util import Config
from simcity.database import CouchDB
from ConfigParser import NoSectionError
import os, simcity.task, simcity.job

config = None
is_initialized = False

def _check_init():
    if not is_initialized:
        raise EnvironmentError("Databases are not initialized yet, please provide a valid configuration file to simcity.init()")

def _load_database(name):
    try:
        cfg = config.section(name)
    except NoSectionError:
        raise ValueError("Configuration file " + config.filename + " does not contain '" + name + "' section")

    try:
        return CouchDB(
                url      = cfg['url'],
                db       = cfg['database'],
                username = cfg['username'],
                password = cfg['password'])
    except IOError as ex:
        raise IOError("Cannot establish connection with " + name + " CouchDB <" + cfg['url'] + ">: " + str(ex))
    

def init(configfile=None):
    global is_initialized, config
    
    if simcity.job.job_id is None and 'SIMCITY_JOBID' in os.environ:
        simcity.job.job_id = os.environ['SIMCITY_JOBID']
    
    try:
        config = Config(configfile)
    except:
        # default initialization may fail
        if configfile is not Config.DEFAULT_FILENAMES:
            raise
    else:
        try:
            simcity.task.database = _load_database('task-db')
        except:
            if configfile is not Config.DEFAULT_FILENAMES:
                raise
        
        try:
            simcity.job.database = _load_database('job-db')
        except NoSectionError:
            simcity.job.database = simcity.task.database
        except:
            if configfile is not Config.DEFAULT_FILENAMES:
                raise
        
        is_initialized = True

def overview_total():
    _check_init()
    
    views = ['todo', 'locked', 'error', 'done', 'finished_jobs', 'active_jobs', 'pending_jobs']
    num = {view: 0 for view in views}

    for view in simcity.task.database.view('overview_total', group=True):
        num[view.key] = view.value

    if simcity.job.database is not simcity.task.database:
        for view in simcity.job.database.view('overview_total', group=True):
            num[view.key] = view.value
    
    return num

init(configfile=Config.DEFAULT_FILENAMES)