import simcity
import simcity.task, simcity.job

config = None
is_initialized = False
__is_initializing = True

def _check_init():
    if not is_initialized:
        raise EnvironmentError("Databases are not initialized yet, please provide a valid configuration file to simcity.init()")

def init(configfile):
    global is_initialized, config
    
    try:
        config = simcity.util.Config(configfile)
    except:
        # default initialization may fail
        if not __is_initializing:
            raise
    else:
        try:
            simcity.task.database = simcity.database._load('task-db')
        except:
            if not __is_initializing:
                raise
        
        try:
            simcity.job.database = simcity.database._load('job-db')
        except EnvironmentError:
            # job database not explicitly configured
            simcity.job.database = simcity.task.database
        except:
            if not __is_initializing:
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


init(None)
__is_initializing = False
