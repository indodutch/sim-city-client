import simcity
from simcity.job.submit import SSHSubmitter, OsmiumSubmitter
from simcity.job.document import Job
from couchdb.http import ResourceConflict

database = None
job_id = None

def get(job_id = None):
    simcity._check_init()
    if job_id is None:
        job_id = simcity.job.job_id
    return Job(database.get(job_id))

def submit_if_needed(hostname, max_jobs):
    num = simcity.overview_total()

    num_jobs = num['active_jobs'] + num['pending_jobs']
    if num_jobs <= num['todo'] and num_jobs < max_jobs:
        return submit(hostname)
    else:
        return None

def submit(hostname):
    simcity._check_init()

    host = hostname + '-host'
    try:
        host_cfg = simcity.config.section(host)
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

def start():
    simcity._check_init()
    try:
        job = get()
    except ValueError: # job ID was not yet added to database
        job = Job({'_id': job_id})

    try:
        return database.save(job.start())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return start()
    
def finish(job):
    simcity._check_init()
    try:
        job = database.save(job.finish())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script after starting.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return finish(get())
    else:
        if job['queue'] > 0:
            archive(job)

def queue(job, method, host = None):
    simcity._check_init()
    try:
        return database.save(job.queue(method, host))
    except ResourceConflict:
        return queue(get(job.id), method)
    else:
        if job['done'] > 0:
            archive(job)

def archive(job):
    simcity._check_init()
    try:
        print "Deleting original job description"
        database.delete(job)
    except ResourceConflict:
        print "Failed to archive jobs"
        return archive(get(job.id))
    else:
        print "Archiving job description"
        return database.save(job.archive())
