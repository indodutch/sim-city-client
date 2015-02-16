import simcity
from simcity.document import Document
from simcity.util import seconds
from couchdb.http import ResourceConflict

class Job(Document):
    __BASE = {
        'type': 'job',
        'hostname': '',
        'start': 0,
        'done': 0,
        'queue': 0,
        'method': '',
        'archive': 0
    }
    
    def __init__(self, job):        
        super(Job, self).__init__(job, Job.__BASE)
        if '_id' not in self.doc:
            raise ValueError('Job ID must be set')
    
    def queue(self, method, host = None):
        self.doc['method'] = method
        if host is not None:
            self.doc['hostname'] = host
        self.doc['queue'] = seconds()
        return self
    
    def start(self):
        self._update_hostname()
        self.doc['start'] = seconds()
        return self

    def finish(self):
        self.doc['done'] = seconds()
        return self
    
    def archive(self):
        self.doc['archive'] = seconds()
        self.id = 'archived-' + self.id + '-' + str(seconds())
        del self.doc['_rev']
        return self

def get(job_id = None):
    simcity._check_init()
    if job_id is None:
        job_id = simcity.job.job_id
    if job_id is None:
        raise EnvironmentError("Job ID cannot be determined")
    return Job(simcity.job.database.get(job_id))

def start():
    simcity._check_init()
    try: # EnvironmentError if job_id cannot be determined falls through
        job = get()
    except ValueError: # job ID was not yet added to database
        job = Job({'_id': simcity.job.job_id})
    
    try:
        return simcity.job.database.save(job.start())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return start()
    
def finish(job):
    simcity._check_init()
    try:
        job = simcity.job.database.save(job.finish())
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
        return simcity.job.database.save(job.queue(method, host))
    except ResourceConflict:
        return queue(get(job.id), method)
    else:
        if job['done'] > 0:
            archive(job)

def archive(job):
    simcity._check_init()
    try:
        simcity.job.database.delete(job)
    except ResourceConflict:
        return archive(get(job.id))
    else:
        return simcity.job.database.save(job.archive())
