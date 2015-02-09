from couchdb.http import ResourceConflict
from simcity_client.document import Document
from simcity_client.util import seconds

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

def start_job(job_id, database):
    try:
        doc = database.get(job_id)
    except ValueError: # job ID was not yet added to database
        doc = {'_id': job_id}

    try:
        return database.save(Job(doc).start())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return start_job(job_id, database)
    
def finish_job(job, database):
    try:
        job = database.save(job.finish())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script after starting.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return finish_job(Job(database.get(job.id)), database)
    else:
        if job['queue'] > 0:
            archive_job(job, database)

def queue_job(job, database, method, host = None):
    try:
        return database.save(job.queue(method, host))
    except ResourceConflict:
        return queue_job(Job(database.get(job.id)), database, method)
    else:
        if job['done'] > 0:
            archive_job(job, database)

def archive_job(job, database):
    database.delete(job)
    return database.save(job.archive())
