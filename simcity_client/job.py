from couchdb.http import ResourceConflict
from simcity_client.document import Document
from uuid import uuid4
from simcity_client.util import seconds

class Job(Document):
    __BASE = {
        'type': 'job',
        'hostname': '',
        'start': 0,
        'done': 0,
        'queue': 0,
        'engine': ''
    }
    
    def __init__(self, job):
        if '_id' not in job:
            raise ValueError('Job ID must be set')
        
        super(Job, self).__init__(job, Job.__BASE)
    
    def queue(self, host = None):
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

def start_job(job_id, database):
    try:
        doc = database.get(job_id)
    except ValueError:
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

def queue_job(job, database, host = None):
    try:
        return database.save(job.queue(host))
    except ResourceConflict:
        return queue_job(Job(database.get(job.id)), database)
    else:
        if job['done'] > 0:
            archive_job(job, database)

def archive_job(job, database):
    database.delete(job)
    job['_id'] = 'archived-job-' + job.id + '-' + str(seconds())
    del job['_rev']
    return database.save(job)