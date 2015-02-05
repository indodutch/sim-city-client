from couchdb.http import ResourceConflict
from simcity_client.document import Job
from simcity_client.util import seconds

def start_job(job_id, database):
    try:
        job = database.get_job(job_id)
    except ValueError:
        job = Job({'_id': job_id})

    try:
        return database.save(job.start())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return start_job()
    
def finish_job(job, database):
    try:
        job = database.save(job.finish())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script after starting.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return finish_job(database.get_job(job.id), database)
    else:
        if job['queue'] > 0:
            archive_job(job, database)

def queue_job(job, database, host = None):
    try:
        return database.save(job.queue(host))
    except ResourceConflict:
        return queue_job(database.get_job(job.id), database)
    else:
        if job['done'] > 0:
            archive_job(job, database)

def archive_job(job, database):
    database.delete(job)
    job['_id'] = 'archived-job-' + job.id + '-' + str(seconds())
    del job['_rev']
    return database.save(job)
