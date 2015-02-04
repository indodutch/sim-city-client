from couchdb.http import ResourceConflict
from simcity_client.document import Job

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
        return database.save(job.finish())
    # Check for concurrent modification: the job may be added to the
    # database by the submission script after starting.
    # Since this happens only once, we don't risk unlimited recursion
    except ResourceConflict:
        return finish_job(database.get_job(job.id), database)

def queue_job(job, host, database):
    try:
        return database.save(job.queue(host))
    except ResourceConflict:
        return queue_job(database.get_job(job.id), host, database)
