from .document import Job, get, start, finish, queue, archive
from .submit import submit_if_needed
from .execute import ExecuteActor, RunActor
import os

database = None

try:
    job_id = os.environ['SIMCITY_JOBID']
except:
    job_id = None
