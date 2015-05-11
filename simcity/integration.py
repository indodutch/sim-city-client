from .management import get_task_database, get_job_database
from .task import add_task, get_task
from .submit import submit_if_needed


def run_task(self, task_properties, host, max_jobs, polling_time=None):
    task = add_task(task_properties)
    job = submit_if_needed(host, max_jobs)

    if polling_time is not None:
        while task['done'] == 0:
            time.sleep(polling_time)
            task = get_task(task.id)

    return (task, job)


def overview_total():
    """
    Overview of all tasks and jobs.

    Returns a dict with the numbers of each type of job and task.
    """
    views = ['todo', 'locked', 'error', 'done',
             'finished_jobs', 'active_jobs', 'pending_jobs']
    num = dict((view, 0) for view in views)

    for view in get_task_database().view('overview_total', group=True):
        num[view.key] = view.value

    if get_job_database() is not get_task_database():
        for view in get_job_database().view('overview_total', group=True):
            num[view.key] = view.value

    return num