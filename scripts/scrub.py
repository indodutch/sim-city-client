import simcity, simcity.task, simcity.job
from simcity import job, task
import argparse
import time
import traceback

if __name__ == '__main__':
    task_views = ['locked', 'error']
    job_views = ['pending_jobs', 'active_jobs', 'finished_jobs']
    parser = argparse.ArgumentParser(description="Make old locked tasks available for processing again (default: all)")
    parser.add_argument('-D', '--days', type=int, help="number of days ago the task was locked", default=0)
    parser.add_argument('-H', '--hours', type=int, help="number of hours ago the task was locked", default=0)
    parser.add_argument('-S', '--seconds', type=int, help="number of seconds ago the task was locked", default=0)
    parser.add_argument('-V', '--view', choices=task_views + job_views, default='locked', help="view to scrub")
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args()
    
    arg_t = 60*((24*args.days) + args.hours) + args.seconds
    min_t = int( time.time() ) - arg_t
    
    simcity.init(configfile=args.config)

    if args.view in task_views:
        db = task.database
        update = []
        for row in db.view(args.view):
            if arg_t <= 0 or row.value['lock'] < min_t:
                task = task.get(row.id)
                update.append(task.scrub())

        if len(update) > 0:
            db.save_documents(update)
            print "Scrubbed", len(update), "task(s)"
        else:
            print "No scrubbing required"

    else:
        count = 0
        total = 0
        for row in job.database.view(args.view):
            total += 1
            if arg_t <= 0 or row.value['queue'] < min_t:
                try:
                    job.archive(job.get(row.id))
                    count += 1
                except Exception as ex:
                    print "Failed to archive job", row.id, "-", type(ex), ":", str(ex), "...", traceback.format_exc(ex)
        if total > 0:
            print "Scrubbed", count, "out of", total, "jobs"
        else:
            print "No scrubbing required"
