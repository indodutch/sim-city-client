import simcity_client
from simcity_client.document import Token
from simcity_client.job import Job, archive_job
import argparse
import time
import traceback

if __name__ == '__main__':
    token_views = ['locked', 'error']
    job_views = ['pending_jobs', 'active_jobs']
    parser = argparse.ArgumentParser(description="Make old locked tokens available for processing again (default: all)")
    parser.add_argument('-D', '--days', type=int, help="number of days ago the token was locked", default=0)
    parser.add_argument('-H', '--hours', type=int, help="number of hours ago the token was locked", default=0)
    parser.add_argument('-S', '--seconds', type=int, help="number of seconds ago the token was locked", default=0)
    parser.add_argument('-V', '--view', choices=token_views + job_views, default='locked', help="view to scrub")
    args = parser.parse_args()
    
    arg_t = 60*((24*args.days) + args.hours) + args.seconds
    min_t = int( time.time() ) - arg_t
    
    simcity_client.init()
    db = simcity_client.database

    if args.view in token_views:
        update = []
        for row in db.view(args.view):
            if arg_t <= 0 or row.value['lock'] < min_t:
                token = Token(db.get(row.id))
                update.append(token.scrub())

        if len(update) > 0:
            db.save_documents(update)
            print "Scrubbed", len(update), "token(s)"
        else:
            print "No scrubbing required"

    else:
        count = 0
        total = 0
        for row in db.view(args.view):
            total += 1
            if arg_t <= 0 or row.value['queue'] < min_t:
                try:
                    job = Job(db.get(row.id))
                    archive_job(job, db)
                    count += 1
                except Exception as ex:
                    print "Failed to archive job", row.id, "-", type(ex), ":", str(ex), "...", traceback.format_exc(ex)
        if total > 0:
            print "Scrubbed", count, "out of", total, "jobs"
        else:
            print "No scrubbing required"