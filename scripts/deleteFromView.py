'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTasks.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tasks with basic fields and a random number for the input field
'''
import simcity
import argparse
import numpy as np

if __name__ == '__main__':
    task_views = ['todo', 'done', 'locked']
    job_views = ['pending_jobs', 'active_jobs', 'finished_jobs', 'archived_jobs']
    parser = argparse.ArgumentParser(description="Remove all tasks in a view")
    parser.add_argument('view', choices=task_views + job_views, help="View to remove documents from")
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args()
    
    simcity.init(configfile=args.config)

    if args.view in task_views:
        db = simcity.task.database
    else:
        db = simcity.job.database

    is_deleted = db.delete_from_view(args.view)
    print "Deleted", np.sum(is_deleted), "out of", len(is_deleted), "tasks from view", args.view
