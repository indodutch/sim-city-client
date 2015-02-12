'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTasks.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tasks with basic fields and a random number for the input field
'''
import simcity
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="start a job")
    parser.add_argument('host', help="host to run pilot job on")
    parser.add_argument('-m', '--max', help="only run if there are less than MAX jobs running", default=2)
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args()

    simcity.init(configfile=args.config)
    
    job = simcity.job.submit_if_needed(args.host, args.max)
    if job is None:
        print "No tasks to process or already " + args.max " jobs running (increase maximum number of jobs with -m)"
    else:
        print "Job " + job['batch_id'] + " (ID: " + job.id + ") started"
