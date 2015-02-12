'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTasks.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tasks with basic fields and a random number for the input field
'''
import simcity_client
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="start a job")
    parser.add_argument('host', help="host to run pilot job on")
    parser.add_argument('-m', '--max', help="only run if there are less than MAX jobs running", default=2)
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args()

    simcity = simcity_client.init(configfile=args.config)
    
    simcity.start_job_if_necessary()

    num = simcity_client.overview_total(simcity['database'])
    print "Overview", num
    num_jobs = num['active_jobs'] + num['pending_jobs']
    if num_jobs <= num['todo'] and num_jobs < args.max:
        print "Starting new job"
        # try:
        job = simcity_client.start_job(args.host, simcity['database'], simcity['config'])
        print "Submitted job with id", job.id, "to", args.host
        # except Exception as ex:
        # print "Pilot job framework could not be started:", ex
    else:
        print "Let job be processed by existing pilot-job scripts"
