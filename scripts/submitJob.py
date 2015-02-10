'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
import simcity_client
import argparse
import sys

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="start a job")
    parser.add_argument('command', help="command to run")
    parser.add_argument('host', help="host to run pilot job on")
    parser.add_argument('-m', '--max', help="only run if there are less than MAX jobs running", default=2)
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args() 

    simcity_client.init(configfile=args.config)
    try:
        token = simcity_client.add_token({'command': args.command})
        print "token", token.id, "added to the database"
    except Exception as ex:
        print "Token could not be added to the database:", ex
        sys.exit(1)

    job = simcity_client.start_job_if_needed(args.host, args.max)
    if job is None:
        print "Let job be processed by existing pilot-job scripts"
    else:
        print "Job " + job['batch_id'] + " (ID: " + job.id + ") will process token"
