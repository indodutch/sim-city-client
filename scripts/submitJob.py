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
    args = parser.parse_args() 

    simcity = simcity_client.init()
    try:
        token = simcity_client.add_token({'command': args.command}, simcity['database'])
        print "token", token.id, "added to the database"
    except Exception as ex:
        print "Token could not be added to the database:", ex
        sys.exit(1)

    overview = simcity['database'].view('overview_total')
    
    num = {}
    for key in ['todo', 'locked', 'active_jobs', 'pending_jobs']:
        try:
            num[key] = list(overview[[key]])[0]['value']
        except IndexError:
            num[key] = 0

    num_jobs = num['active_jobs'] + num['pending_jobs']
    if num_jobs <= num['todo'] and num_jobs < 2:
        print "Starting new job"
        # try:
        job = simcity_client.start_job(args.host, simcity['database'], simcity['config'])
        print "Submitted job with id", job.id, "to", args.host
        # except Exception as ex:
        # print "Pilot job framework could not be started:", ex
    else:
        print "Let job be processed by existing pilot-job scripts"