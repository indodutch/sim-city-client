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
        simcity_client.add_token({'command': args.command}, simcity['database'])
    except Exception as ex:
        print "Token could not be added to the database:", ex
        sys.exit(1)

    overview = simcity['database'].view('overview_total')
    active_jobs = overview['active_jobs']['value']
    todo = overview['todo']['value']
    if active_jobs <= todo and active_jobs < 2:
        try:
            simcity_client.submit_job(args.host, simcity['database'], simcity['config'])
        except Exception as ex:
            print "Pilot job framework could not be started:", ex
