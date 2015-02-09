'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
import simcity_client
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="create new tokens in the database")
    parser.add_argument('command', help="command to run")
    parser.add_argument('-n', '--number', type=int, help="number of tokens to create", default=1)
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args() 

    db = simcity_client.init(configfile=args.config)['database']

    #Load the tokens to the database
    for i in xrange(args.number):
        try:
            simcity_client.add_token({'command': args.command}, db)
            print "added token", i
        except:
            print "ERROR: token", i, "failed to be added"