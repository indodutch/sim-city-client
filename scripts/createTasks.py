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
    parser = argparse.ArgumentParser(description="create new tasks in the database")
    parser.add_argument('command', help="command to run")
    parser.add_argument('-n', '--number', type=int, help="number of tasks to create", default=1)
    parser.add_argument('-c', '--config', help="configuration file", default=None)
    args = parser.parse_args() 

    simcity.init(configfile=args.config)

    #Load the tasks to the database
    for i in xrange(args.number):
        try:
            simcity.task.add_task({'command': args.command})
            print "added task", i
        except:
            print "ERROR: task", i, "failed to be added"