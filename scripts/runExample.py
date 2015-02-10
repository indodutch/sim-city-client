'''
PiCaS client to run commands
'''
#python imports
import simcity_client
from simcity_client.execute import ExecuteActor
import sys

if __name__ == '__main__':
    try:
        job_id = sys.argv[1]
    except:
        job_id = None

    simcity_client.init(job=job_id)
        
    actor = ExecuteActor()

    # Start work!
    print "Connected to the database sucessfully. Now starting work..."
    actor.run()
    print "No more tokens to process, done."
