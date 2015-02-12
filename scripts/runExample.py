'''
PiCaS client to run commands
'''
#python imports
import simcity
from simcity.job.execute import ExecuteActor
import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        simcity.job.job_id = sys.argv[1]
    
    if simcity.job.job_id is None:
        print "Job ID cannot be determined"
        sys.exit(1)
        
    actor = ExecuteActor()

    # Start work!
    print "Connected to the database sucessfully. Now starting work..."
    actor.run()
    print "No more tasks to process, done."
