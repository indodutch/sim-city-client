'''
PiCaS client to run commands
'''
#python imports
import simcity_client
from simcity_client.execute import ExecuteActor
import sys

if __name__ == '__main__':
    simcity = simcity_client.init()

    if len(sys.argv) > 1:
        job_id = sys.argv[1]
    elif 'job_id' in simcity:
        job_id = simcity['job_id']
    else:
        raise EnvironmentError('Job ID not defined')
        
    actor = ExecuteActor(simcity['database'], simcity['config'], job_id)

    # Start work!
    print "Connected to the database %s sucessfully. Now starting work..." %(simcity['config'].section('CouchDB')['database'])
    actor.run()
    print "No more tokens to process, done."
