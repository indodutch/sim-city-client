'''
PiCaS client to run commands
'''
#python imports
import simcity_client
from simcity_client.execute import ExecuteActor

if __name__ == '__main__':
    simcity = simcity_client.init()
    actor = ExecuteActor(simcity['database'], simcity['config'], simcity['job_id'])

    # Start work!
    print "Connected to the database %s sucessfully. Now starting work..." %(config.section('CouchDB')['database'])
    actor.run()
    print "No more tokens to process, done."
