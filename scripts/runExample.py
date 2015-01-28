'''
PiCaS client to run commands
'''
#python imports
import simcity_client
from simcity_client.execute import ExecuteActor

if __name__ == '__main__':
    config, db = simcity_client.init()
    actor = ExecuteActor(db, config)

    # Start work!
    print "Connected to the database %s sucessfully. Now starting work..." %(config.section('CouchDB')['database'])
    actor.run()
