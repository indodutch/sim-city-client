'''
PiCaS client to run commands
'''
#python imports
from simcity_client import util
from simcity_client import execute

def main():
    config = util.Config()
    actor = execute.create_actor(config)

    # Start work!
    print "Connected to the database %s sucessfully. Now starting work..." %(config.section('CouchDB')['database'])
    actor.run()

if __name__ == '__main__':
    main()
