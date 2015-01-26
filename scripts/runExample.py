'''
PiCaS client to run commands
'''
#python imports
from simcity_client import util
from simcity_client.execute import ExecuteActor

#picas imports
from picas.clients import CouchClient
from picas.iterators import BasicViewIterator
from picas.modifiers import BasicTokenModifier

def main():
    config = util.Config()
    couch_cfg = config.section('CouchDB')
    
    # setup connection to db
    client = CouchClient(url=couch_cfg['url'], db=couch_cfg['database'], username=couch_cfg['username'], password=couch_cfg['password'])
    # Create token modifier
    modifier = BasicTokenModifier()
    # Create iterator, point to the right todo view
    iterator = BasicViewIterator(client, "Monitor/todo", modifier)
    # Create actor
    actor = ExecuteActor(iterator, modifier, config.section('Execution'))
    # Start work!
    print "Connected to the database %s sucessfully. Now starting work..." %(couch_cfg['database'])
    actor.run()

if __name__ == '__main__':
    main()