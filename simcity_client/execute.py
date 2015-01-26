import os
from simcity_client.util import listfiles, write_json
from os.path import join, expandvars
from subprocess import call

#picas imports
from picas.actors import RunActor
from picas.clients import CouchClient
from picas.iterators import BasicViewIterator
from picas.modifiers import BasicTokenModifier

class ExecuteActor(RunActor):
    def __init__(self, iterator, modifier, config):
        self.iterator = iterator
        self.modifier = modifier
        self.client = iterator.client
        self.config = config
    
    # Overwrite this method to process your work
    def process_token(self, key, token):
        # Print token information
        print "-----------------------"
        print "Working on token: " + token['_id']

        dirs = self.create_dirs(token['_id'])
        params_file = join(dirs['input'], 'input.json')
        write_json(params_file, token['input'])
        
        token['execute_properties'] = {'dirs': dirs, 'input_file': params_file}
        
        command = [token['command'], dirs['tmp'], dirs['input'], dirs['output']]
        
        try:
            call(command)
        except Exception as e:
            token['error'] = str(e)
        
        out_files = listfiles(dirs['output'])
        token['output'] = {f: open(f, 'r').read() for f in out_files}
                
        token = self.modifier.close(token)
        self.client.db[token['_id']] = token
        print "-----------------------"
    
    def create_dirs(self, _id):
        dirs = {}
        dirs['tmp'] = join(expandvars(self.config['tmp_dir']), _id)
        dirs['input'] = join(expandvars(self.config['input_dir']), _id)
        dirs['output'] = join(expandvars(self.config['output_dir']), _id)
        for d in dirs.values():
            os.mkdir(d)
        
        return dirs

def create_actor(config):
    couch_cfg = config.section('CouchDB')
    
    # setup connection to db
    client = CouchClient(url=couch_cfg['url'], db=couch_cfg['database'], username=couch_cfg['username'], password=couch_cfg['password'])
    # Create token modifier
    modifier = BasicTokenModifier()
    # Create iterator, point to the right todo view
    iterator = BasicViewIterator(client, "Monitor/todo", modifier)
    # Create actor
    return ExecuteActor(iterator, modifier, config.section('Execution'))
