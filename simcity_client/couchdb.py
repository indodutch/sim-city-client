from util import merge_dicts

class CouchDB(object):
    def __init__(self, couch_cfg):
    	self.server = couchdb.Server(couch_cfg['url'])
    	server.resource.credentials = (couch_cfg['username'],couch_cfg['password'])
    	self.database = server[couch_cfg['database']]

    def add_tokens(self, tokens):
        token_list = []
    
        base = {
    		'type': 'token',
    		'lock': 0,
    		'done': 0,
    		'hostname': '',
    		'scrub_count': 0,
    		'input': {},
    		'output': {}
    	}
    
        for name, token in enumerate(tokens):
            base['_id'] = 'token_' + str(name)
            token_list.append(merge_dicts(base, token))
    
        self.database.update(token_list)

    def add_view(self, view_definition):
        view_definition.sync(self.database)
