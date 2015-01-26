from simcity_client.util import merge_dicts
from couchdb.client import Server
import numpy as np

class CouchDB(object):
    def __init__(self, couch_cfg):
        server = Server(couch_cfg['url'])
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
        
        for _id, token in tokens.iteritems():
            base['_id'] = _id
            token_list.append(merge_dicts(base, token))
        
        updateValue = self.database.update(token_list)
        return {_id: is_added for is_added, _id, _rev in updateValue}

    def add_view(self, view_definition):
        view_definition.sync(self.database)

    def view(self, design_doc, view):
        return self.database.view(design_doc + '/' + view)
        
    def delete(self, tokens):
        result = np.ones(len(tokens),dtype=np.bool)
        for i, token in enumerate(tokens):
            try:
                self.database.delete(token)
            except Exception as ex:
                print "Could not delete token ", token['_id'], ':', ex
                result[i] = False

        return result
    
    def delete_from_view(self, design_doc, view):
        return self.delete(self.view(design_doc, view))