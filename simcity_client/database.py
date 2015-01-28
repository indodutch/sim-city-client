"""
Created on Mon Jun  4 11:40:06 2012

@author: Jan Bot
"""
from couchdb.client import Server
from simcity_client.token import Token
import random
import couchdb
from couchdb.design import ViewDefinition
from couchdb.http import ResourceConflict
import numpy as np
from picas.iterators import ViewIterator
        
class CouchDB(object):
    """Client class to handle communication with the CouchDB back-end.
    """
    def __init__(self, url="http://localhost:5984", db="test",
            username="", password=""):
        """Create a CouchClient object. 
        :param url: the location where the CouchDB instance is located, 
        including the port at which it's listening. Default: http://localhost:5984
        :param db: the database to use. Default: test.
        """
        self.server = Server(url=url)
        if username == "":            
            self.db = self.server[db]
        else:
            self.db = couchdb.Database(url + "/" + db)
            self.db.resource.credentials = (username, password)
    
    def __getitem__(self, idx):
        return self.db[idx]
    
    def get_all_tokens(self, design_doc, view, **view_params):
        """
        Get tokens from the specified view.
        .. function:: get_all(viewLoc[, view_params={}])
        :param viewLoc: location of the view in 'design/view' notation.
        :param view_params: optional extra parameters for the view.
        :return: row list returned by the view.
        """
        view = self.view(design_doc, view, **view_params)
        return [self.get_token(row['key']) for row in view.rows]
    
    def get_token(self, id):
        return Token(self.db[id])
    
    def get_single_token(self, design_doc, view, window_size=1, **view_params):
        """Get a token from the specified view.
        :param view: the view to get the token from.
        :param view_params: the parameters that should be added to the view
        request. Optional.
        :return: a CouchDB token.
        """
        view = self.view(design_doc, view, limit=window_size, **view_params)
        row = random.choice(view.rows)
        return self.get_token(row['key'])
    
    def view(self, design_doc, view, **view_params):
        return self.db.view(design_doc + '/' + view, **view_params)
    
    def token_iterator(self, design_doc, view):
        return TokenViewIterator(self, design_doc, view)
    
    def save(self, doc):
        _, _rev = self.db.save(doc.value)
        doc['_rev'] = _rev
        return doc
    
    def save_tokens(self, tokens):
        """Save a sequence of Tokens to the database.
        
        - If the token was newly created and the _id is already is in the
          database the token will not be added.
        - If the token is an existing document, it will be updated if the _rev key
          matches.
        :param tokens [token1, token2, ...]; tokens for which the save was succesful will get new _rev values
        :return: a sequence of [succeeded1, succeeded2, ...] values.
        """
        updated = self.db.update([token.value for token in tokens])
        
        result = np.zeros(len(tokens), dtype=np.bool)
        for i in xrange(len(tokens)):
            is_added, _, _rev = updated[i]
            if is_added:
                tokens[i]['_rev'] = _rev
                result[i] = True
        
        return result

    def add_view(self, design_doc, view, map_fun, reduce_fun=None, *args, **kwargs):
        definition = ViewDefinition('Monitor', view, map_fun, reduce_fun, *args, **kwargs)
        definition.sync(self.db)

    def delete_tokens(self, tokens):
        result = np.ones(len(tokens),dtype=np.bool)
        for i, token in enumerate(tokens):
            try:
                self.db.delete(token.value)
            except ResourceConflict as ex:
                print "Could not delete token", token, "due to resource conflict:", ex
                result[i] = False
            except Exception as ex:
                print "Could not delete token ", token, ':', ex
                result[i] = False

        return result

    def delete_tokens_from_view(self, design_doc, view):
        tokens = self.get_all_tokens(design_doc, view)
        return self.delete_tokens(tokens)

class TokenViewIterator(ViewIterator):
    """Iterator object to fetch tokens while available.
    """
    def __init__(self, client, design_doc, view, **view_params):
        """
        @param client: CouchClient for handling the connection to the CouchDB
        server.
        @param view: CouchDB view from which to fetch the token.
        @param token_modifier: instance of a TokenModifier.
        @param view_params: parameters which need to be passed on to the view
        (optional).
        """
        super(TokenViewIterator, self).__init__(design_doc, view, **view_params)
        self.client = client
    
    def claim_token(self, allowed_failures=10):
        for _ in xrange(allowed_failures):
            try:
                token = self.client.get_single_token(self.design_doc, self.view, window_size=100, **self.view_params)
                token.lock()
                return self.client.save(token)
            except ResourceConflict:
                pass

        raise EnvironmentError("Unable to claim token.")
