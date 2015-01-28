"""
Provides access to a CouchDB database.
Originally used PiCaS, now all functionality directly accesses CouchDB.

Created on Mon Jun  4 11:40:06 2012
Updated Wed Jan 28 17:12 2015

@author: Jan Bot
@author: Joris Borgdorff
"""
from simcity_client.token import Token
import random
import numpy as np

# Couchdb imports
import couchdb
from couchdb.design import ViewDefinition
from couchdb.http import ResourceConflict
from couchdb.client import Server
        
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
        Get tokens from the specified view with token _id as key.
        :param design_doc: design document of the view
        :param view: name of the view, having the token _id as keys
        :param view_params: name of the view optional extra parameters for the view.
        :return: a list of Token objects in the view
        """
        view = self.view(design_doc, view, **view_params)
        return [self.get_token(row['key']) for row in view.rows]
    
    def get_token(self, id):
        """
        Get the token associated to the given ID
        :param id: _id string of the token
        :return: Token object with given id
        """
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
        """
        Get the data from a view
        
        :param design_doc: name of the design document
        :param view: name of the view
        :return: iterator over the view; the rows property contains the rows.
        """
        return self.db.view(design_doc + '/' + view, **view_params)
    
    def token_iterator(self, design_doc, view):
        """ Iterate over all tokens in a view.
        
        Gets one token at a time
        :param design_doc: name of the design document
        :param view: name of the view
        :return: iterator returning Token objects
        """
        return TokenViewIterator(self, design_doc, view)
    
    def save(self, doc):
        """ Save a Document to the database.
        
        Updates the document to have the new _rev value.
        :param doc: Document object
        """
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
        """ Add a view to the database
        All extra parameters are passed to couchdb.design.ViewDefinition
        :param design_doc: name of the design document
        :param view: name of the view
        :param map_fun: string of the javascript map function
        :param reduce_fun: string of the javascript reduce function (optional)
        """
        definition = ViewDefinition('Monitor', view, map_fun, reduce_fun, *args, **kwargs)
        definition.sync(self.db)

    def delete_tokens(self, tokens):
        """
        Delete a sequence of tokens from the database.
        
        The tokens must have a valid and current _id and _rev, so they must be
        retrieved from the database and not be altered there in the mean time.
        :param tokens: list of Token objects
        :return: array of booleans indicating whether the respective token was deleted.
        """
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
        """
        Delete all tokens in a view
        
        :param design_doc: name of the design document
        :param view: name of the view
        :return: array of booleans indicating whether the respective tokens were deleted
        """
        tokens = self.get_all_tokens(design_doc, view)
        return self.delete_tokens(tokens)

class ViewIterator(object):
    """Dummy class to show what to implement for a PICaS iterator.
    """
    def __init__(self, design_doc, view, **view_params):
        self._stop = False
        self.design_doc = design_doc
        self.view = view
        self.view_params = view_params
    
    def __repr__(self):
        return "<ViewIterator object>"
    
    def __str__(self):
        return "<view: " + self.design_doc + "/" + self.view + ">"
    
    def __iter__(self):
        """Python needs this."""
        return self
    
    def next(self):
        if self._stop:
            raise StopIteration
        
        try:
            return self.claim_token()
        except IndexError:
            self._stop = True
            raise StopIteration
    
    def claim_token(self, allowed_failures=10):
        """
        Get the first available token from a view.
        :param allowed_failures: the number of times a lock failure may
        occur before giving up. Default=10.
        """
        raise NotImplementedError("claim_token function not implemented.")

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
