import socket
from simcity_client.util import merge_dicts, seconds
import traceback
    
class Document(object):
    def __init__(self, data):
        self.doc = data
    
    def __getitem__(self, idx):
        return self.doc.__getitem__(idx)

    def __setitem__(self, idx, value):
        return self.doc.__setitem__(idx, value)
    
    @property
    def id(self):
        try:
            return self.doc['_id']
        except KeyError:
            raise KeyError("_id for document is not set")
        
    @property
    def value(self):
        return self.doc

    def update(self, values):
        """Add the output of the RunActor to the token.
        """
        self.doc.update(values)
    
_token_base = {
    'type': 'token',
    'lock': 0,
    'done': 0,
    'hostname': '',
    'scrub_count': 0,
    'input': {},
    'output': {},
    'error': []
}

class Token(Document):
    """Class to manage token modifications with.
    """
    def __init__(self, token):
        global _token_base
        
        if '_rev' not in token:
            token = merge_dicts(_token_base, token)

        super(Token, self).__init__(token)

    def lock(self):
        """Function which modifies the token such that it is locked.
        """
        self.doc['hostname'] = socket.gethostname()
        self.doc['lock']     = seconds()
    
    def unlock(self):
        """Reset the token to its unlocked state.
        """
        self.doc['hostname'] = socket.gethostname()
        self.doc['lock']     = 0
    
    def mark_done(self):
        """Function which modifies the token such that it is closed for ever
        to the view that has supplied it.
        """
        self.doc['done'] = seconds()
    
    def unmark_done(self):
        """Reset the token to be fetched again.
        """
        self.doc['done'] = 0
    
    @property
    def input(self):
        """ Get input """
        return self.doc['input']
    
    @input.setter
    def input(self, value):
        """ Set input """
        self.doc['input'] = value
    
    @property
    def output(self):
        """Get the output from the RunActor."""
        return self.doc['output']

    @output.setter
    def output(self, output):
        """Add the input for the RunActor to the token.
        """
        self.doc['output'] = output
    
    def scrub(self):
        """
        Token scrubber: makes sure a token can be handed out again if it was locked
        more than t seconds ago.
        """
        if 'scrub_count' not in self.doc:
            self.doc['scrub_count'] = 0
        self.doc['scrub_count'] += 1
        self.unlock()
        self.unmark_done()
    
    def error(self, msg = None, exception=None):
        error = {'time': seconds()}
        if msg is not None:
            error['message'] = str(msg)

        if exception is not None:
            error['exception'] = traceback.format_exc()
            
        self.doc['lock'] = -1
        self.doc['done'] = -1
        if 'error' not in self.doc:
            self.doc['error'] = []
        self.doc['error'].append(error)
    
    def get_errors(self):
        try:
            return self.doc['error']
        except:
            return []
