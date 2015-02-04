import socket
from simcity_client.util import merge_dicts, seconds
import traceback
import base64
import mimetypes
from uuid import uuid4

class Document(object):
    def __init__(self, data = {}, base = {}):
        if '_rev' not in data:
            data = merge_dicts(base, data)

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
    
    def put_attachment(self, name, data, mimetype=None):
        if '_attachments' not in self.doc:
            self.doc['_attachments'] = {}
        
        if mimetype is None:
            mimetype, encoding = mimetypes.guess_type(name)
            if mimetype is None:
                mimetype = 'text/plain'
        
        b64data = base64.b64encode(data)
        self.doc['_attachments'][name] = {'content_type': mimetype, 'data': b64data}
        return self
    
    def get_attachment(self, name):
        attachment = self.doc['_attachments'][name]
        attachment['data'] = base64.b64decode(attachment['data'])
        return attachment
    
    def remove_attachment(self, name):
        del self.doc['_attachments'][name]
        return self
        
    def _update_hostname(self):
        self.doc['hostname'] = socket.gethostname()

class Token(Document):
    __BASE = {
        'type': 'token',
        'lock': 0,
        'done': 0,
        'hostname': '',
        'scrub_count': 0,
        'input': {},
        'output': {},
        'error': []
    }

    """Class to manage token modifications with.
    """
    def __init__(self, token={}):
        super(Token, self).__init__(token, Token.__BASE)
        if '_id' not in self.doc:
            self.doc['_id'] = uuid4().hex

    def lock(self):
        """Function which modifies the token such that it is locked.
        """
        self._update_hostname()
        self.doc['lock'] = seconds()
        return self
    
    def unlock(self):
        """Reset the token to its unlocked state.
        """
        self._update_hostname()
        self.doc['lock'] = 0
        return self
    
    def mark_done(self):
        """Function which modifies the token such that it is closed for ever
        to the view that has supplied it.
        """
        self.doc['done'] = seconds()
        return self
    
    def unmark_done(self):
        """Reset the token to be fetched again.
        """
        self.doc['done'] = 0
        return self
    
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
        return self
    
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
        return self
    
    def get_errors(self):
        try:
            return self.doc['error']
        except:
            return []

class Job(Document):
    __BASE = {
        'type': 'job',
        'hostname': '',
        'start': 0,
        'done': 0,
        'queue': 0,
        'engine': ''
    }
    
    def __init__(self, job):
        if '_id' not in job:
            raise ValueError('Job ID must be set')
        
        super(Job, self).__init__(job, Job.__BASE)
    
    def queue(self, host):
        self.doc['hostname'] = host
        self.doc['queue'] = seconds()
        return self
    
    def start(self):
        self._update_hostname()
        self.doc['start'] = seconds()
        return self

    def finish(self):
        self.doc['done'] = seconds()
        return self
