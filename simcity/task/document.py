from simcity.document import Document
from simcity.util import seconds
import traceback
from uuid import uuid4

class Task(Document):
    __BASE = {
        'type': 'task',
        'lock': 0,
        'done': 0,
        'hostname': '',
        'scrub_count': 0,
        'input': {},
        'output': {},
        'error': []
    }

    """Class to manage task modifications with.
    """
    def __init__(self, task={}):
        super(Task, self).__init__(task, Task.__BASE)
        if '_id' not in self.doc:
            self.doc['_id'] = 'task_' + uuid4().hex

    def lock(self):
        """Function which modifies the task such that it is locked.
        """
        self._update_hostname()
        self.doc['lock'] = seconds()
        return self
    
    def done(self):
        """Function which modifies the task such that it is closed for ever
        to the view that has supplied it.
        """
        self.doc['done'] = seconds()
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
        """Add the input for the RunActor to the task.
        """
        self.doc['output'] = output
    
    def scrub(self):
        """
        Task scrubber: makes sure a task can be handed out again if it was locked
        more than t seconds ago.
        """
        if 'scrub_count' not in self.doc:
            self.doc['scrub_count'] = 0
        self.doc['scrub_count'] += 1
        self.doc['done'] = 0
        self.doc['lock'] = 0
        self._update_hostname()
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
