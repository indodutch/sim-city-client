# SIM-CITY client
# 
# Copyright 2015 Joris Borgdorff <j.borgdorff@esciencecenter.nl>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
from simcity.util import merge_dicts
import mimetypes
import base64

class Document(object):
    def __init__(self, data = {}, base = {}):
        if isinstance(data, Document):
            data = data.value
        
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
            raise AttributeError("_id for document is not set")
    
    @property
    def rev(self):
        try:
            return self.doc['_rev']
        except KeyError:
            raise AttributeError("_rev is not available: document is not retrieved from database")

    @id.setter
    def id(self, new_id):
        self.doc['_id'] = new_id

    @property
    def value(self):
        return self.doc

    def update(self, values):
        """Add the output of the RunActor to the task.
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
        # Copy all attributes except data, it may be very large
        attachment = {}
        for key in self.doc['_attachments'][name]:
            if key != 'data':
                attachment[key] = self.doc['_attachments'][name][key]

        attachment['data'] = base64.b64decode(self.doc['_attachments'][name]['data'])
        return attachment
    
    def remove_attachment(self, name):
        del self.doc['_attachments'][name]
        return self
        
    def _update_hostname(self):
        self.doc['hostname'] = socket.gethostname()
