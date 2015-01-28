from ConfigParser import ConfigParser
import json
import os
import time

class Config(object):
    def __init__(self, filenames=["config.ini", ["..", "config.ini"], ["~", ".simcity_client"]]):
        exp_filenames = expandfilenames(filenames)

        self.parser = ConfigParser()
        self.filename = self.parser.read(exp_filenames)
        if len(self.filename) == 0:
            raise ValueError("No valid configuration files could be found: tried " + str(exp_filenames))
    
    def section(self, name):
        return dict(self.parser.items(name))

def expandfilenames(filenames):
    if type(filenames) is not list:
        filenames = [filenames]

    result = []
    for filename in filenames:
        if type(filename) is list:
            filename = os.path.join(*filename)    
        filename = os.path.expanduser(filename)
        result.append(filename)

    return result

def write_json(fname, obj):
    with open(fname, 'w') as outfile:
        json.dump(obj, outfile)
        
def listfiles(mypath):
    return [ f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath,f)) ]
    
def merge_dicts(dict1, dict2):
    merge = dict1.copy()
    merge.update(dict2)
    return merge

class Timer(object):
    def __init__(self):
        self.t = time.time()
    
    @property
    def elapsed(self):
        return time.time() - self.t
    
    def reset(self):
        new_t = time.time()
        diff = new_t - self.t
        self.t = new_t
        return diff
