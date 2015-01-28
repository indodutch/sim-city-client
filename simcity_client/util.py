from ConfigParser import ConfigParser
import json
from os import listdir
from os.path import isfile, join, expanduser
import time

class Config(object):
    def __init__(self, filename=None):
        if filename is None:
            self.filename = existing_path_from_list(["config.ini", ["..", "config.ini"], ["~", ".simcity_client"]])
        else:
            self.filename = filename

        self.parser = ConfigParser()
        self.parser.read(self.filename)
    
    def section(self, name):
        return dict(self.parser.items(name))

def existing_path_from_list(fnames):
    for fname in fnames:
        if type(fname) is list:
            fname = join(*fname)    
        fname = expanduser(fname)
        if isfile(fname):
            return fname
    return None

def write_json(fname, obj):
    with open(fname, 'w') as outfile:
        json.dump(obj, outfile)
        
def listfiles(mypath):
    return [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
    
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
