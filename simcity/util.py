from ConfigParser import ConfigParser
import json
import os
import time
import glob
import shutil
from copy import deepcopy 

class Config(object):
    DEFAULT_FILENAMES = ["config.ini", ("..", "config.ini"), ("~", ".simcity_client")]
    def __init__(self, filenames=None):
        if filenames is None:
            filenames = Config.DEFAULT_FILENAMES
        
        exp_filenames = expandfilenames(filenames)

        self.parser = ConfigParser()
        self.filename = self.parser.read(exp_filenames)
        if len(self.filename) == 0:
            raise ValueError("No valid configuration files could be found: tried " + str(exp_filenames))
    
    def section(self, name):
        return dict(self.parser.items(name))

def issequence(obj):
    return isinstance(obj, (list, tuple))
    
def expandfilename(filename):
    if issequence(filename):
        filename = os.path.join(*filename)    
    return os.path.expandvars(os.path.expanduser(filename))

def expandfilenames(filenames):
    if not issequence(filenames):
        filenames = [filenames]
    return [expandfilename(fname) for fname in filenames]

def write_json(fname, obj):
    with open(fname, 'w') as outfile:
        json.dump(obj, outfile)
        
def listfiles(mypath):
    return [ f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath,f)) ]

def listdirs(mypath):
    return [ d for d in os.listdir(mypath) if os.path.isdir(os.path.join(mypath,d)) ]
    
def merge_dicts(dict1, dict2):
    merge = deepcopy(dict1)
    merge.update(dict2)
    return merge

def seconds():
    return int( time.time() )

def copyglob(srcglob, dstdir, prefix=""):
    if not os.path.isdir(dstdir):
        raise ValueError("Destination of copyglob must be a directory")
    
    for src in glob.glob(expandfilename(srcglob)):
        _, fname = os.path.split(src)
        shutil.copyfile(src, os.path.join(dstdir, prefix + fname))

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
