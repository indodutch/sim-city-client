import couchdb
from ConfigParser import ConfigParser
import json
from os import listdir
from os.path import isfile, join, expanduser

class Config(object):
	def __init__(self, filename=None):
		if filename is None:
			filename = get_existing_path(["config.ini", ["..", "config.ini"], ["~", ".simcity_client"]])
		self.parser = ConfigParser()
		self.parser.read(filename)
	
	def section(self, name):
		return dict(self.parser.items(name))

def get_db(couch_cfg):
	server = couchdb.Server(couch_cfg['url'])
	server.resource.credentials = (couch_cfg['username'],couch_cfg['password'])
	return server[couch_cfg['database']]

def get_existing_path(fnames):
	for fname in fnames:
		if type(fname) is list:
			fname = join(*fname)	
		fname = expanduser(path)
		if isfile(fname):
			return fname
	return None

def write_json(fname, obj):
	with open(fname, 'w') as outfile:
	    json.dump(obj, outfile)
		
def listfiles(mypath):
	return [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]