import couchdb
from ConfigParser import ConfigParser

class Config(object):
	def __init__(self, filename):
		self.parser = ConfigParser()
		self.parser.read(filename)
	
	def section(self, name):
		return dict(self.parser.items(name))

def get_db(couch_cfg):
	server = couchdb.Server(couch_cfg['url'])
	server.resource.credentials = (couch_cfg['username'],couch_cfg['password'])
	db = server[couch_cfg['database']]
	return db
