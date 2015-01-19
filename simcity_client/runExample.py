'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
										 
usage: python runExample.py [picas_db_name] [picas_username] [picas_pwd]	
description: iterate over the 'todo' view and process the tokens one by one. Return the square of input value in the output field.
'''
#python imports
import sys
import util

#picas imports
from picas.actors import RunActor
from picas.clients import CouchClient
from picas.iterators import BasicViewIterator
from picas.modifiers import BasicTokenModifier

class ExampleActor(RunActor):
	def __init__(self, iterator, modifier):
		self.iterator = iterator
		self.modifier = modifier
		self.client = iterator.client

	# Overwrite this method to process your work
	def process_token(self, key, token):
		# Print token information
		print "-----------------------"
		print "Working on token: " + token['_id']
		for k, v in token.iteritems():
			print k, v
		print "-----------------------"
		token['output'] = token['input']**2
		
		token = self.modifier.close(token)
		self.client.db[token['_id']] = token

def main():
	config = util.Config('config.ini')
	couch_cfg = config.section('CouchDB')
	
	# setup connection to db
	client = CouchClient(url=couch_cfg['url'], db=couch_cfg['database'], username=couch_cfg['username'], password=couch_cfg['password'])
	# Create token modifier
	modifier = BasicTokenModifier()
	# Create iterator, point to the right todo view
	iterator = BasicViewIterator(client, "Monitor/todo", modifier)
	# Create actor
	actor = ExampleActor(iterator, modifier)
	# Start work!
	print "Connected to the database %s sucessfully. Now starting work..." %(couch_cfg['database'])
	actor.run()

if __name__ == '__main__':
	main()