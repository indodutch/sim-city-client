'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
										 
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
import sys
import random
import util

def loadTokens(db):
	tokens = []

	for i in range(5):
		token = {
			'_id': 'token_' + str(i),
			'type': 'token',
			'lock': 0,
			'done': 0,
			'hostname': '',
			'scrub_count': 0,
			'input': random.random() * 10,
			'output': {}
		}
		tokens.append(token)
	db.update(tokens)

if __name__ == '__main__':
	config = util.Config('config.ini')
	#Create a connection to the server
	db = util.get_db(config.section('CouchDB'))
	#Load the tokens to the database
	loadTokens(db)
