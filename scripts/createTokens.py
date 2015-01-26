'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
import random
from simcity_client import util
from simcity_client.couchdb import CouchDB

def loadTokens(db):
    tokens = {(str(i), {'input': random.random() * 10}) for i in xrange(5)}
    db.add_tokens(tokens)

if __name__ == '__main__':
    config = util.Config('config.ini')
    #Create a connection to the server
    db = CouchDB(config.section('CouchDB'))
    #Load the tokens to the database
    loadTokens(db)
