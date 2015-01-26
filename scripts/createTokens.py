'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
import random
from simcity_client import util
from simcity_client.database import CouchDB

def loadTokens(db):
    tokens = { 'token_' + str(i): {'input': {'a': random.random() * 10}, 'command': 'scripts/example_script.py'} for i in xrange(5, 10) }
    is_added = db.add_tokens(tokens)
    if any(is_added.values()):
        for _id in is_added:
            if not is_added[_id]:
                print "ERROR:", _id, "failed to be updated, it was already in the database"
    else:
        print "ERROR: all tokens were already in the database"

if __name__ == '__main__':
    # try:
        config = util.Config()
        #Create a connection to the server
        db = CouchDB(config.section('CouchDB'))
        #Load the tokens to the database
        loadTokens(db)
    # except Exception as ex:
    #     print "configuration file is not valid: ", ex
