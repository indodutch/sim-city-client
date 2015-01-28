'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
import random
import simcity_client
import sys
from simcity_client.token import Token

def createToken(i):
    return Token( {
        '_id': 'token_' + str(i),
        'input': {'a': random.random() * 10},
        'command': 'scripts/example_script.py'
    })

def loadTokens(db, start, stop):
    tokens = [createToken(i) for i in xrange(start, stop)]
    is_added = db.save_tokens(tokens)

    if all(is_added):
        print "Added", stop - start, "tokens"
    elif any(is_added):
        for i in xrange(len(tokens)):
            if not is_added[i]:
                print "ERROR: token", tokens[i].id, "failed to be added"
    else:
        print "ERROR: all tokens were already in the database"

if __name__ == '__main__':
    start = int(sys.argv[1])
    stop = int(sys.argv[2])
    _, db = simcity_client.init()

    #Load the tokens to the database
    loadTokens(db, start, stop)
