'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
import simcity_client
from simcity_client.token import Token
import argparse

def createToken(i, command):
    return Token( {
        '_id': 'token_' + str(i),
        'input': {},
        'command': command
    })

def loadTokens(db, command, start, stop):
    tokens = [createToken(i, command) for i in xrange(start, stop)]
    is_added = db.save_tokens(tokens)

    if all(is_added):
        print "Added", stop - start, "tokens"
    elif any(is_added):
        for i in xrange(len(tokens)):
            if not is_added[i]:
                print "ERROR: token", tokens[i].id, "failed to be added"
    else:
        print "ERROR: all token IDs were already in the database"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="create new tokens in the database")
    parser.add_argument('command', help="command to run")
    parser.add_argument('start', type=int, help="token ID to start at.")
    parser.add_argument('-n', '--number', type=int, help="number of tokens to create", default=1)
    args = parser.parse_args() 

    db = simcity_client.init()['database']

    #Load the tokens to the database
    loadTokens(simcity['database'], args.command, args.start, args.start + args.number)
