'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
from simcity_client import util
from simcity_client.database import CouchDB

if __name__ == '__main__':
    # try:
        config = util.Config()
        #Create a connection to the server
        db = CouchDB(config.section('CouchDB'))
        #Load the tokens to the database
        print db.delete_from_view('Monitor', 'done')

    # except Exception as ex:
    #     print "configuration file is not valid: ", ex
