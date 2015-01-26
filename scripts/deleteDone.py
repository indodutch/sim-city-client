'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createTokens.py [picas_db_name] [picas_username] [picas_pwd]
description: create 5 tokens with basic fields and a random number for the input field
'''
import simcity_client

if __name__ == '__main__':
    config, db = simcity_client.init_couchdb()
    
    print db.delete_from_view('Monitor', 'done')
