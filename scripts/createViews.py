'''
Created on 22 Dec 2014

@author: Anatoli Danezi <anatoli.danezi@surfsara.nl>
@helpdesk: Grid Services <grid.support@surfsara.nl>
                                         
usage: python createViews.py [picas_db_name] [picas_username] [picas_pwd]   
description: create the following Views in [picas_db_name]:                     
    todo View : lock_timestamp == 0 && done_timestamp == 0              
    locked View : lock_timestamp > 0 && done_timestamp == 0             
    done View : lock_timestamp > 0 && done _timestamp > 0    
    overview_total View : sum tokens per View (Map/Reduce)  
'''
from couchdb.design import ViewDefinition
import pystache
import simcity_client

def createViews(db):
    generalMapTemplate = '''
function(doc) {
  if(doc.type == "token") {
    if({{condition}}) {
      emit(doc._id, doc._id);
    }
  }
}
    '''
    overviewMapTemplate='''
function(doc) {
  if(doc.type == "token") {
  {{#views}}
    if ({{condition}}) {
      emit({{name}}, 1);
    }
  {{/views}}
  }
}
    '''
    overviewReduceCode='''
function (key, values, rereduce) {
   return sum(values);
}
    '''
    
    views = {
        'todo':   'doc.lock == 0 && doc.done == 0',
        'locked': 'doc.lock > 0  && doc.done == 0',
        'done':   'doc.lock > 0  && doc.done > 0'
    }
    pystache_views = {'views': [{'name': view, 'condition': condition} for view, condition in views.iteritems()]}

    for view in pystache_views['views']:
        mapCode = pystache.render(generalMapTemplate, view)
        db.add_view(ViewDefinition('Monitor', view['name'], mapCode))
    
    # overview_total View -- lists all views and the number of tokens in each view
    overviewMapCode = pystache.render(overviewMapTemplate, pystache_views)
    db.add_view(ViewDefinition('Monitor', 'overview_total', overviewMapCode, overviewReduceCode))

if __name__ == '__main__':
    config, db = simcity_client.init_couchdb()

    #Create the Views in database
    createViews(db)
