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
import pystache
import simcity_client

def createViews(db):
    generalMapTemplate = '''
function(doc) {
  if(doc.type == "token" && {{condition}}) {
    emit(doc._id, { 'lock': doc.lock, 'done': doc.done });
  }
}
    '''
    erroneousMapCode = '''
function(doc) {
    if (doc.type == "token" && doc.done == -1) {
        emit(doc._id, doc.error)
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
    renderer = pystache.renderer.Renderer(escape=lambda u: u)

    for view in pystache_views['views']:
        mapCode = renderer.render(generalMapTemplate, view)
        db.add_view(view['name'], mapCode)
    
    db.add_view('error', erroneousMapCode)
    
    # overview_total View -- lists all views and the number of tokens in each view
    overviewMapCode = renderer.render(overviewMapTemplate, pystache_views)
    db.add_view('overview_total', overviewMapCode, overviewReduceCode)

if __name__ == '__main__':
    _, db = simcity_client.init()

    #Create the Views in database
    createViews(db)
