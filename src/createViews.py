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
import sys
import couchdb
from couchdb.design import ViewDefinition
import util

def createViews(db):
	generalViewCode='''
function(doc) {
   if(doc.type == "token") {
	if(%s) {
	  emit(doc._id, doc._id);
	}
  }
}
'''
	# todo View
	todoCondition = 'doc.lock == 0 && doc.done == 0'
	todo_view = ViewDefinition('Monitor', 'todo', generalViewCode %(todoCondition))
	todo_view.sync(db)
	# locked View
	lockedCondition = 'doc.lock > 0 && doc.done == 0'
	locked_view = ViewDefinition('Monitor', 'locked', generalViewCode %(lockedCondition))
	locked_view.sync(db)
	# done View
	doneCondition = 'doc.lock > 0 && doc.done > 0'
	done_view = ViewDefinition('Monitor', 'done', generalViewCode %(doneCondition))
	done_view.sync(db)
	# overview_total View -- lists all views and the number of tokens in each view
	overviewMapCode='''
function(doc) {
   if(doc.type == "token") {
	   if (doc.lock == 0 && doc.done == 0){
		  emit('todo', 1);
	   }
	   if(doc.lock > 0 && doc.done == 0) {
		  emit('locked', 1);
	   }
	   if(doc.lock > 0 && doc.done > 0) {
		  emit('done', 1);
	   }
   }
}
'''
	overviewReduceCode='''
function (key, values, rereduce) {
   return sum(values);
}
'''
	overview_total_view = ViewDefinition('Monitor', 'overview_total', overviewMapCode, overviewReduceCode)
	overview_total_view.sync(db)

if __name__ == '__main__':
	config = util.Config('config.ini')
	#Create a connection to the server
	db = util.get_db(config.section('CouchDB'))
	#Create the Views in database
	createViews(db)
