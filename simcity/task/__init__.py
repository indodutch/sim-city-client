import simcity
from .document import Task
from .iterator import TaskViewIterator

database = None

def add(properties):
    simcity._check_init()
    t = Task(properties)
    return database.save(t)

def get(task_id):
    simcity._check_init()
    return Task(database.get(task_id))
