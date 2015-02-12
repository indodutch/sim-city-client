import simcity
from simcity.task.document import Task

database = None

def add(properties):
    simcity._check_init()
    t = Task(properties)
    return database.save(t)

def get(task_id):
    simcity._check_init()
    return Task(database.get(task_id))
