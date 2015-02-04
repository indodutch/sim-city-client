from simcity_client import util
from simcity_client import database
from ConfigParser import NoSectionError
import os

def init():
    try:
        result = {}

        if 'SIMCITY_JOBID' in os.environ:
            result['job_id'] = os.environ['SIMCITY_JOBID']
        
        result['config'] = util.Config()
        couch_cfg = result['config'].section('CouchDB')
        result['database'] = database.CouchDB(
                                        url=couch_cfg['url'],
                                        db=couch_cfg['database'],
                                        username=couch_cfg['username'],
                                        password=couch_cfg['password'])
        return result
    except NoSectionError:
        raise ValueError("Configuration file " + result['config'].filename + "does not contain CouchDB section")
    except IOError as ex:
        raise IOError("Cannot establish connection with CouchDB: " + ex)
