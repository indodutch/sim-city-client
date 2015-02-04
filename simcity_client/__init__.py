from simcity_client import util
from simcity_client import database
from ConfigParser import NoSectionError
import os

def init(job_id = None):
    try:
        if job_id is None:
            for key in os.environ:
                if "JOBID" in key or "JOB_ID" in key:
                    job_id = os.environ[key]
                    break
            else:
                raise EnvironmentError("Job ID is not defined in environment")                
        
        config = util.Config()
        couch_cfg = config.section('CouchDB')
        client = database.CouchDB(url=couch_cfg['url'], db=couch_cfg['database'], username=couch_cfg['username'], password=couch_cfg['password'])
        return {'config': config, 'database': client, 'job_id': job_id}
    except NoSectionError:
        raise ValueError("Configuration file " + config.filename + "does not contain CouchDB section")
    except IOError as ex:
        raise IOError("Cannot establish connection with CouchDB: " + ex)
