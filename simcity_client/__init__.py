from simcity_client import util
from simcity_client import database
from ConfigParser import NoSectionError

def init():
    try:
        config = util.Config()
        couch_cfg = config.section('CouchDB')
        client = database.CouchDB(url=couch_cfg['url'], db=couch_cfg['database'], username=couch_cfg['username'], password=couch_cfg['password'])
        return (config, client)
    except NoSectionError:
        raise ValueError("Configuration file " + config.filename + "does not contain CouchDB section")
    except IOError as ex:
        raise IOError("Cannot establish connection with CouchDB: " + ex)