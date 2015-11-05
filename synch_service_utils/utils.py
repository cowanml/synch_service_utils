from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import os
import yaml
import tornado.web
from pkg_resources import resource_filename as rs_fn
import ujson
import datetime
from bson.objectid import ObjectId

__all__ = []

def load_configuration(name, prefix, fields, logger):
    """
    Load configuration data form a cascading series of locations.

    The precedence order is (highest priority last):

    1. CONDA_ENV/etc/{name}.yaml (if CONDA_ETC_ env is defined)
    2. /etc/{name}.yml
    3. ~/.config/{name}/connection.yml
    4. reading {PREFIX}_{FIELD} environmental variables

    Parameters
    ----------
    name : str
        The expected base-name of the configuration files

    prefix : str
        The prefix when looking for environmental variables

    fields : iterable of strings
        The required configuration fields

    logger : logging.Logger
        logging channel to report loading errors on

    Returns
    ------
    conf : dict
        Dictionary keyed on ``fields`` with the values extracted
    """
    filenames = [os.path.join('/etc', name + '.yml'),
                 os.path.join(os.path.expanduser('~'), '.config',
                              name, 'connection.yml'),
                ]
    if 'CONDA_ETC_' in os.environ:
        filenames.insert(0, os.path.join(os.environ['CONDA_ETC_'],
                                         name + '.yml'))

    config = {}
    for filename in filenames:
        if os.path.isfile(filename):
            with open(filename) as f:
                config.update(yaml.load(f))
            logger.debug("Using db connection specified in config file. \n%r",
                         config)

    for field in fields:
        var_name = prefix + '_' + field.upper().replace(' ', '_')
        config[field] = os.environ.get(var_name, config.get(field, None))

    missing = [k for k, v in config.items() if v is None]
    if missing:
        raise KeyError("The configuration field(s) {0} were not found in any "
                       "file or environmental variable.".format(missing))
    return config


def db_connect(database ,host, port, replicaset=None, write_concern="majority",
               write_timeout=1000):
    """Helper function to deal with stateful connections to motor.
    Connection established lazily. Asycnc so do not treat like mongonengine connection pool.

    Parameters
    ----------
    server: str
        The name of the server that data is stored
    host: str
        Name/address of the server that mongo daemon lives
    port: int
        Port num of the server
    replicaset: str
        Name of the replica set. Configured within mongo deployment.
    write_concern: int
        Traditional mongo write concern. Int denotes number of replica set writes
        to be verified

    write_timeout: int
        Time tolerance before write fails. Affects the package size in bulk insert so use wisely

    Returns motor.MotorDatabase
    -------
        Async server object which comes in handy as server has to juggle multiple clients
        and makes no difference for a single client compared to pymongo
    """
    client = motor.MotorClient(host, port, replicaset=replicaset)
    client.write_concern = {'w': write_concern, 'wtimeout': write_timeout}
    database = client[database]
    return database


def load_schemas(service_name, schema_path, schema_names):
    schemas = {}
    fn = '{}/{{}}'.format(schema_path)

    for name, filename in schema_names.items():
        with open(rs_fn(service_name, resource_name=fn.format(filename))) as fin:
            schemas[name] = ujson.load(fin)

    return schemas


def _unpack_params(handler):
    """Unpacks the queries from the body of the header
    Parameters
    ----------
    handler: tornado.web.RequestHandler
        Handler for incoming request to collection

    Returns: dict
    -------
        Unpacked query in dict format.
    """
    if isinstance(handler, tornado.web.RequestHandler):
        return ujson.loads(list(handler.request.arguments.keys())[0])
    else:
        raise TypeError("Handler provided must be of tornado.web.RequestHandler type")


def _return2client(handler, payload):
    """Dump the result back to client's open socket. No need to worry about package size
    or socket behavior as tornado handles this for us

    Parameters
    -----------
    handler: tornado.web.RequestHandler
        Request handler for the collection of operation(post/get)
    payload: dict, str, list
        Information to be sent to the client
    """
    handler.write(ujson.dumps(_stringify_data(payload)))


def _stringify_data(docs):
    """ujson does not allow encoding/decoding of any object other than
    the basic python types. Therefore, we need to convert datetime and oid
    into string. If nested dictionary, it is handled here as well.

    Parameters
    -----------
    docs: list or dict
        Query results to be 'stringified'
    Returns
    -----------
    stringed: list or dict
        Stringified list or dictionary
    """
    if isinstance(docs, list):
        new_docs = []
        tmp = dict
        for d in docs:
            for k, v  in d.items():
                if isinstance(v, ObjectId):
                    d[k] = 'N/A'
                elif isinstance(v, datetime.datetime):
                    tmp = str(v)
                    d[k] = tmp
                elif isinstance(v, dict):
                    d[k] = _stringify_data(v)
        return docs
    elif isinstance(docs, dict):
        for k, v in docs.items():
            if isinstance(v, ObjectId):
                docs[k] = 'N/A'
            elif isinstance(v, datetime.date):
                tmp = str(v)
                docs[k] = tmp
            elif isinstance(v, dict):
                docs[k] = _stringify_data(v)
        return docs
    else:
        raise TypeError("Unsupported type ", type(docs))
