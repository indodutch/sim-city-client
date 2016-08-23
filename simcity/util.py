# SIM-CITY client
#
# Copyright 2015 Netherlands eScience Center
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Utility functions. """

import os
import glob
import shutil
from numbers import Number
import time
import jsonschema
import ijson
import mimetypes
import io
from datetime import datetime


def parse_parameters(parameters, schema):
    """
    Validates given parameters according to a JSON schema

    Parameters
    ----------
    parameters: dict
        a deep dict of values, where values may be simple types, dicts or lists
    schema: dict
        an object conforming to JSON schema syntax and semantics. Parameters
        will be checked according to this schema.

    Raises
    ------
    ValueError: if the parameters do not conform to the schema
    EnvironmentError: if the schema is not a valid JSON schema
    """
    try:
        jsonschema.validate(parameters, schema)
    except jsonschema.SchemaError as ex:
        raise EnvironmentError(ex.message)
    except jsonschema.ValidationError as ex:
        raise ValueError(ex.message)


def seconds():
    """ Return the current time in seconds since the epoch. """
    return int(time.time())


def seconds_to_str(timestamp, default_value=''):
    """ Convert a timestamp in seconds to string. """
    if timestamp > 0:
        return datetime.fromtimestamp(timestamp).strftime('%F %T')
    else:
        return default_value


def sizeof_fmt(num, suffix='B'):
    """ formatted bytes """
    if abs(num) < 1000.0:
        return "%d %s" % (int(num), suffix)

    num /= 1000.0
    for unit in ['k', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1000.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1000.0

    return "%.1f %s%s" % (num, 'Y', suffix)


class Timer(object):
    """ Measures elapsed time. """
    def __init__(self):
        self.t = time.time()

    def elapsed(self):
        """ Return elapsed time since creation or last reset."""
        return time.time() - self.t

    def reset(self):
        """ Reset the timer. """
        new_t = time.time()
        diff = new_t - self.t
        self.t = new_t
        return diff


def get_truthy(value):
    """ Returns True on non-zero numbers and on '1', 'true', 'yes', 'on'.
        False otherwise. """
    truthy = ['1', 'true', 'yes', 'on']
    if isinstance(value, Number):
        return bool(value)
    else:
        return value.lower() in truthy


def issequence(obj):
    """ True if given object is a list or a tuple. """
    return isinstance(obj, (list, tuple))


def expandfilename(filename):
    """ Joins sequences of filenames as directories, and expands variables and
        user directory. """
    if issequence(filename):
        filename = os.path.join(*filename)
    return os.path.expandvars(os.path.expanduser(filename))


def expandfilenames(filenames):
    """ Runs expandfilename on each item of a given list. """
    if not issequence(filenames):
        filenames = [filenames]
    return [expandfilename(fname) for fname in filenames]


def listfiles(mypath):
    """ Returns a list of regular filenames in given directory. """
    return [f
            for f in os.listdir(mypath)
            if os.path.isfile(os.path.join(mypath, f))]


def listdirs(mypath):
    """ Returns a list of directories in given directory. """
    return [d
            for d in os.listdir(mypath)
            if os.path.isdir(os.path.join(mypath, d))]


def copyglob(srcglob, dstdir, prefix=""):
    """ Copy a number of files in glob (wildcard) to a directory.
        Raises ValueError if the destination is not a directory. Each filename
        is first prefixed with given prefix. """
    if not os.path.isdir(dstdir):
        raise ValueError("Destination of copyglob must be a directory")

    new_files = []
    for src in glob.glob(expandfilename(srcglob)):
        _, fname = os.path.split(src)
        new_file = os.path.join(dstdir, prefix + fname)
        shutil.copyfile(src, new_file)
        new_files.append(new_file)
    return new_files


def data_content_type(filename, data):
    """ Get the content type of bytes data with a filename. """
    if filename.endswith('json'):
        with io.BytesIO(data) as f:
            if is_geojson(f):
                return 'application/vnd.geo+json'
    return filename_content_type(filename)


def file_content_type(filename, path):
    """ Get the content type of a path with a filename. """
    if filename.endswith('json'):
        with open(path, 'rb') as f:
            if is_geojson(f):
                return 'application/vnd.geo+json'
    return filename_content_type(filename)


def is_geojson(f):
    """ Whether given file pointer contains GeoJSON data. """
    try:
        json_type = next(ijson.items(f, 'type'))
        return json_type in ['Feature', 'FeatureCollection']
    except (ijson.common.JSONError, StopIteration):
        return False


def filename_content_type(filename):
    """ Guess content type based on filename. """
    content_type, encoding = mimetypes.guess_type(filename)
    if content_type is None:
        content_type = 'text/plain'
    return content_type
