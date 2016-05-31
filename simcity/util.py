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

import json
import os
import glob
import shutil
from numbers import Number
import time
from copy import deepcopy
import jsonschema


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


def merge_dicts(dict1, dict2):
    """ Deep merge two dictionaries. """
    merge = deepcopy(dict1)
    merge.update(dict2)
    return merge


def seconds():
    """ Return the current time in seconds since the epoch. """
    return int(time.time())


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


def write_json(fname, obj):
    """ Write given object to the file referenced by fname. """
    with open(fname, 'w') as outfile:
        json.dump(obj, outfile)


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

    for src in glob.glob(expandfilename(srcglob)):
        _, fname = os.path.split(src)
        shutil.copyfile(src, os.path.join(dstdir, prefix + fname))
