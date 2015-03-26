# -*- coding: utf-8 -*-
# SIM-CITY client
#
# Copyright 2015 Joris Borgdorff <j.borgdorff@esciencecenter.nl>
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

"""
Provides access to a CouchDB database.
Originally used PiCaS, now all functionality directly accesses CouchDB.

Created on Mon Jun  4 11:40:06 2012
Updated Wed Jan 28 17:12 2015

@author: Joris Borgdorff
"""
import simcity
from picas.documents import Document
from picas.clients import CouchDB
from ConfigParser import NoSectionError


def _load(name):
    try:
        cfg = simcity.config.section(name)
    except NoSectionError:
        raise EnvironmentError(
            "Configuration file " + simcity.config.filename +
            " does not contain '" + name + "' section")

    try:
        return CouchDB(
            url=cfg['url'],
            db=cfg['database'],
            username=cfg['username'],
            password=cfg['password'])
    except IOError as ex:
        raise IOError("Cannot establish connection with " +
                      name + " CouchDB <" + cfg['url'] + ">: " + str(ex))
