#!/usr/bin/env python
# SIM-CITY client
#
# Copyright 2015 Joris Borgdorff <j.borgdorff@esciencecenter.nl>,
#                Anatoli Danezi  <anatoli.danezi@surfsara.nl>
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

'''
Create the databases and views
'''
from __future__ import print_function
import simcity
import argparse
import getpass
import couchdb
import sys

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="create databases and views for the simcity client")
    parser.add_argument(
        '-c', '--config', help="configuration file", default=None)
    parser.add_argument(
        '-p', '--password', help="admin password", default=None)
    parser.add_argument(
        'admin', help="admin user", default=None)
    args = parser.parse_args()

    try:
        simcity.init(config=args.config)
    except couchdb.http.ResourceNotFound:
        pass # database does not exist yet
    except couchdb.http.Unauthorized:
        pass # user does not exist yet

    if args.password is None:
        try:
            args.password = getpass.getpass('Password:')
        except KeyboardInterrupt: # cancel password prompt
            print("")
            sys.exit(1)

    try:
        simcity.create(args.admin, args.password)
    except couchdb.http.Unauthorized:
        print("User and/or password incorrect")
        sys.exit(1)
