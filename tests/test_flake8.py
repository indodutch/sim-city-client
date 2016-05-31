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

""" Run flake8 tests on all Python files """

from flake8.main import check_file
import os


def test_flake8():
    """ Test known python directories and files. """
    for test in flake8(directories=['tests', 'scripts', 'simcity'],
                       files=['setup.py']):
        yield test


def assert_zero(func, *args):
    """ Assert that given function with given arguments returns zero. """
    assert func(*args) == 0


def flake8(directories=(), files=()):
    """ Yield tests where flake8 runs recursively over given files and
    directories. """
    for directory in directories:
        for path, dirs, file_names in os.walk(directory):
            for file_name in file_names:
                if file_name.endswith('.py'):
                    yield assert_zero, check_file, os.path.join(path,
                                                                file_name)

    for file_name in files:
        yield assert_zero, check_file, file_name
