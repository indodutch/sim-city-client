# sim-city-client
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

from flake8.api import legacy as flake8
import os
import logging
import pytest


@pytest.fixture
def disable_logging():
    # flake8 is logging is really verbose. disable.
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture(params=['tests', 'scripts', 'xenon', 'setup.py'])
def flake8_filenames(request):
    """ Yield filenames for flake8 to run over recursively, given files and
    directories. """
    if os.path.isdir(request.param):
        paths = []
        for path, dirs, filenames in os.walk(request.param):
            paths += [os.path.join(os.path.abspath(path), filename)
                      for filename in filenames]
        yield paths
    else:
        yield [request.param]


@pytest.mark.usefixtures("disable_logging")
def test_flakes8(flake8_filenames):
    """ Test a single file with flake8. """
    if len(flake8_filenames) > 0:
        report = flake8.get_style_guide().check_files(flake8_filenames)
        assert report.total_errors == 0, "flake8 found a warning or error"
