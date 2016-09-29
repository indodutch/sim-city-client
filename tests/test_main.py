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

import simcity.__main__ as main
import pytest


@pytest.fixture(params=['cancel', 'check', 'create', 'delete', 'init', 'run',
                        'scrub', 'summary', 'submit'])
def test_main_parser_init(request, argument_parser):
    main.fill_argument_parser(argument_parser)
    assert 'func' in argument_parser.parse_args([request.param])
