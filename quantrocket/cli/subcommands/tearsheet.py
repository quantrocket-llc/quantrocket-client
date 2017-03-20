# Copyright 2017 QuantRocket - All Rights Reserved
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

from quantrocket.cli.utils.parse import parse_dict

def add_subparser(subparsers):
    _parser = subparsers.add_parser("tearsheet", description="QuantRocket tearsheet CLI", help="quantrocket report -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("performance", help="generate a performance tearsheet from performance data")
    parser.add_argument("filename", nargs="?", metavar="FILE", help="JSON file containing performance data (can also be passed on stdin)")
    parser.add_argument("-z", "--zip", action="store_true", help="return zip file of figures (default is to return a PDF)")
    parser.set_defaults(func="quantrocket.tearsheet.get_performance_report")

    parser = _subparsers.add_parser("paramscan", help="generate a parameter scan tearsheet from parameter scan performance data")
    parser.add_argument("filename", nargs="?", metavar="FILE", help="JSON file containing performance data (can also be passed on stdin)")
    parser.add_argument("-z", "--zip", action="store_true", help="return zip file of figures (default is to return a PDF)")
    parser.set_defaults(func="quantrocket.tearsheet.get_paramscan_report")

    parser = _subparsers.add_parser("shortfall", help="generate an implementation shortfall tearsheet from two sets of performance data")
    parser.add_argument("live_filename", nargs="?", metavar="LIVE_FILE", help="JSON file containing live performance data (or other data to evaluate)")
    parser.add_argument("sim_filename", nargs="?", metavar="SIMULATED_FILE", help="JSON file containing simulated performance data (or other benchmark data)")
    parser.add_argument("-z", "--zip", action="store_true", help="return zip file of figures (default is to return a PDF)")
    parser.set_defaults(func="quantrocket.tearsheet.get_shortfall_report")

    parser = _subparsers.add_parser("pyfolio", help="generate a Pyfolio tearsheet from performance data")
    parser.add_argument("filename", nargs="?", metavar="FILE", help="JSON file containing performance data (can also be passed on stdin)")
    parser.add_argument("-z", "--zip", action="store_true", help="return zip file of figures (default is to return a PDF)")
    parser.set_defaults(func="quantrocket.tearsheet.get_pyfolio_report")
