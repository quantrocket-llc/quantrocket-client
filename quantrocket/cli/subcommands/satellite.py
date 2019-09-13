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

import argparse
from quantrocket.cli.utils.parse import dict_str

def add_subparser(subparsers):
    _parser = subparsers.add_parser("satellite", description="QuantRocket Satellite CLI", help="Run custom scripts")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Execute a Python function or abitrary shell command on a satellite service.

Examples:

Run a Python function called 'create_calendar_spread' defined in '/codeload/scripts/combos.py'
and pass it arguments:

    quantrocket satellite exec 'codeload.scripts.combos.create_calendar_spread' --params 'universe:cl-fut' 'contract_months:[1,2]'

Run a backtrader backtest and save the performance chart to file:

    quantrocket satellite exec 'python /codeload/backtrader/dual_moving_average.py' --return-file '/tmp/backtrader-plot.pdf' --outfile 'backtrader-plot.pdf'
    """
    parser = _subparsers.add_parser(
        "exec",
        help="execute a Python function or abitrary shell command on a satellite service",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "cmd",
        metavar="CMD",
        help="the shell command to run, or the Python function in dot notation (must "
        "start with 'codeload.' to be interpreted as a Python function)")
    parser.add_argument(
        "-r", "--return-file",
        metavar="FILEPATH",
        help="the path of a file to be returned after the command completes")
    parser.add_argument(
        "-o", "--outfile",
        metavar="FILEPATH",
        dest="filepath_or_buffer",
        help="the location to write the return_file (omit to write to stdout)")
    parser.add_argument(
        "-p", "--params",
        nargs="*",
        type=dict_str,
        metavar="PARAM:VALUE",
        help="one or more params to pass to the Python function (pass as {param:value})")
    parser.add_argument(
        "-s", "--service",
        metavar="SERVICE_NAME",
        default="satellite",
        help="the service name (default 'satellite')")
    parser.set_defaults(func="quantrocket.satellite._cli_execute_command")
