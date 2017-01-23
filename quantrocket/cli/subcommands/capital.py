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

from ..utils.parse import parse_date
from quantrocket.constants.capital import ACCOUNT_FIELDS

def add_subparser(subparsers):
    _parser = subparsers.add_parser("capital", description="QuantRocket capital balance CLI [COMING SOON]", help="quantrocket capital -h [COMING SOON]")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("balance", help="Display a snapshot of current capital balance info")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-n", "--nlv", action="store_true", help="only display NLV")
    group.add_argument("-c", "--cushion", action="store_true", help="only display margin cushion")
    group.add_argument("-f", "--fields", metavar="FIELD", nargs="*", choices=ACCOUNT_FIELDS, help="only display these fields")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="only display these accounts")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-b", "--below-cushion", type=float, metavar="CUSHION", help="only display accounts where the cushion is below this level (e.g. 0.05)")
    group.add_argument("-s", "--save", action="store_true", help="save a snapshot of capital balance info to the capital database")
    parser.add_argument("-g", "--gateways", nargs="*", metavar="IBG_SERVICE", help="only query these IB Gateway services")
    parser.set_defaults(func="quantrocket.capital.get_balance")

    parser = _subparsers.add_parser("history", help="Display historical capital balance snapshots from the capital database")
    parser.add_argument("-s", "--start", dest="start_date", type=parse_date, help="limit to history on or after this date")
    parser.add_argument("-e", "--end", dest="end_date", type=parse_date, help="limit to history on or before this date")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-n", "--nlv", action="store_true", help="only display NLV")
    group.add_argument("-f", "--fields", metavar="FIELD", nargs="*", choices=ACCOUNT_FIELDS, help="only display these fields")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="only display these accounts")
    parser.add_argument("-l", "--latest", action="store_true", help="only display the latest historical snapshot in the date range")
    parser.set_defaults(func="quantrocket.capital.get_balance_history")
