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
from quantrocket.constants.account import ACCOUNT_FIELDS

def add_subparser(subparsers):
    _parser = subparsers.add_parser("account", description="QuantRocket account CLI", help="quantrocket account -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("balance", help="get a snapshot of current account balance info")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-n", "--nlv", action="store_true", help="only show NLV")
    group.add_argument("-c", "--cushion", action="store_true", help="only show margin cushion")
    group.add_argument("-f", "--fields", metavar="FIELD", nargs="*", choices=ACCOUNT_FIELDS, help="only display these fields")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="only show these accounts")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-b", "--below-cushion", type=float, metavar="CUSHION", help="only show accounts where the cushion is below this level (e.g. 0.05)")
    group.add_argument("-s", "--save", action="store_true", help="save a snapshot of account balance info to the account database")
    parser.set_defaults(func="quantrocket.account.get_balance")

    parser = _subparsers.add_parser("history", help="get historical account balance snapshots from the account database")
    parser.add_argument("-s", "--start-date", metavar="YYYY-MM-DD", help="limit to history on or after this date")
    parser.add_argument("-e", "--end-date", metavar="YYYY-MM-DD", help="limit to history on or before this date")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-n", "--nlv", action="store_true", help="only show NLV")
    group.add_argument("-f", "--fields", metavar="FIELD", nargs="*", choices=ACCOUNT_FIELDS, help="only display these fields")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="only show these accounts")
    parser.add_argument("-l", "--latest", action="store_true", help="only show the latest historical snapshot in the date range")
    parser.set_defaults(func="quantrocket.account.get_balance_history")

    parser = _subparsers.add_parser("portfolio", help="get current account portfolio")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    parser.set_defaults(func="quantrocket.account.get_portfolio")

    parser = _subparsers.add_parser("default", help="view or set the default account")
    parser.add_argument("account", nargs="?", metavar="ACCOUNT", help="set this account as the default (omit to view current default account)")
    parser.set_defaults(func="quantrocket.account._get_or_set_default_account")

    parser = _subparsers.add_parser("fx", help="fetch base currency exchange rates from Google and optionally save to account database")
    parser.add_argument("-c", "--currencies", nargs="*", metavar="CURRENCY", help="limit to these currencies (default all IB-tradeable currencies)")
    parser.add_argument("-s", "--save", action="store_true", help="save exhange rates to the account database")
    parser.set_defaults(func="quantrocket.account.get_exchange_rates")

    parser = _subparsers.add_parser("fxhistory", help="return historical exchange rates from the account database")
    parser.add_argument("-c", "--currencies", nargs="*", metavar="CURRENCY", help="limit to these currencies (default all IB-tradeable currencies)")
    parser.add_argument("-s", "--start-date", metavar="YYYY-MM-DD", help="limit to history on or after this date")
    parser.add_argument("-e", "--end-date", metavar="YYYY-MM-DD", help="limit to history on or before this date")
    parser.add_argument("-l", "--latest", action="store_true", help="only show the latest exchange rate in the date range")
    parser.set_defaults(func="quantrocket.account.get_exchange_rate_history")
