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
    _parser = subparsers.add_parser("account", description="QuantRocket account CLI", help="quantrocket account -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Query IB account balances.

Examples:

Query the latest account balances.

    quantrocket account balance --latest

Query the latest NLV (Net Liquidation Value) for a particular account:

    quantrocket account balance --latest --fields NetLiquidation --accounts U123456

Check for accounts that have fallen below a 5% cushion and log the results,
if any, to flightlog:

    quantrocket account balance --latest --below Cushion:0.05 | quantrocket flightlog log --name quantrocket.account --level CRITICAL

Query historical account balances over a date range:

    quantrocket account balance --start-date 2017-06-01 --end-date 2018-01-31
    """
    parser = _subparsers.add_parser(
        "balance",
        help="query IB account balances",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to account balance snapshots taken on or after "
        "this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to account balance snapshots taken on or before "
        "this date")
    filters.add_argument(
        "-l", "--latest",
        action="store_true",
        help="return the latest account balance snapshot")
    filters.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to these accounts")
    filters.add_argument(
        "-b", "--below",
        type=dict_str,
        nargs="*",
        metavar="FIELD:AMOUNT",
        help="limit to accounts where the specified field is below "
        "the specified amount (pass as field:amount, for example Cushion:0.05)")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    output_format_group.add_argument(
        "-p", "--pretty",
        action="store_const",
        const="txt",
        dest="output",
        help="format output in human-readable format (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="fetch account balances directly from IB (default is to query the "
        "database, which is updated every minute)")
    parser.set_defaults(func="quantrocket.account._cli_download_account_balances")

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
