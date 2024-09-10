# Copyright 2017-2024 QuantRocket - All Rights Reserved
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

from quantrocket._cli.utils.parse import (
    dict_str,
    HelpFormatter
)
from quantrocket._cli.utils import completers

def add_subparser(subparsers):
    _parser = subparsers.add_parser("account", description="QuantRocket account CLI", help="Query account balances and exchange rates")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Query account balances.

Notes
-----
Usage Guide:

* Account Monitoring: https://qrok.it/dl/qr/account

Examples
--------

Query the latest account balances.

.. code-block:: bash

    quantrocket account balance --latest

Query the latest NLV (Net Liquidation Value) for a particular account:

.. code-block:: bash

    quantrocket account balance --latest --fields NetLiquidation --accounts U123456

Check for accounts that have fallen below a 5% cushion and log the results,
if any, to flightlog:

.. code-block:: bash

    quantrocket account balance --latest --below Cushion:0.05 | quantrocket flightlog log --name quantrocket.account --level CRITICAL

Query historical account balances over a date range:

.. code-block:: bash

    quantrocket account balance --start-date 2017-06-01 --end-date 2018-01-31
    """
    parser = _subparsers.add_parser(
        "balance",
        help="query account balances",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to account balance snapshots taken on or after "
        "this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to account balance snapshots taken on or before "
        "this date").completer = completers.end_date_completer
    filters.add_argument(
        "-l", "--latest",
        action="store_true",
        help="return the latest account balance snapshot")
    filters.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to these accounts").completer = completers.account_completer
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
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="balances")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields. By default a core set of fields is returned. "
        "Pass a list of fields, or '*' to return all fields. Pass '?' or any "
        "invalid fieldname to see available fields."
        ).completer = completers.account_balance_fields_completer
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="refresh account balances to ensure the latest data (default is to query the "
        "database, which is refreshed every minute)")
    parser.set_defaults(func="quantrocket.account._cli_download_account_balances")

    examples = """
Download current portfolio.

Notes
-----
Usage Guide:

* Account Monitoring: https://qrok.it/dl/qr/account

Examples
--------

View current portfolio in terminal:

.. code-block:: bash

    quantrocket account portfolio | csvlook

Download current portfolio for a particular account and save to file:

.. code-block:: bash

    quantrocket account portfolio --accounts U12345 -o portfolio.csv
    """
    parser = _subparsers.add_parser(
        "portfolio",
        help="download current portfolio",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-b", "--brokers",
        nargs="*",
        metavar="BROKER",
        choices=["ibkr", "alpaca"],
        help="limit to these brokers. Possible choices: ibkr, alpaca")
    filters.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to these accounts").completer = completers.account_completer
    filters.add_argument(
        "-t", "--sec-types",
        nargs="*",
        metavar="SEC_TYPE",
        help="limit to these security types").completer = completers.sec_type_completer()
    filters.add_argument(
        "-e", "--exchanges",
        nargs="*",
        metavar="EXCHANGE",
        help="limit to these exchanges").completer = completers.exchange_calendar_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "-s", "--symbols",
        nargs="*",
        metavar="SYMBOL",
        help="limit to these symbols").completer = completers.symbol_completer
    filters.add_argument(
        "-z", "--zero",
        action="store_true",
        dest="include_zero",
        help="include zero position rows (default is to exclude them). Only "
        "supported for Interactive Brokers.")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="portfolio")
    outputs.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields. By default a core set of fields is returned. "
        "Pass a list of fields, or '*' to return all fields. Pass '?' or any "
        "invalid fieldname to see available fields."
        ).completer = completers.account_portfolio_fields_completer
    parser.set_defaults(func="quantrocket.account._cli_download_account_portfolio")

    examples = """
Query exchange rates for the base currency.

The exchange rates in the exchange rate database are sourced from the
European Central Bank's reference rates, which are updated each day at 4 PM
CET.

Notes
-----
Usage Guide:

* Account Monitoring: https://qrok.it/dl/qr/account

Examples
--------

Query the latest exchange rates.

.. code-block:: bash

    quantrocket account rates --latest
    """
    parser = _subparsers.add_parser(
        "rates",
        help="query exchange rates for the base currency",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to exchange rates on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to exchange rates on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-l", "--latest",
        action="store_true",
        help="return the latest exchange rates")
    filters.add_argument(
        "-b", "--base-currencies",
        nargs="*",
        metavar="CURRENCY",
        help="limit to these base currencies"
        ).completer = completers.currency_completer
    filters.add_argument(
        "-q", "--quote-currencies",
        nargs="*",
        metavar="CURRENCY",
        help="limit to these quote currencies"
        ).completer = completers.currency_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="rates")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    parser.set_defaults(func="quantrocket.account._cli_download_exchange_rates")
