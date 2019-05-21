# Copyright 2019 QuantRocket - All Rights Reserved
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

def add_subparser(subparsers):
    _parser = subparsers.add_parser("realtime", description="QuantRocket real-time market data CLI", help="Collect and query real-time market data")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Create a new real-time database.

The market data requirements you specify when you create a new database are
applied each time you collect data for that database.

Examples:

Create a database for collecting real-time trades and volume for US stocks:

    quantrocket realtime create-db usa-stk-trades -u usa-stk

Create a database for collecting trades and quotes for a universe of futures:

    quantrocket realtime create-db globex-fut-taq -u globex-fut --fields last volume bid ask bid_size ask_size
    """
    parser = _subparsers.add_parser(
        "create-db",
        help="create a new real-time database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the database (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-u", "--universes",
        metavar="UNIVERSE",
        nargs="*",
        help="include these universes")
    parser.add_argument(
        "-i", "--conids",
        metavar="CONID",
        nargs="*",
        help="include these conids")
    parser.add_argument(
        "-v", "--vendor",
        metavar="VENDOR",
        choices=["ib"],
        help="the vendor to collect data from (default 'ib'. Possible choices: "
        "%(choices)s)")
    parser.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="collect these fields (pass '?' or any invalid fieldname to see "
        "available fields, default fields are 'last' and 'volume')")
    parser.add_argument(
        "-p", "--primary-exchange",
        action="store_true",
        help="limit to data from the primary exchange")
    parser.set_defaults(func="quantrocket.realtime._cli_create_db")

    examples = """
Return the configuration for a real-time database.

Examples:

Return the configuration for a database called "globex-fut-taq":

    quantrocket realtime config 'globex-fut-taq'
    """
    parser = _subparsers.add_parser(
        "config",
        help="return the configuration for a real-time database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        help="the database code")
    parser.set_defaults(func="quantrocket.realtime._cli_get_db_config")

    examples = """
Delete a real-time database.

Deleting a real-time database deletes its configuration and data and is
irreversible.

Examples:

Delete a database called "usa-stk-trades":

    quantrocket realtime drop-db 'usa-stk-trades' --confirm-by-typing-db-code-again 'usa-stk-trades'

    """
    parser = _subparsers.add_parser(
        "drop-db",
        help="delete a real-time database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        help="the database code")
    parser.add_argument(
        "--confirm-by-typing-db-code-again",
        metavar="CODE",
        required=True,
        help="enter the db code again to confirm you want to drop the database, its config, "
        "and all its data")
    parser.set_defaults(func="quantrocket.realtime._cli_drop_db")

    examples = """
Collect real-time market data and save it to a real-time database.

A single snapshot of market data or a continuous stream of market data can
be collected, depending on the `--snapshot` parameter.

Streaming real-time data is collected until cancelled, or can be scheduled
for cancellation using the `--until` parameter.

Examples:

Collect market data for all securities in a database called 'japan-banks-trades':

    quantrocket realtime collect japan-banks-trades

Collect market data for a subset of securities in a database called 'usa-stk-trades'
and automatically cancel the data collection in 30 minutes:

    quantrocket realtime collect usa-stk-trades --conids 12345 23456 34567 --until 30m

Collect a market data snapshot and wait until it completes:

    quantrocket realtime collect usa-stk-trades --snapshot --wait
    """
    parser = _subparsers.add_parser(
        "collect",
        help="collect real-time market data and save it to a real-time database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the database code(s) to collect data for")
    parser.add_argument(
        "-i", "--conids",
        nargs="*",
        metavar="CONID",
        help="collect market data for these conids, overriding db config "
        "(typically used to collect a subset of securities)")
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="collect market data for these universes, overriding db config "
        "(typically used to collect a subset of securities)")
    parser.add_argument(
        "-f", "--fields",
        nargs="*",
        metavar="FIELD",
        help="limit to these fields, overriding db config")
    parser.add_argument(
        "--until",
        metavar="TIME_OR_TIMEDELTA",
        help="schedule data collection to end at this time. Can be a datetime "
        "(YYYY-MM-DD HH:MM:SS), a time (HH:MM:SS), or a Pandas timedelta "
        "string (e.g. 2h or 30min). If not provided, market data is collected "
        "until cancelled.")
    parser.add_argument(
        "-s", "--snapshot",
        action="store_true",
        help="collect a snapshot of market data (default is to collect a continuous "
        "stream of market data)")
    parser.add_argument(
        "-w", "--wait",
        action="store_true",
        help="wait for market data snapshot to complete before returning (default is "
        "to return immediately). Requires --snapshot")
    parser.set_defaults(func="quantrocket.realtime._cli_collect_market_data")

    examples = """
Return the number of currently subscribed tickers by vendor and database.

Examples:

    quantrocket realtime active
    """
    parser = _subparsers.add_parser(
        "active",
        help="return the number of currently subscribed tickers by vendor and database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="return lists of subscribed tickers (default is to return "
        "counts of subscribed tickers)")
    parser.set_defaults(func="quantrocket.realtime._cli_get_active_subscriptions")

    examples = """
Cancel market data collection.

Examples:

Cancel market data collection for a database called 'globex-fut-taq':

    quantrocket realtime cancel 'globex-fut-taq'

Cancel all market data collection:

    quantrocket realtime cancel --all
    """
    parser = _subparsers.add_parser(
        "cancel",
        help="cancel market data collection",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="*",
        help="the database code(s) to cancel collection for")
    parser.add_argument(
        "-i", "--conids",
        nargs="*",
        metavar="CONID",
        help="cancel market data for these conids, overriding db config")
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="cancel market data for these universes, overriding db config")
    parser.add_argument(
        "-a", "--all",
        action="store_true",
        dest="cancel_all",
        help="cancel all market data collection")
    parser.set_defaults(func="quantrocket.realtime._cli_cancel_market_data")
