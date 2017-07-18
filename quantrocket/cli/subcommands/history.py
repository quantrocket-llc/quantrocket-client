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

def add_subparser(subparsers):
    _parser = subparsers.add_parser("history", description="QuantRocket historical market data CLI", help="quantrocket history -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Create a new history database.

The historical data requirements you specify when you create a new database (bar size,
universes, etc.) are applied each time you fetch data for that database.

Examples:

Create an end-of-day database called "arca-etf-eod" for a universe called "arca-etf":

    quantrocket history create-db 'arca-etf-eod' --universes 'arca-etf' --bar-size '1 day' --start-date 2010-01-01

Create a similar end-of-day database, but fetch primary exchange prices instead of
consolidated prices, and adjust prices for dividends:

    quantrocket history create-db 'arca-etf-eod' -u 'arca-etf' -z '1 day' --primary-exchange --dividend-adjust -s 2010-01-01

Create a database of 1-minute bars showing the midpoint for a universe of forex pairs:

    quantrocket history create-db 'fx-1m' -u 'fx' -z '1 min' --bar-type MIDPOINT

Create a database of 1-second bars just before the open for a universe of Canadian energy
stocks in 2016:

    quantrocket history create-db 'tse-enr-929' -u 'tse-enr' -z '1 secs' --outside-rth --times 09:29:55 09:29:56 09:29:57 09:29:58 09:29:59 -s 2016-01-01 -e 2016-12-31

Create a database which won't fetch any data but into which you will load shortable shares
market data for Australian stocks from the realtime service:

    quantrocket history create-db 'asx-shortable' --no-config
    """
    parser = _subparsers.add_parser(
        "create-db",
        help="create a new history database",
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
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="fetch history back to this start date (default is to fetch as far back as data "
        "is available)")
    parser.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="fetch history up to this end date (default is to fetch up to the present)")
    parser.add_argument(
        "-v", "--vendor",
        metavar="VENDOR",
        choices=["ib"],
        help="the vendor to fetch data from (defaults to 'ib' which is currently the only "
        "supported vendor)")
    parser.add_argument(
        "-z", "--bar-size",
        metavar="BAR_SIZE",
        choices=[
            "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
            "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins",
            "1 hour", "2 hours", "3 hours", "4 hours", "8 hours",
            "1 day",
            "1 week",
            "1 month"],
        help="the bar size to fetch. Possible choices: %(choices)s")
    parser.add_argument(
        "-t", "--bar-type",
        metavar="BAR_TYPE",
        choices=["TRADES",
                 "MIDPOINT",
                 "BID",
                 "ASK",
                 "BID_ASK",
                 "HISTORICAL_VOLATILITY",
                 "OPTION_IMPLIED_VOLATILITY"],
        help="the bar type to fetch (if not specified, defaults to MIDPOINT for forex and "
        "TRADES for everything else). Possible choices: %(choices)s")
    parser.add_argument(
        "-o", "--outside-rth",
        action="store_true",
        help="include data from outside regular trading hours (default is to limit to regular "
        "trading hours)")
    parser.add_argument(
        "-p", "--primary-exchange",
        action="store_true",
        help="limit to data from the primary exchange")
    parser.add_argument(
        "--times",
        nargs="*",
        metavar="HH:MM:SS",
        help="limit to these times")
    parser.add_argument(
        "-d", "--dividend-adjust",
        action="store_true",
        help="adjust for dividends")
    parser.add_argument(
        "-n", "--no-config",
        action="store_true",
        help="create a database with no config (data can be loaded manually instead of fetched "
        "from a vendor)")
    parser.add_argument(
        "-f", "--config-file",
        dest="config_filepath_or_buffer",
        metavar="CONFIG_FILE",
        help="the path to a YAML config file defining the historical data requirements")
    parser.set_defaults(func="quantrocket.history._cli_create_db")

    parser = _subparsers.add_parser("query", help="query historical market data from one or more history databases")
    parser.add_argument("databases", metavar="DB", nargs="+", help="the database key(s), for example 'canada'")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="YYYY-MM-DD", help="limit to price history on or after this date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="YYYY-MM-DD", help="limit to price history on or before this date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("-f", "--fields", nargs="*", metavar="FIELD", help="limit to these fields")
    parser.add_argument("-t", "--times", metavar="HH:MM:SS", help="limit to these times")
    parser.add_argument("-c", "--continuous", choices=["concat", "adjust"], metavar="METHOD", help="join futures underlyings into continuous contracts, using the specified joining method ('concat' or 'adjust')")
    parser.set_defaults(func="quantrocket.history.get_history")

    parser = _subparsers.add_parser("fetch", help="fetch historical market data from a vendor into a history database")
    parser.add_argument("databases", metavar="DB", nargs="+", help="the database key(s), for example 'canada'")
    parser.add_argument("-p", "--priority", action="store_true", help="use the priority queue (default is to use the standard queue)")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="fetch history for these conids, overriding config")
    parser.set_defaults(func="quantrocket.history._cli_fetch_history")

    parser = _subparsers.add_parser("load", help="load historical market data from a file into a history database")
    parser.add_argument("database", metavar="DB", help="the database key, for example 'canada'")
    parser.add_argument("filename", help="JSON file containing price data (can also be passed on stdin)")
    parser.set_defaults(func="quantrocket.history.load_from_file")

    parser = _subparsers.add_parser("status", help="show info about pending and running historical market data retrievals")
    parser.set_defaults(func="quantrocket.history.get_status")

    parser = _subparsers.add_parser("cancel", help="cancel a running or pending historical market data retrieval")
    parser.add_argument("database", metavar="DB", help="the database key, for example 'canada'")
    parser.set_defaults(func="quantrocket.history.cancel_download")

    parser = _subparsers.add_parser("adjust", help="adjust prices for dividends in a history database")
    parser.add_argument("databases", metavar="DB", nargs="+", help="the database key(s), for example 'canada'")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("-c", "--on-cluster", dest="on_cluster", choices=["skip", "adjust"], help="whether to adjust price history if a cluster is present, or skip and log a warning")
    parser.set_defaults(func="quantrocket.history.dividend_adjust")

    examples = """
Return the configuration for a history database.

Examples:

Return the configuration for a database called "jpn-lrg-15m":

    quantrocket history config 'jpn-lrg-15m'
    """
    parser = _subparsers.add_parser(
        "config",
        help="return the configuration for a history database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        help="the database code")
    parser.set_defaults(func="quantrocket.history._cli_get_db_config")

    examples = """
Delete a history database.

Deleting a history database deletes its configuration and data and is irreversible.

Examples:

Delete a database called "jpn-lrg-15m":

    quantrocket history drop-db 'jpn-lrg-15m' --confirm-by-typing-db-code-again 'jpn-lrg-15m'

    """
    parser = _subparsers.add_parser(
        "drop-db",
        help="delete a history database",
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
    parser.set_defaults(func="quantrocket.history._cli_drop_db")