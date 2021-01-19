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
    _parser = subparsers.add_parser("history", description="QuantRocket historical market data CLI", help="Collect and query historical data")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Create a new database into which custom data can be loaded.

Examples:

Create a custom database for loading fundamental data:

    quantrocket history create-custom-db custom-fundamentals --bar-size '1 day' --columns Revenue:int EPS:float Currency:str TotalAssets:int

Create a custom database for loading intraday OHCLV data:

    quantrocket history create-custom-db custom-stk-1sec --bar-size '1 sec' --columns Open:float High:float Low:float Close:float Volume:int
"""
    parser = _subparsers.add_parser(
        "create-custom-db",
        help="create a new database into which custom data can be loaded",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the database (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-z", "--bar-size",
        metavar="BAR_SIZE",
        help="the bar size that will be loaded. This isn't enforced but facilitates efficient "
        "querying and provides a hint to other parts of the API. Use a Pandas timedelta "
        "string, for example, '1 day' or '1 min' or '1 sec'.")
    parser.add_argument(
        "-c", "--columns",
        metavar="NAME:TYPE",
        nargs="*",
        type=dict_str,
        help="the columns to create, specified as 'name:type'. For example, 'Close:float' "
        " or 'Volume:int'. Valid column types are 'int', 'float', 'text', 'date', and "
        "'datetime'. Column names must start with a letter and include only letters, "
        "numbers, and underscores. Sid and Date columns are automatically created and "
        "need not be specified. For boolean columns, choose type 'int' and store 1 or 0. ")
    parser.set_defaults(func="quantrocket.history._cli_create_custom_db")

    examples = """
Create a new database for collecting historical data from EDI.

Examples:

Create a database for end-of-day China stock prices from EDI:

    quantrocket history create-edi-db china-1d -e XSHG XSHE
"""
    parser = _subparsers.add_parser(
        "create-edi-db",
        help="create a new database for collecting historical data from EDI",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the database (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-e", "--exchanges",
        metavar="MIC",
        nargs="*",
        help="one or more exchange codes (MICs) which should be collected")
    parser.set_defaults(func="quantrocket.history._cli_create_edi_db")

    examples = """
Create a new database for collecting historical data from Interactive Brokers.

The historical data requirements you specify when you create a new database (bar size,
universes, etc.) are applied each time you collect data for that database.

Examples:

Create an end-of-day database called "arca-etf-eod" for a universe called "arca-etf":

    quantrocket history create-ibkr-db 'arca-etf-eod' --universes 'arca-etf' --bar-size '1 day'

Create a similar end-of-day database, but collect primary exchange prices instead of
consolidated prices, adjust prices for dividends (=ADJUSTED_LAST), and use an explicit
start date:

    quantrocket history create-ibkr-db 'arca-etf-eod' -u 'arca-etf' -z '1 day' --primary-exchange --bar-type 'ADJUSTED_LAST' -s 2010-01-01

Create a database of 1-minute bars showing the midpoint for a universe of FX pairs:

    quantrocket history create-ibkr-db 'fx-1m' -u 'fx' -z '1 min' --bar-type MIDPOINT

Create a database of 1-second bars just before the open for a universe of Canadian energy
stocks in 2016:

    quantrocket history create-ibkr-db 'tse-enr-929' -u 'tse-enr' -z '1 secs' --outside-rth --times 09:29:55 09:29:56 09:29:57 09:29:58 09:29:59 -s 2016-01-01 -e 2016-12-31
    """
    parser = _subparsers.add_parser(
        "create-ibkr-db",
        help="create a new database for collecting historical data from Interactive Brokers",
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
        "-i", "--sids",
        metavar="SID",
        nargs="*",
        help="include these sids")
    parser.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="collect history back to this start date (default is to collect as far back as data "
        "is available)")
    parser.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="collect history up to this end date (default is to collect up to the present)")
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
        help="the bar size to collect. Possible choices: %(choices)s")
    parser.add_argument(
        "-t", "--bar-type",
        metavar="BAR_TYPE",
        choices=["TRADES",
                 "ADJUSTED_LAST",
                 "MIDPOINT",
                 "BID",
                 "ASK",
                 "BID_ASK",
                 "HISTORICAL_VOLATILITY",
                 "OPTION_IMPLIED_VOLATILITY"],
        help="the bar type to collect (if not specified, defaults to MIDPOINT for FX and "
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
    times_group = parser.add_mutually_exclusive_group()
    times_group.add_argument(
        "--times",
        nargs="*",
        metavar="HH:MM:SS",
        help="limit to these times (refers to the bar's start time; mutually exclusive "
        "with --between-times)")
    times_group.add_argument(
        "--between-times",
        nargs=2,
        metavar="HH:MM:SS",
        help="limit to times between these two times (refers to the bar's start time; "
        "mutually exclusive with --times)")
    parser.add_argument(
        "--shard",
        metavar="HOW",
        choices=["year", "month", "day", "time", "sid", "sid,time", "off"],
        help="whether and how to shard the database, i.e. break it into smaller pieces. "
        "Required for intraday databases. Possible choices are `year` (separate "
        "database for each year), `month` (separate database for each year+month), "
        "`day` (separate database for each day), `time` (separate database for each bar "
        "time), `sid` (separate database for each security), `sid,time` (duplicate "
        "copies of database, one sharded by sid and the other by time),or `off` (no "
        "sharding). See http://qrok.it/h/shard for more help.")
    parser.set_defaults(func="quantrocket.history._cli_create_ibkr_db")

    examples = """
Create a new database for collecting historical data from Sharadar.

Examples:

Create a database for Sharadar US stocks and call it "sharadar-us-stk-1d":

    quantrocket history create-sharadar-db sharadar-us-stk-1d --sec-type STK --country US
"""
    parser = _subparsers.add_parser(
        "create-sharadar-db",
        help="create a new database for collecting historical data from Sharadar",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the database (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=["STK","ETF"],
        help="the security type to collect. Possible choices: %(choices)s")
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect data for. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.history._cli_create_sharadar_db")

    examples = """
Create a new database for collecting historical US stock data from QuantRocket.

Examples:

Create a database for end-of-day US stock prices:

    quantrocket history create-usstock-db usstock-1d
"""
    parser = _subparsers.add_parser(
        "create-usstock-db",
        help="create a new database for collecting historical US stock data from QuantRocket",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the database (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-z", "--bar-size",
        metavar="BAR_SIZE",
        choices=["1 day"],
        help="the bar size to collect. Possible choices: %(choices)s")
    parser.add_argument(
        "--free",
        action="store_true",
        help="limit to free sample data. Default is to collect the full dataset.")
    parser.add_argument(
        "-u", "--universe",
        choices=[
            "US",
            "FREE"
            ],
        help="[DEPRECATED] whether to collect free sample data or the full dataset. "
        "This parameter is deprecated and will be removed in a future release. Please "
        "use --free to request free sample data or omit --free to request the full dataset.")
    parser.set_defaults(func="quantrocket.history._cli_create_usstock_db")

    examples = """
List history databases.

Examples:

    quantrocket history list
    """
    parser = _subparsers.add_parser(
        "list",
        help="list history databases",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.history._cli_list_databases")

    examples = """
Return the configuration for a history database.

Examples:

Return the configuration for a database called "jpn-lrg-15m":

    quantrocket history config jpn-lrg-15m
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

    quantrocket history drop-db jpn-lrg-15m --confirm-by-typing-db-code-again jpn-lrg-15m

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

    examples = """
Collect historical market data from a vendor and save it to a history database.

The vendor and collection parameters are determined by the stored database
configuration as defined at the time the database was created. For certain
vendors, collection parameters can be overridden at the time of data collection.

Examples:

Collect historical data for a database of Chinese stock prices:

    quantrocket history collect china-1d

Collect historical data for an IBKR database of US futures, using the priority
queue to jump in front of other queued IBKR collections:

    quantrocket history collect globex-10m --priority
    """
    parser = _subparsers.add_parser(
        "collect",
        help="collect historical market data from a vendor and save it to a history database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the database code(s) to collect data for")
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="collect history for these sids, overriding config "
        "(typically used to collect a subset of securities). Only "
        "supported for IBKR databases.")
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="collect history for these universes, overriding config "
        "(typically used to collect a subset of securities). Only "
        "supported for IBKR databases.")
    parser.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="collect history back to this start date, overriding config")
    parser.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="collect history up to this end date, overriding config. Only "
        "supported for IBKR databases.")
    parser.add_argument(
        "-p", "--priority",
        action="store_true",
        help="use the priority queue (default is to use the standard queue). "
        "Only applicable to IBKR databases.")
    parser.set_defaults(func="quantrocket.history._cli_collect_history")

    examples = """
Get the current queue of historical data collections.

Examples:

    quantrocket history queue
    """
    parser = _subparsers.add_parser(
        "queue",
        help="get the current queue of historical data collections",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.history._cli_get_history_queue")

    examples = """
Cancel running or pending historical data collections.

Examples:

Cancel collections for a database called japan-1d:

    quantrocket history cancel japan-1d
    """
    parser = _subparsers.add_parser(
        "cancel",
        help="cancel running or pending historical data collections",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the database code(s) to cancel collections for")
    parser.set_defaults(func="quantrocket.history._cli_cancel_collections")

    examples = """
Wait for historical data collection to finish.

Examples:

Wait at most 10 minutes for data collection to finish for a database called 'fx-1h':

    quantrocket history wait 'fx-1h' -t 10min
    """
    parser = _subparsers.add_parser(
        "wait",
        help="wait for historical data collection to finish",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the database code(s) to wait for")
    parser.add_argument(
        "-t", "--timeout",
        metavar="TIMEDELTA",
        help="time out if data collection hasn't finished after this much time (use Pandas "
        "timedelta string, e.g. 30sec or 5min or 2h)")
    parser.set_defaults(func="quantrocket.history._cli_wait_for_collections")

    examples = """
Query historical market data from a history database and download to file.

Examples:

Download a CSV of all historical market data since 2015 from a database called
"arca-eod" to a file called arca.csv:

    quantrocket history get arca-eod --start-date 2015-01-01 -o arca.csv
    """
    parser = _subparsers.add_parser(
        "get",
        help="query historical market data from a history database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code of the database to query")
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to history on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to history on or before this date")
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
    filters.add_argument(
        "-t", "--times",
        nargs="*",
        metavar="HH:MM:SS",
        help="limit to these times")
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
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    outputs.add_argument(
        "-c", "--cont-fut",
        choices=["concat"],
        metavar="HOW",
        help="stitch futures into continuous contracts using this method "
        "(default is not to stitch together). Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.history._cli_download_history_file")
