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
Create a new database for collecting real-time tick data from Interactive Brokers.

The market data requirements you specify when you create a new database are
applied each time you collect data for that database.

Examples:

Create a database for collecting real-time trades and volume for US stocks:

    quantrocket realtime create-ibkr-tick-db usa-stk-trades -u usa-stk --fields LastPrice Volume

Create a database for collecting trades and quotes for a universe of futures:

    quantrocket realtime create-ibkr-tick-db globex-fut-taq -u globex-fut --fields LastPrice Volume BidPrice AskPrice BidSize AskSize
    """
    parser = _subparsers.add_parser(
        "create-ibkr-tick-db",
        help="create a new database for collecting real-time tick data from Interactive Brokers",
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
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="collect these fields (pass '?' or any invalid fieldname to see "
        "available fields, default fields are 'LastPrice' and 'Volume')")
    parser.add_argument(
        "-p", "--primary-exchange",
        action="store_true",
        help="limit to data from the primary exchange")
    parser.set_defaults(func="quantrocket.realtime._cli_create_ibkr_tick_db")

    examples = """
Create a new database for collecting real-time tick data from Polygon.

The market data requirements you specify when you create a new database are
applied each time you collect data for that database.

Examples:

Create a database for collecting real-time trade prices and sizes for US stocks:

    quantrocket realtime create-polygon-tick-db usa-stk-trades -u usa-stk --fields LastPrice LastSize
    """
    parser = _subparsers.add_parser(
        "create-polygon-tick-db",
        help="create a new database for collecting real-time tick data from Polygon",
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
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="collect these fields (pass '?' or any invalid fieldname to see "
        "available fields, default fields are 'LastPrice' and 'LastSize')")
    parser.set_defaults(func="quantrocket.realtime._cli_create_polygon_tick_db")

    examples = """
Create an aggregate database from a tick database.

Aggregate databases provide rolled-up views of the underlying tick data,
aggregated to a desired frequency (such as 1-minute bars).

Examples:

Create an aggregate database of 1 minute bars consisting of OHLC trades and volume,
from a tick database of US stocks, resulting in fields called LastPriceOpen, LastPriceHigh,
LastPriceLow, LastPriceClose, and VolumeClose:

    quantrocket realtime create-agg-db usa-stk-trades-1min --tick-db usa-stk-trades -z 1m -f LastPrice:Open,High,Low,Close Volume:Close

Create an aggregate database of 1 second bars containing the closing bid and ask and
the mean bid size and ask size, from a tick database of futures trades and
quotes, resulting in fields called BidPriceClose, AskPriceClose, BidSizeMean, and AskSizeMean:

    quantrocket realtime create-agg-db globex-fut-taq-1sec --tick-db globex-fut-taq -z 1s -f BidPrice:Close AskPrice:Close BidSize:Mean AskSize:Mean
    """
    parser = _subparsers.add_parser(
        "create-agg-db",
        help="create an aggregate database from a tick database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the aggregate database (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-t", "--tick-db",
        metavar="CODE",
        required=True,
        dest="tick_db_code",
        help="the code of the tick database to aggregate")
    parser.add_argument(
        "-z", "--bar-size",
        metavar="BAR_SIZE",
        required=True,
        help="the time frequency to aggregate to (use a Pandas timedelta string, for example "
        "10s or 1m or 2h or 1d)")
    parser.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="include these fields in aggregate database, aggregated in these ways. Specify as a "
        "list of strings mapping tick db fields to a comma-separated list of aggregate functions "
        "to apply to the field. Format strings as 'FIELD:FUNC1,FUNC2'. Available aggregate functions "
        "are 'Close', 'Open', 'High', 'Low', 'Mean', 'Sum', and 'Count'. See examples. If not "
        "specified, defaults to including the 'Close' for each tick db field.")
    parser.set_defaults(func="quantrocket.realtime._cli_create_agg_db")

    examples = """
Return the configuration for a tick database or aggregate database.

Examples:

Return the configuration for a tick database called "globex-fut-taq":

    quantrocket realtime config globex-fut-taq

Return the configuration for an aggregate database called "globex-fut-taq-1s":

    quantrocket realtime config globex-fut-taq-1s
    """
    parser = _subparsers.add_parser(
        "config",
        help="return the configuration for a tick database or aggregate database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        help="the tick database code or aggregate database code")
    parser.set_defaults(func="quantrocket.realtime._cli_get_db_config")

    examples = """
Delete a tick database or aggregate database.

Deleting a tick database deletes its configuration and data and any
associated aggregate databases. Deleting an aggregate database does not
delete the tick database from which it is derived.

Deleting databases is irreversible.

Examples:

Delete a database called "usa-stk-trades":

    quantrocket realtime drop-db usa-stk-trades --confirm-by-typing-db-code-again usa-stk-trades
    """
    parser = _subparsers.add_parser(
        "drop-db",
        help="delete a tick database or aggregate database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        help="the tick database code or aggregate database code")
    parser.add_argument(
        "--confirm-by-typing-db-code-again",
        metavar="CODE",
        required=True,
        help="enter the db code again to confirm you want to drop the database, its config, "
        "and all its data")
    parser.add_argument(
        "--cascade",
        action="store_true",
        help="also delete associated aggregated databases, if any. Only applicable when "
       "deleting a tick database.")
    parser.set_defaults(func="quantrocket.realtime._cli_drop_db")

    examples = """
Delete ticks from a tick database. Does not delete any aggregate
database records.

Deleting ticks is a way to free up disk space by deleting ticks older
than a certain threshold while maintaining the ability to continue
collecting new ticks as well as use any aggregate databases derived from
the ticks.

Note: ticks are stored in the database in chunks, and this command only
deletes chunks in which *all* of the ticks are older than you specify. If
some of the ticks are older but some are newer, the chunk is not deleted.
This means you may still see older data returned in queries.

Examples:

Delete ticks older than 7 days in a database called 'usa-tech-stk-tick' (no
aggregate records are deleted):

    quantrocket realtime drop-ticks usa-tech-stk-tick --older-than 7d
    """
    parser = _subparsers.add_parser(
        "drop-ticks",
        help="delete ticks from a tick database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        help="the tick database code")
    parser.add_argument(
        "-o", "--older-than",
        metavar="TIMEDELTA",
        required=True,
        help="delete ticks older than this (use a Pandas timedelta string, for example "
        "7d)")
    parser.set_defaults(func="quantrocket.realtime._cli_drop_ticks")

    examples = """
List tick databases and associated aggregate databases.

Examples:

    quantrocket realtime list
    """
    parser = _subparsers.add_parser(
        "list",
        help="list tick databases and associated aggregate databases",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.realtime._cli_list_databases")

    examples = """
Collect real-time market data and save it to a tick database.

A single snapshot of market data or a continuous stream of market data can
be collected, depending on the `--snapshot` parameter. (Snapshots are not
supported for all vendors.)

Streaming real-time data is collected until cancelled, or can be scheduled
for cancellation using the `--until` parameter.

Examples:

Collect market data for all securities in a tick database called 'japan-banks-trades':

    quantrocket realtime collect japan-banks-trades

Collect market data for a subset of securities in a tick database called 'usa-stk-trades'
and automatically cancel the data collection in 30 minutes:

    quantrocket realtime collect usa-stk-trades --sids FIBBG12345 FIBBG23456 FIBBG34567 --until 30m

Collect a market data snapshot and wait until it completes:

    quantrocket realtime collect usa-stk-trades --snapshot --wait
    """
    parser = _subparsers.add_parser(
        "collect",
        help="collect real-time market data and save it to a tick database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the tick database code(s) to collect data for")
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="collect market data for these sids, overriding db config "
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
Return the number of tickers currently being collected, by vendor and database.

Examples:

    quantrocket realtime active
    """
    parser = _subparsers.add_parser(
        "active",
        help="return the number of tickers currently being collected, by vendor and database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="return lists of tickers (default is to return counts of tickers)")
    parser.set_defaults(func="quantrocket.realtime._cli_get_active_collections")

    examples = """
Cancel market data collection.

Examples:

Cancel market data collection for a tick database called 'globex-fut-taq':

    quantrocket realtime cancel globex-fut-taq

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
        help="the tick database code(s) to cancel collection for")
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="cancel market data for these sids, overriding db config")
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

    examples = """
Query market data from a tick database or aggregate database and download to file.

Examples:

Download a CSV of futures market data since 08:00 AM Chicago time:

    quantrocket realtime get globex-fut-taq --start-date '08:00:00 America/Chicago' -o globex_taq.csv
    """
    parser = _subparsers.add_parser(
        "get",
        help="query market data from a tick database or aggregate database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code of the tick database or aggregate database to query")
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to market data on or after this datetime. Can pass a date (YYYY-MM-DD), "
        "datetime with optional timezone (YYYY-MM-DD HH:MM:SS TZ), or time with "
        "optional timezone. A time without date will be interpreted as referring to "
        "today if the time is earlier than now, or yesterday if the time is later than "
        "now.")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to market data on or before this datetime. Can pass a date (YYYY-MM-DD), "
        "datetime with optional timezone (YYYY-MM-DD HH:MM:SS TZ), or time with "
        "optional timezone.")
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
    parser.set_defaults(func="quantrocket.realtime._cli_download_market_data_file")

    examples = """
Stream incoming market data.

This command does not cause data to be collected but connects to the stream of
data already being collected.

Examples:

Stream all incoming market data:

    quantrocket realtime stream

Stream a subset of fields and sids:

    quantrocket realtime stream --sids FIBBG265598 --fields BidPrice AskPrice
    """
    parser = _subparsers.add_parser(
        "stream",
        help="stream incoming market data",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    parser.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
    parser.add_argument(
        "-f", "--fields",
        nargs="*",
        metavar="FIELD",
        help="limit to these fields")
    parser.set_defaults(func="quantrocket.realtime._cli_stream_market_data")
