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
    _parser = subparsers.add_parser("zipline", description="QuantRocket CLI for Zipline", help="Backtest and trade Zipline strategies")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Create a Zipline bundle for US stocks.

This command defines the bundle parameters but does not ingest the actual
data. To ingest the data, see `quantrocket zipline ingest`.

Examples:

Create a minute data bundle for all US stocks:

    quantrocket zipline create-usstock-bundle usstock-1min

Create a bundle for daily data only:

    quantrocket zipline create-usstock-bundle usstock-1d --data-frequency daily

Create a minute data bundle based on a universe:

    quantrocket zipline create-usstock-bundle usstock-tech-1min --universes us-tech

Create a minute data bundle of free sample data:

    quantrocket zipline create-usstock-bundle usstock-free-1min --free
    """
    parser = _subparsers.add_parser(
        "create-usstock-bundle",
        help="create a Zipline bundle for US stocks",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the bundle (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-i", "--sids",
        metavar="SID",
        help="limit to these sids (only supported for minute data bundles)")
    parser.add_argument(
        "-u", "--universes",
        metavar="UNIVERSE",
        help="limit to these universes (only supported for minute data bundles)")
    parser.add_argument(
        "--free",
        action="store_true",
        help="limit to free sample data")
    parser.add_argument(
        "-d", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="whether to collect minute data (which also includes daily data) or "
        "only daily data. Default is minute data. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.zipline._cli_create_usstock_bundle")

    examples = """
Create a Zipline bundle from a history database or real-time aggregate
database.

You can ingest 1-minute or 1-day databases.

This command defines the bundle parameters but does not ingest the actual
data. To ingest the data, see `quantrocket zipline ingest`.

Examples:

Create a bundle from a history database called "es-fut-1min" and name
it like the history database:

    quantrocket zipline create-bundle-from-db es-fut-1min --from-db es-fut-1min --calendar us_futures --start-date 2015-01-01

Create a bundle named "usa-stk-1min-2017" for ingesting a single year of US
1-minute stock data from a history database called "usa-stk-1min":

    quantrocket zipline create-bundle-from-db usa-stk-1min-2017 --from-db usa-stk-1min -s 2017-01-01 -e 2017-12-31 --calendar XNYS

Create a bundle from a real-time aggregate database and specify how to map
Zipline fields to the database fields:

    quantrocket zipline create-bundle-from-db free-stk-1min --from-db free-stk-tick-1min --calendar XNYS --start-date 2020-06-01 --fields close:LastPriceClose open:LastPriceOpen high:LastPriceHigh low:LastPriceLow volume:VolumeClose
    """
    parser = _subparsers.add_parser(
        "create-bundle-from-db",
        help="create a Zipline bundle from a history database or real-time aggregate database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the bundle (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-d", "--from-db",
        metavar="CODE",
        help="the code of a history database or real-time aggregate database to ingest")
    parser.add_argument(
        "-c", "--calendar",
        metavar="NAME",
        help="the name of the calendar to use with this bundle "
        "(provide '?' or any invalid calendar name to see available choices)")
    parser.add_argument(
        "-f", "--fields",
        nargs="*",
        type=dict_str,
        metavar="ZIPLINE_FIELD:DB_FIELD",
        help="mapping of Zipline fields (open, high, low, close, volume) to "
        "db fields. Pass as 'zipline_field:db_field'. Defaults to mapping Zipline "
        "'open' to db 'Open', etc.")
    filters = parser.add_argument_group("filtering options for db ingestion")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        required=True,
        help="limit to historical data on or after this date. This parameter is required "
        "and also determines the default start date for backtests and queries.")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to historical data on or before this date")
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
    parser.set_defaults(func="quantrocket.zipline._cli_create_bundle_from_db")

    examples = """
Ingest data into a previously defined bundle.

Examples:

Ingest data into a bundle called usstock-1min:

    quantrocket zipline ingest usstock-1min
    """
    parser = _subparsers.add_parser(
        "ingest",
        help="ingest data into a previously defined bundle",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code")
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids, overriding stored config")
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes, overriding stored config")
    parser.set_defaults(func="quantrocket.zipline._cli_ingest_bundle")

    examples = """
List available data bundles and whether data has been ingested into them.

Examples:

    quantrocket zipline list-bundles
    """
    parser = _subparsers.add_parser(
        "list-bundles",
        help="list available data bundles and whether data has been ingested into them",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.zipline._cli_list_bundles")

    examples = """
Return the configuration of a bundle.

Examples:

Return the configuration of a bundle called 'usstock-1min':

    quantrocket zipline config usstock-1min
    """
    parser = _subparsers.add_parser(
        "config",
        help="return the configuration of a bundle",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code")
    parser.set_defaults(func="quantrocket.zipline._cli_get_bundle_config")

    examples = """
Delete a bundle.

Examples:

Delete a bundle called 'es-fut-1min':

    quantrocket zipline drop-bundle es-fut-1min --confirm-by-typing-bundle-code-again es-fut-1min
    """
    parser = _subparsers.add_parser(
        "drop-bundle",
        help="delete a bundle",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code")
    parser.add_argument(
        "--confirm-by-typing-bundle-code-again",
        metavar="CODE",
        required=True,
        help="enter the bundle code again to confirm you want to drop the bundle, its config, "
        "and all its data")
    parser.set_defaults(func="quantrocket.zipline._cli_drop_bundle")

    examples = """
Set or show the default bundle to use for backtesting and trading.

Setting a default bundle is a convenience and is optional. It can be
overridden by manually specifying a bundle when backtesting or
trading.

Examples:

Set a bundle named usstock-1min as the default:

    quantrocket zipline default-bundle usstock-1min

Show current default bundle:

    quantrocket zipline default-bundle
    """
    parser = _subparsers.add_parser(
        "default-bundle",
        help="set or show the default bundle to use for backtesting and trading",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "bundle",
        nargs="?",
        help="the bundle code")
    parser.set_defaults(func="quantrocket.zipline._cli_get_or_set_default_bundle")

    examples = """
Query minute or daily data from a Zipline bundle and download to a CSV file.

Examples:

Download a CSV of minute prices since 2015 for a single security from a bundle called
"usstock-1min":

    quantrocket zipline get usstock-1min --start-date 2015-01-01 -i FIBBG12345 -o minute_prices.csv
    """
    parser = _subparsers.add_parser(
        "get",
        help="query minute or daily data from a Zipline bundle and download to a CSV file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code")
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
        "-d", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="whether to query minute or daily data. If omitted, defaults to "
        "minute data for minute bundles and to daily data for daily bundles. "
        "This parameter only needs to be set to request daily data from a minute "
        "bundle. Possible choices: %(choices)s")
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
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.zipline._cli_download_bundle_file")

    examples = """
Backtest a Zipline strategy and write the test results to a CSV file.

The CSV result file contains several DataFrames stacked into one: the Zipline performance
results, plus the extracted returns, transactions, positions, and benchmark returns from those
results.

Examples:

Run a backtest from a strategy file called etf-arb.py and save a CSV file of results,
logging backtest progress at annual intervals:

    quantrocket zipline backtest etf-arb --bundle arca-etf-eod -s 2010-04-01 -e 2016-02-01 -o results.csv --progress A
    """
    parser = _subparsers.add_parser(
        "backtest",
        help="backtest a Zipline strategy and write the test results to a CSV file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "strategy",
        metavar="CODE",
        help="the strategy to run (strategy filename without extension)")
    parser.add_argument(
        "-f", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="the data frequency to use. Possible choices: %(choices)s "
        "(default is minute)")
    parser.add_argument(
        "--capital-base",
        type=float,
        metavar="FLOAT",
        help="the starting capital for the simulation (default is 1e6 (1 million))")
    parser.add_argument(
        "-b", "--bundle",
        metavar="CODE",
        help="the data bundle to use for the simulation. If omitted, the default "
        "bundle (if set) is used.")
    parser.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="the start date of the simulation (defaults to the bundle start date)")
    parser.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="the end date of the simulation (defaults to today)")
    parser.add_argument(
        "-p", "--progress",
        metavar="FREQ",
        help="log backtest progress at this interval (use a pandas offset alias, "
        "for example 'D' for daily, 'W' for weeky, 'M' for monthly, 'A' for annually)")
    parser.add_argument(
        "-o", "--output",
        metavar="FILENAME",
        dest="filepath_or_buffer",
        help="the location to write the output file (omit to write to stdout)")
    parser.set_defaults(func="quantrocket.zipline._cli_backtest")

    examples = """
Create a pyfolio PDF tear sheet from a Zipline backtest result.

Examples:

Create a pyfolio tear sheet from a Zipline CSV results file:

    quantrocket zipline tearsheet results.csv -o results.pdf

Run a Zipline backtest and create a pyfolio tear sheet without saving
the CSV file:

    quantrocket zipline backtest dma -s 2010-04-01 -e 2016-02-01 | quantrocket zipline tearsheet -o dma.pdf
    """
    parser = _subparsers.add_parser(
        "tearsheet",
        help="create a pyfolio tear sheet from a Zipline backtest result",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "infilepath_or_buffer",
        metavar="FILENAME",
        nargs="?",
        default="-",
        help="the CSV file from a Zipline backtest (omit to read file from stdin)")
    parser.add_argument(
        "-o", "--output",
        metavar="FILENAME",
        required=True,
        dest="outfilepath_or_buffer",
        help="the location to write the pyfolio tear sheet")
    parser.set_defaults(func="quantrocket.zipline._cli_create_tearsheet")

    examples = """
Trade a Zipline strategy.

Examples:

Trade a strategy defined in momentum-pipeline.py:

    quantrocket zipline trade momentum-pipeline --bundle my-bundle
    """
    parser = _subparsers.add_parser(
        "trade",
        help="trade a Zipline strategy",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "strategy",
        metavar="CODE",
        help="the strategy to run (strategy filename without extension)")
    parser.add_argument(
        "-b", "--bundle",
        metavar="CODE",
        help="the data bundle to use. If omitted, the default bundle "
        "(if set) is used.")
    parser.add_argument(
        "-a", "--account",
        help="the account to run the strategy in. Only required "
        "if the strategy is allocated to more than one "
        "account in quantrocket.zipline.allocations.yml")
    parser.add_argument(
        "-f", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="the data frequency to use. Possible choices: %(choices)s "
        "(default is minute)")
    parser.set_defaults(func="quantrocket.zipline._cli_trade")

    examples = """
List actively trading Zipline strategies.

Examples:

List strategies:

    quantrocket zipline active
    """
    parser = _subparsers.add_parser(
        "active",
        help="list actively trading Zipline strategies",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.zipline._cli_list_active_strategies")

    examples = """
Cancel actively trading strategies.

Examples:

Cancel a single strategy:

    quantrocket zipline cancel --strategies momentum-pipeline

Cancel all strategies:

    quantrocket zipline cancel --all
    """
    parser = _subparsers.add_parser(
        "cancel",
        help="cancel actively trading strategies",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-s", "--strategies",
        nargs="*",
        metavar="CODE",
        help="limit to these strategies")
    parser.add_argument(
        "-a", "--accounts",
        metavar="ACCOUNT",
        nargs="*",
        help="limit to these accounts")
    parser.add_argument(
        "--all",
        action="store_true",
        dest="cancel_all",
        help="cancel all actively trading strategies")
    parser.set_defaults(func="quantrocket.zipline._cli_cancel_strategies")
