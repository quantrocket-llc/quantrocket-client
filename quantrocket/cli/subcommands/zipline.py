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
    _parser = subparsers.add_parser("zipline", description="QuantRocket CLI for Zipline", help="Backtest and trade with Zipline")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Ingest a history database into Zipline for later backtesting.

You can ingest 1-minute or 1-day history databases.

Re-ingesting a previously ingested database will create a new version of the
ingested data, while preserving the earlier version. See
`quantrocket zipline clean` to remove earlier versions.

Ingestion parameters (start_date, end_date, universes, conids, exclude_universes,
exclude_conids) can only be specified the first time a bundle is ingested, and
will be reused for subsequent ingestions. You must remove the bundle and start
over to change the parameters.

Examples:

Ingest a history database called "arca-etf-eod" into Zipline:

    quantrocket zipline ingest --history-db arca-etf-eod --calendar NYSE

Re-ingest "arca-etf-eod" (calendar and other ingestion parameters aren't
needed as they will be re-used from the first ingestion):

    quantrocket zipline ingest --history-db arca-etf-eod

Ingest a history database called "lse-stk" into Zipline and associate it with
the LSE calendar:

    quantrocket zipline ingest --history-db lse-stk --calendar LSE

Ingest a single year of US 1-minute stock data and name the bundle usa-stk-2017:

    quantrocket zipline ingest --history-db usa-stk-1min -s 2017-01-01 -e 2017-12-31 --bundle usa-stk-2017 --calendar NYSE

Re-ingest the bundle usa-stk-2017:

    quantrocket zipline ingest --bundle usa-stk-2017
    """
    parser = _subparsers.add_parser(
        "ingest",
        help="ingest a history database into Zipline",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-d", "--history-db",
        metavar="CODE",
        help="the code of a history db to ingest")
    parser.add_argument(
        "-c", "--calendar",
        metavar="NAME",
        help="the name of the calendar to use with this history db bundle "
        "(provide '?' or any invalid calendar name to see available choices)")
    parser.add_argument(
        "-b", "--bundle",
        metavar="NAME",
        help="the name to assign to the bundle (defaults to the history "
        "database code)")
    filters = parser.add_argument_group("filtering options for history db ingestion")
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
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="limit to these conids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="exclude these conids")
    parser.set_defaults(func="quantrocket.zipline._cli_ingest_bundle")

    examples = """
List all of the available data bundles.

Examples:

    quantrocket zipline bundles
    """
    parser = _subparsers.add_parser(
        "bundles",
        help="list all of the available data bundles",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.zipline._cli_list_bundles")

    examples = """
Remove previously ingested data for one or more bundles.

Examples:

Remove all but the last ingestion for a bundle called 'aus-1min':

    quantrocket zipline clean -b aus-1min --keep-last 1

Remove all ingestions for bundles called 'aus-1min' and 'usa-1min':

    quantrocket zipline clean -b aus-1min usa-1min --all
    """
    parser = _subparsers.add_parser(
        "clean",
        help="remove previously ingested data for one or more bundles",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-b", "--bundles",
        nargs="+",
        metavar="NAME",
        help="the data bundle(s) to clean")
    parser.add_argument(
        "-e", "--before",
        metavar="YYYY-MM-DD[ HH:MM:SS]",
        help="clear all data before this timestamp. Mutually exclusive with keep_last "
        "and clean_all")
    parser.add_argument(
        "-a", "--after",
        metavar="YYYY-MM-DD[ HH:MM:SS]",
        help="clear all data after this timestamp. Mutually exclusive with keep_last "
        "and clean_all.")
    parser.add_argument(
        "-k", "--keep-last",
        metavar="N",
        type=int,
        help="clear all but the last N ingestions. Mutually exclusive with before, "
        "after, and clean_all")
    parser.add_argument(
        "--all",
        action="store_true",
        dest="clean_all",
        help="clear all ingestions for bundle(s), and delete bundle configuration. "
        "Default False. Mutually exclusive with before, after, and keep_last.")
    parser.set_defaults(func="quantrocket.zipline._cli_clean_bundles")

    examples = """
Run a Zipline backtest and write the test results to a CSV file.

The CSV result file contains several DataFrames stacked into one: the Zipline performance
results, plus the extracted returns, transactions, positions, and benchmark returns from those
results.

Examples:

Run a backtest from an algo file called etf_arb.py and save a CSV file of results:

    quantrocket zipline run --bundle 'arca-etf-eod' -f 'etf_arb.py' -s 2010-04-01 -e 2016-02-01 -o results.csv

Run a backtest using the us_futures calendar:

    quantrocket zipline run --bundle 'cl-rb-1day' -f 'futures_pairs_trading.py' --calendar us_futures -s 2015-04-01 -e 2016-02-01 -o results.csv
    """
    parser = _subparsers.add_parser(
        "run",
        help="run a Zipline backtest and write the test results to a CSV file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-f", "--algofile",
        required=True,
        metavar="FILENAME",
        help="the file that contains the algorithm to run")
    parser.add_argument(
        "--data-frequency",
        choices=["daily", "minute"],
        help="the data frequency of the simulation (default is daily)")
    parser.add_argument(
        "--capital-base",
        type=float,
        metavar="FLOAT",
        help="the starting capital for the simulation (default is 10000000.0)")
    parser.add_argument(
        "-b", "--bundle",
        metavar="BUNDLE-NAME",
        required=True,
        help="the data bundle to use for the simulation")
    parser.add_argument(
        "--bundle-timestamp",
        metavar="TIMESTAMP",
        help="the date to lookup data on or before (default is <current-time>)")
    parser.add_argument(
        "-s", "--start",
        required=True,
        metavar="DATE",
        help="the start date of the simulation")
    parser.add_argument(
        "-e", "--end",
        required=True,
        metavar="DATE",
        help="the end date of the simulation")
    parser.add_argument(
        "-o", "--output",
        metavar="FILENAME",
        dest="filepath_or_buffer",
        help="the location to write the output file (omit to write to stdout)")
    parser.add_argument(
        "--calendar",
        metavar="CALENDAR",
        help="the calendar you want to use e.g. LSE (default is to use the calendar "
        "associated with the data bundle)")
    parser.set_defaults(func="quantrocket.zipline._cli_run_algorithm")

    examples = """
Create a pyfolio PDF tear sheet from a Zipline backtest result.

Examples:

Create a pyfolio tear sheet from a Zipline CSV results file:

    quantrocket zipline tearsheet results.csv -o results.pdf

Run a Zipline backtest and create a pyfolio tear sheet without saving
the CSV file:

    quantrocket zipline run -f 'buy_aapl.py' -s 2010-04-01 -e 2016-02-01 | quantrocket zipline tearsheet -o buy_aapl.pdf
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
