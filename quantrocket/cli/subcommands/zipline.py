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
Ingest a data bundle into Zipline for later backtesting.

You can ingest 1-minute or 1-day history databases from QuantRocket, or you
can ingest data using Zipline's built-in capabilities.

Examples:

Ingest a history database called "arca-etf-eod" into Zipline:

    quantrocket zipline ingest --history-db 'arca-etf-eod'

Ingest a history database called "lse-stk" into Zipline and associate it with
the LSE calendar:

    quantrocket zipline ingest --history-db 'lse-stk' --calendar 'LSE'

Ingest the quantopian-quandl bundle into Zipline:

    quantrocket zipline ingest -b 'quantopian-quandl'
    """
    parser = _subparsers.add_parser(
        "ingest",
        help="ingest a data bundle",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-d", "--history-db",
        metavar="CODE",
        help="the code of a history db to ingest")
    parser.add_argument(
        "-c", "--calendar",
        metavar="NAME",
        help="the name of the calendar to use with this history db bundle (default is "
        "NYSE; provide an invalid calendar name to see available choices)")
    parser.add_argument(
        "-b", "--bundle",
        metavar="BUNDLE-NAME",
        help="the data bundle to ingest (default is quantopian-quandl); don't provide "
        "if specifying --history-db")
    parser.add_argument(
        "--assets-versions",
        metavar="INTEGER",
        nargs="*",
        help="versions of the assets db to which to downgrade")
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
Clean up data downloaded with the ingest command.

Examples:

Remove all but the last bundle called 'aus-1min':

    quantrocket zipline clean -b 'aus-1min' --keep-last 1
    """
    parser = _subparsers.add_parser(
        "clean",
        help="clean up data downloaded with the ingest command",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-b", "--bundle",
        metavar="BUNDLE-NAME",
        help="the data bundle to clean (default is quantopian-quandl)")
    parser.add_argument(
        "-e", "--before",
        metavar="TIMESTAMP",
        help="clear all data before TIMESTAMP. This may not be passed with -k / --keep-last")
    parser.add_argument(
        "-a", "--after",
        metavar="TIMESTAMP",
        help="clear all data after TIMESTAMP. This may not be passed with -k / --keep-last")
    parser.add_argument(
        "-k", "--keep-last",
        metavar="N",
        type=int,
        help="clear all but the last N downloads. This may not be passed with -e / --before "
        "or -a / --after")
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
        help="the data bundle to use for the simulation (default is quantopian-quandl)")
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

Create a full pyfolio tear sheet from a Zipline CSV results file:

    quantrocket zipline tearsheet results.csv -o results.pdf

Create a simple pyfolio tear sheet:

    quantrocket zipline tearsheet results.csv -o results.pdf --simple

Run a Zipline backtest and create a full pyfolio tear sheet without saving
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
    parser.add_argument(
        "-s", "--simple",
        action="store_true",
        help="create a simple tear sheet (default is to create a full tear sheet)"),
    parser.add_argument(
        "-l", "--live-start-date",
        metavar="YYYY-MM-DD",
        help="date when the strategy began live trading")
    parser.add_argument(
        "--slippage",
        metavar="INT",
        type=float,
        help="basis points of slippage to apply to returns before generating tear sheet "
        "stats and plots")
    parser.add_argument(
        "--hide-positions",
        action="store_true",
        help="don't output any symbol names"),
    parser.add_argument(
        "--bayesian",
        action="store_true",
        help="include a Bayesian tear sheet"),
    parser.add_argument(
        "--round-trips",
        action="store_true",
        help="include a round-trips tear sheet"),
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="perform bootstrap analysis for the performance metrics (takes a few minutes "
        "longer)")
    parser.set_defaults(func="quantrocket.zipline._cli_create_tearsheet")
