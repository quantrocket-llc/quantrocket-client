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
    list_or_int_or_float_or_str,
    HelpFormatter,
)
from quantrocket._cli.utils import completers

def add_subparser(subparsers):
    _parser = subparsers.add_parser("zipline", description="QuantRocket CLI for Zipline", help="Backtest and trade Zipline strategies")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Create a Zipline bundle for US stocks.

This command defines the bundle parameters but does not ingest the actual
data. To ingest the data, see `quantrocket zipline ingest`.

Notes
-----
Usage Guide:

* US Stock bundle: https://qrok.it/dl/qr/zipline-usstock

Examples
--------

Create a minute data bundle for all US stocks:

.. code-block:: bash

    quantrocket zipline create-usstock-bundle usstock-1min

Create a bundle for daily data only:

.. code-block:: bash

    quantrocket zipline create-usstock-bundle usstock-1d --data-frequency daily

Create a minute data bundle based on a universe:

.. code-block:: bash

    quantrocket zipline create-usstock-bundle usstock-tech-1min --universes us-tech

Create a minute data bundle of free sample data (full minute history for a small number of stocks):

.. code-block:: bash

    quantrocket zipline create-usstock-bundle usstock-free-1min --free

Create the learning bundle (daily history for all stocks from 2007-2011):

.. code-block:: bash

    quantrocket zipline create-usstock-bundle usstock-learn-1d --learn
    """
    parser = _subparsers.add_parser(
        "create-usstock-bundle",
        help="create a Zipline bundle for US stocks",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the bundle (lowercase alphanumerics and hyphens only)"
        ).completer = completers.example_completer(["usstock-1min", "usstock-free-1min", "usstock-learn-1d", "usstock-1d-bundle"])
    parser.add_argument(
        "-i", "--sids",
        metavar="SID",
        nargs="*",
        help="limit to these sids (only supported for minute data bundles)"
        ).completer = completers.sid_completer
    parser.add_argument(
        "-u", "--universes",
        metavar="UNIVERSE",
        nargs="*",
        help="limit to these universes (only supported for minute data bundles)"
        ).completer = completers.universe_completer
    parser.add_argument(
        "--free",
        action="store_true",
        help="limit to free sample data")
    parser.add_argument(
        "--learn",
        action="store_true",
        help="create the learning data bundle (daily history for all stocks from 2007-2011)")
    parser.add_argument(
        "-d", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="whether to collect minute data (which also includes daily data) or "
        "only daily data. Default is minute data. Possible choices: daily, minute"
        ).completer = completers.bundle_data_frequency_completer
    parser.set_defaults(func="quantrocket.zipline._cli_create_usstock_bundle")

    examples = """
Create a Zipline bundle of daily data for Sharadar stocks and/or ETFs.

This command defines the bundle parameters but does not ingest the actual
data. To ingest the data, see `quantrocket zipline ingest`.

Notes
-----
Usage Guide:

* Sharadar bundle: https://qrok.it/dl/qr/zipline-sharadar

Examples
--------

Create a bundle for all Sharadar stocks and ETFs:

.. code-block:: bash

    quantrocket zipline create-sharadar-bundle sharadar-1d

Create a bundle for ETFs only:

.. code-block:: bash

    quantrocket zipline create-sharadar-bundle sharadar-etf-1d --sec-types ETF

Create a bundle of free sample data:

.. code-block:: bash

    quantrocket zipline create-sharadar-bundle sharadar-free-1d --free
    """
    parser = _subparsers.add_parser(
        "create-sharadar-bundle",
        help="create a Zipline bundle of daily data for Sharadar stocks and/or ETFs",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the bundle (lowercase alphanumerics and hyphens only)"
        ).completer = completers.example_completer(["sharadar-1d"])
    parser.add_argument(
        "-t", "--sec-types",
        nargs="*",
        metavar="SEC_TYPE",
        choices=["STK", "ETF"],
        help="limit to these security types. Possible choices: STK, ETF. "
        "Default is to include both stocks and ETFs.")
    parser.add_argument(
        "--free",
        action="store_true",
        help="limit to free sample data")
    parser.set_defaults(func="quantrocket.zipline._cli_create_sharadar_bundle")

    examples = """
Create a Zipline bundle from a history database or real-time aggregate
database.

You can ingest 1-minute or 1-day databases.

This command defines the bundle parameters but does not ingest the actual
data. To ingest the data, see `quantrocket zipline ingest`.

Notes
-----
Usage Guide:

* History db bundle: https://qrok.it/dl/qr/zipline-fromdb

Examples
--------

Create a bundle from a history database called "es-fut-1min" and name
it like the history database:

.. code-block:: bash

    quantrocket zipline create-bundle-from-db es-fut-1min --from-db es-fut-1min --calendar us_futures --start-date 2015-01-01

Create a bundle named "usa-stk-1min-2017" for ingesting a single year of US
1-minute stock data from a history database called "usa-stk-1min":

.. code-block:: bash

    quantrocket zipline create-bundle-from-db usa-stk-1min-2017 --from-db usa-stk-1min -s 2017-01-01 -e 2017-12-31 --calendar XNYS

Create a bundle from a real-time aggregate database and specify how to map
Zipline fields to the database fields:

.. code-block:: bash

    quantrocket zipline create-bundle-from-db free-stk-1min --from-db free-stk-tick-1min --calendar XNYS --start-date 2020-06-01 --fields close:LastPriceClose open:LastPriceOpen high:LastPriceHigh low:LastPriceLow volume:VolumeClose
    """
    parser = _subparsers.add_parser(
        "create-bundle-from-db",
        help="create a Zipline bundle from a history database or real-time aggregate database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the bundle (lowercase alphanumerics and hyphens only)"
        ).completer = completers.example_completer(["es-fut-1min-bundle", "edi-japan-1d-bundle"])
    parser.add_argument(
        "-d", "--from-db",
        metavar="CODE",
        nargs="+",
        required=True,
        help="the code(s) of one or more history databases or real-time aggregate databases "
        "to ingest. If multiple databases are specified, they must have the same bar "
        "size and same fields. If a security is present in multiple databases, the first "
        "database's values will be used.").completer = completers.history_db_completer
    parser.add_argument(
        "-c", "--calendar",
        metavar="NAME",
        help="the name of the calendar to use with this bundle "
        "(provide '?' or any invalid calendar name to see available choices)"
        ).completer = completers.exchange_calendar_completer
    parser.add_argument(
        "-f", "--fields",
        nargs="*",
        type=dict_str,
        metavar="ZIPLINE_FIELD:DB_FIELD",
        help="mapping of Zipline fields (open, high, low, close, volume) to "
        "db fields. Pass as 'zipline_field:db_field'. Defaults to mapping Zipline "
        "'open' to db 'Open', etc.").completer = completers.bundle_from_db_fields_completer
    filters = parser.add_argument_group("filtering options for db ingestion")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        required=True,
        help="limit to historical data on or after this date. This parameter is required "
        "and also determines the default start date for backtests and queries."
        ).completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to historical data on or before this date"
        ).completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    parser.set_defaults(func="quantrocket.zipline._cli_create_bundle_from_db")

    examples = """
Ingest data into a previously defined bundle.

Notes
-----
Usage Guide:

* Data bundles: https://qrok.it/dl/qr/zipline-bundles

Examples
--------

Ingest data into a bundle called usstock-1min:

.. code-block:: bash

    quantrocket zipline ingest usstock-1min
    """
    parser = _subparsers.add_parser(
        "ingest",
        help="ingest data into a previously defined bundle",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code").completer = completers.bundle_completer
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids, overriding stored config"
        ).completer = completers.sid_completer
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes, overriding stored config"
        ).completer = completers.universe_completer
    parser.set_defaults(func="quantrocket.zipline._cli_ingest_bundle")

    examples = """
List available data bundles and whether data has been ingested into them.

Notes
-----
Usage Guide:

* Data bundles: https://qrok.it/dl/qr/zipline-bundles

Examples
--------

.. code-block:: bash

    quantrocket zipline list-bundles
    """
    parser = _subparsers.add_parser(
        "list-bundles",
        help="list available data bundles and whether data has been ingested into them",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.zipline._cli_list_bundles")

    examples = """
Return the configuration of a bundle.

Notes
-----
Usage Guide:

* Data bundles: https://qrok.it/dl/qr/zipline-bundles

Examples
--------

Return the configuration of a bundle called 'usstock-1min':

.. code-block:: bash

    quantrocket zipline config usstock-1min
    """
    parser = _subparsers.add_parser(
        "config",
        help="return the configuration of a bundle",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code").completer = completers.bundle_completer
    parser.set_defaults(func="quantrocket.zipline._cli_get_bundle_config")

    examples = """
Delete a bundle.

Notes
-----
Usage Guide:

* Data bundles: https://qrok.it/dl/qr/zipline-bundles

Examples
--------

Delete a bundle called 'es-fut-1min':

.. code-block:: bash

    quantrocket zipline drop-bundle es-fut-1min --confirm-by-typing-bundle-code-again es-fut-1min
    """
    parser = _subparsers.add_parser(
        "drop-bundle",
        help="delete a bundle",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code").completer = completers.bundle_completer
    parser.add_argument(
        "--confirm-by-typing-bundle-code-again",
        metavar="CODE",
        required=True,
        help="enter the bundle code again to confirm you want to drop the bundle, its config, "
        "and all its data").completer = completers.bundle_completer
    parser.set_defaults(func="quantrocket.zipline._cli_drop_bundle")

    examples = """
Set or show the default bundle to use for backtesting and trading.

Setting a default bundle is a convenience and is optional. It can be
overridden by manually specifying a bundle when backtesting or
trading.

Notes
-----
Usage Guide:

* Data bundles: https://qrok.it/dl/qr/zipline-bundles

Examples
--------

Set a bundle named usstock-1min as the default:

.. code-block:: bash

    quantrocket zipline default-bundle usstock-1min

Show current default bundle:

.. code-block:: bash

    quantrocket zipline default-bundle
    """
    parser = _subparsers.add_parser(
        "default-bundle",
        help="set or show the default bundle to use for backtesting and trading",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "bundle",
        nargs="?",
        help="the bundle code").completer = completers.bundle_completer
    parser.set_defaults(func="quantrocket.zipline._cli_get_or_set_default_bundle")

    examples = """
List the sids in a bundle.
"""
    parser = _subparsers.add_parser(
        "sids",
        help="list the sids in a bundle",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code").completer = completers.bundle_completer
    parser.set_defaults(func="quantrocket.zipline._cli_list_sids")

    examples = """
Query minute or daily data from a Zipline bundle and download to a CSV file.

Notes
-----
Usage Guide:

* Query bundle: https://qrok.it/dl/qr/zipline-query-bundle
* get_prices: https://qrok.it/dl/qr/prices

Examples
--------

Download a CSV of minute prices since 2015 for a single security from a bundle called
"usstock-1min":

.. code-block:: bash

    quantrocket zipline get usstock-1min --start-date 2015-01-01 -i FIBBG12345 -o minute_prices.csv
    """
    fields = ["Close", "High", "Low", "Open", "Volume"]
    parser = _subparsers.add_parser(
        "get",
        help="query minute or daily data from a Zipline bundle and download to a CSV file",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the bundle code").completer = completers.bundle_completer
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to history on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to history on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-d", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="whether to query minute or daily data. If omitted, defaults to "
        "minute data for minute bundles and to daily data for daily bundles. "
        "This parameter only needs to be set to request daily data from a minute "
        "bundle. Possible choices: daily, minute"
        ).completer = completers.bundle_data_frequency_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    filters.add_argument(
        "-t", "--times",
        nargs="*",
        metavar="HH:MM:SS",
        help="limit to these times").completer = completers.example_completer(["9:30:00", "16:00:00"], "HH:MM:SS (examples only)")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv"], outfile_prefix="prices")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        choices=fields,
        help=f"only return these fields. Possible choices: {', '.join(fields)}")
    parser.set_defaults(func="quantrocket.zipline._cli_download_bundle_file")

    examples = """
Backtest a Zipline strategy and write the test results to a CSV file.

The CSV result file contains several DataFrames stacked into one: the Zipline performance
results, plus the extracted returns, transactions, positions, and benchmark returns from those
results.

Notes
-----
Usage Guide:

* Zipline backtesting: https://qrok.it/dl/qr/zipline-backtest

Examples
--------

Run a backtest from a strategy file called etf-arb.py and save a CSV file of results,
logging backtest progress at annual intervals:

.. code-block:: bash

    quantrocket zipline backtest etf-arb --bundle arca-etf-eod -s 2010-04-01 -e 2016-02-01 -o results.csv --progress A
    """
    parser = _subparsers.add_parser(
        "backtest",
        help="backtest a Zipline strategy and write the test results to a CSV file",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "strategy",
        metavar="CODE",
        help="the strategy to run (strategy filename without extension)"
        ).completer = completers.zipline_strategy_completer
    parser.add_argument(
        "-f", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="the data frequency to use. Possible choices: daily, minute "
        "(default is minute)").completer = completers.bundle_data_frequency_completer
    parser.add_argument(
        "--capital-base",
        type=float,
        metavar="FLOAT",
        help="the starting capital for the simulation (default is 1e6 (1 million))"
        ).completer = completers.zipline_capital_base_completer
    parser.add_argument(
        "-b", "--bundle",
        metavar="CODE",
        help="the data bundle to use for the simulation. If omitted, the default "
        "bundle (if set) is used.").completer = completers.bundle_completer
    parser.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="the start date of the simulation (defaults to the bundle start date)"
        ).completer = completers.start_date_completer
    parser.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="the end date of the simulation (defaults to today)"
        ).completer = completers.end_date_completer
    parser.add_argument(
        "-p", "--progress",
        metavar="FREQ",
        help="log backtest progress at this interval (use a pandas offset alias, "
        "for example 'D' for daily, 'W' for weeky, 'M' for monthly, 'A' for annually)"
        ).completer = completers.frequency_completer
    parser.add_argument(
        "--params",
        nargs="*",
        type=dict_str,
        metavar="PARAM:VALUE",
        help="one or more strategy parameters (defined as module-level attributes "
        "in the algo file) to modify on the fly before backtesting (pass as "
        "'param:value')").completer = completers.param_val_completer
    parser.add_argument(
        "-o", "--output",
        metavar="FILENAME",
        dest="filepath_or_buffer",
        help="the location to write the output file (omit to write to stdout)"
        ).completer = completers.outfile_completer(["csv"])
    parser.set_defaults(func="quantrocket.zipline._cli_backtest")

    examples = """
Run a parameter scan for a Zipline strategy. The resulting CSV can be plotted with
moonchart.ParamscanTearsheet.

Notes
-----
Usage Guide:

* Zipline parameter scans: https://qrok.it/dl/qr/zipline-paramscan

Examples
--------

Run a parameter scan for a moving average strategy called dma:

.. code-block:: bash

    quantrocket zipline paramscan dma -b usstock-1min -f daily -s 2015-01-03 -e 2022-06-30 -p MAVG_WINDOW -v 20 50 100 -o dma_MAVG_WINDOW.csv
    """
    parser = _subparsers.add_parser(
        "paramscan",
        help="run a parameter scan for a Zipline strategy",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "strategy",
        metavar="CODE",
        help="the strategy to run (strategy filename without extension)"
        ).completer = completers.zipline_strategy_completer
    parser.add_argument(
        "-f", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="the data frequency to use. Possible choices: daily, minute "
        "(default is minute)").completer = completers.bundle_data_frequency_completer
    parser.add_argument(
        "--capital-base",
        type=float,
        metavar="FLOAT",
        help="the starting capital for the simulation (default is 1e6 (1 million))"
        ).completer = completers.zipline_capital_base_completer
    parser.add_argument(
        "-b", "--bundle",
        metavar="CODE",
        help="the data bundle to use for the simulation. If omitted, the default "
        "bundle (if set) is used.").completer = completers.bundle_completer
    parser.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="the start date of the simulation (defaults to the bundle start date)"
        ).completer = completers.start_date_completer
    parser.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="the end date of the simulation (defaults to today)"
        ).completer = completers.end_date_completer
    parser.add_argument(
        "-p", "--param1",
        metavar="PARAM",
        type=str,
        required=True,
        help="the name of the parameter to test (a module-level attribute in the "
        "algo file)").completer = completers.paramscan_param_completer
    parser.add_argument(
        "-v", "--vals1",
        type=list_or_int_or_float_or_str,
        metavar="VALUE",
        nargs="+",
        required=True,
        help="parameter values to test (values can be integers, floats, strings, 'True', "
        "'False', 'None', or 'default' (to test current param value); for lists/tuples, "
        "use comma-separated values)").completer = completers.paramscan_vals_completer
    parser.add_argument(
        "--param2",
        metavar="PARAM",
        type=str,
        help="name of a second parameter to test (for 2-D parameter scans)"
        ).completer = completers.paramscan_param_completer
    parser.add_argument(
        "--vals2",
        type=list_or_int_or_float_or_str,
        metavar="VALUE",
        nargs="*",
        help="values to test for parameter 2 (values can be integers, floats, strings, "
        "'True', 'False', 'None', or 'default' (to test current param value); for "
        "lists/tuples, use comma-separated values)"
        ).completer = completers.paramscan_vals_completer
    parser.add_argument(
        "--params",
        nargs="*",
        type=dict_str,
        metavar="PARAM:VALUE",
        help="one or more strategy parameters (defined as module-level attributes "
        "in the algo file) to modify on the fly before running the parameter scan "
        "(pass as 'param:value')").completer = completers.param_val_completer
    parser.add_argument(
        "-n", "--num-workers",
        type=int,
        metavar="INT",
        help="the number of parallel workers to run. Running in parallel can speed "
        "up the parameter scan if your system has adequate resources. Default "
        "is 1, meaning no parallel processing."
        ).completer = completers.backtest_num_workers_completer
    parser.add_argument(
        "--progress",
        metavar="FREQ",
        help="log backtest progress at this interval (use a pandas offset alias, "
        "for example 'D' for daily, 'W' for weeky, 'M' for monthly, 'A' for annually). "
        "This parameter controls logging in the underlying backtests; a summary of scan "
        "results will be logged regardless of this parameter. Using this parameter when "
        "--num-workers is greater than 1 will result in messy and interleaved log output "
        "and is not recommended.").completer = completers.frequency_completer
    parser.add_argument(
        "-o", "--output",
        metavar="FILENAME",
        dest="filepath_or_buffer",
        help="the location to write the output file (omit to write to stdout)"
        ).completer = completers.outfile_completer(["csv"])
    parser.set_defaults(func="quantrocket.zipline._cli_scan_parameters")

    examples = """
Create a pyfolio PDF tear sheet from a Zipline backtest result.

Examples
--------

Create a pyfolio tear sheet from a Zipline CSV results file:

.. code-block:: bash

    quantrocket zipline tearsheet results.csv -o results.pdf

Run a Zipline backtest and create a pyfolio tear sheet without saving
the CSV file:

.. code-block:: bash

    quantrocket zipline backtest dma -s 2010-04-01 -e 2016-02-01 | quantrocket zipline tearsheet -o dma.pdf
    """
    parser = _subparsers.add_parser(
        "tearsheet",
        help="create a pyfolio tear sheet from a Zipline backtest result",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "infilepath_or_buffer",
        metavar="FILENAME",
        nargs="?",
        default="-",
        help="the CSV file from a Zipline backtest (omit to read file from stdin)"
        ).completer = completers.infile_completer(["csv"], allow_stdin=True)
    parser.add_argument(
        "-o", "--output",
        metavar="FILENAME",
        required=True,
        dest="outfilepath_or_buffer",
        help="the location to write the pyfolio tear sheet"
        ).completer = completers.outfile_completer(["pdf"])
    parser.set_defaults(func="quantrocket.zipline._cli_create_tearsheet")

    examples = """
Trade a Zipline strategy.

Notes
-----
Usage Guide:

* Zipline live trading: https://qrok.it/dl/qr/zipline-trade

Examples
--------

Trade a strategy defined in momentum-pipeline.py:

.. code-block:: bash

    quantrocket zipline trade momentum-pipeline --bundle my-bundle
    """
    parser = _subparsers.add_parser(
        "trade",
        help="trade a Zipline strategy",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "strategy",
        metavar="CODE",
        help="the strategy to run (strategy filename without extension)"
        ).completer = completers.zipline_strategy_completer
    parser.add_argument(
        "-b", "--bundle",
        metavar="CODE",
        help="the data bundle to use. If omitted, the default bundle "
        "(if set) is used.").completer = completers.bundle_completer
    parser.add_argument(
        "-a", "--account",
        help="the account to run the strategy in. Only required "
        "if the strategy is allocated to more than one "
        "account in quantrocket.zipline.allocations.yml"
        ).completer = completers.account_completer
    parser.add_argument(
        "-f", "--data-frequency",
        choices=["daily", "d", "minute", "m"],
        help="the data frequency to use. Possible choices: daily, minute. "
        "(default is minute)"
        ).completer = completers.bundle_data_frequency_completer
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="write orders to file instead of sending them to the blotter. "
        "Orders will be written to "
        "/codeload/zipline/{strategy}.{account}.orders.{date}.csv. "
        "If omitted, orders are sent to the blotter and not written to file.")
    parser.set_defaults(func="quantrocket.zipline._cli_trade")

    examples = """
List actively trading Zipline strategies.

Notes
-----
Usage Guide:

* Zipline live trading: https://qrok.it/dl/qr/zipline-trade

Examples
--------

List strategies:

.. code-block:: bash

    quantrocket zipline active
    """
    parser = _subparsers.add_parser(
        "active",
        help="list actively trading Zipline strategies",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.zipline._cli_list_active_strategies")

    examples = """
Cancel actively trading strategies.

Notes
-----
Usage Guide:

* Zipline live trading: https://qrok.it/dl/qr/zipline-trade

Examples
--------

Cancel a single strategy:

.. code-block:: bash

    quantrocket zipline cancel --strategies momentum-pipeline

Cancel all strategies:

.. code-block:: bash

    quantrocket zipline cancel --all
    """
    parser = _subparsers.add_parser(
        "cancel",
        help="cancel actively trading strategies",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-s", "--strategies",
        nargs="*",
        metavar="CODE",
        help="limit to these strategies").completer = completers.zipline_strategy_completer
    parser.add_argument(
        "-a", "--accounts",
        metavar="ACCOUNT",
        nargs="*",
        help="limit to these accounts").completer = completers.account_completer
    parser.add_argument(
        "--all",
        action="store_true",
        dest="cancel_all",
        help="cancel all actively trading strategies")
    parser.set_defaults(func="quantrocket.zipline._cli_cancel_strategies")
