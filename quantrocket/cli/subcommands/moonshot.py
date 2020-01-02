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
from quantrocket.cli.utils.parse import dict_str, list_or_int_or_float_or_str

def add_subparser(subparsers):
    _parser = subparsers.add_parser("moonshot", description="QuantRocket Moonshot CLI", help="Backtest and trade Moonshot strategies")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Backtest one or more strategies.

By default returns a CSV of backtest results but can also return a PDF tear sheet of
performance charts.

If testing multiple strategies, each column in the CSV represents a strategy.
If testing a single strategy with the `--details` option, each column in the CSV
represents a security in the strategy universe.

Examples:

Backtest several HML (High Minus Low) strategies from 2005-2015 and return a
CSV of results:

    quantrocket moonshot backtest hml-us hml-eur hml-asia -s 2005-01-01 -e 2015-12-31 -o hml_results.csv

Backtest a single strategy called demo using all available history and return a
PDF tear sheet:

    quantrocket moonshot backtest demo --pdf -o tearsheet.pdf

Run a backtest in 1-year segments to reduce memory usage:

    quantrocket moonshot backtest big-strategy -s 2000-01-01 -e 2018-01-01 --segment A -o results.csv
    """
    parser = _subparsers.add_parser(
        "backtest",
        help="backtest one or more strategies",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "strategies",
        nargs="+",
        metavar="CODE",
        help="one or more strategy codes")
    backtest_options = parser.add_argument_group("backtest options")
    backtest_options.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="the backtest start date (default is to use all available history)")
    backtest_options.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="the backtest end date (default is to use all available history)")
    backtest_options.add_argument(
        "-g", "--segment",
        metavar="FREQ",
        help="backtest in date segments of this size, to reduce memory usage "
        "(use Pandas frequency string, e.g. 'A' for annual segments or 'Q' "
        "for quarterly segments)")
    backtest_options.add_argument(
        "-l", "--allocations",
        type=dict_str,
        metavar="CODE:FLOAT",
        nargs="*",
        help="the allocation for each strategy, passed as 'code:allocation' (default "
        "allocation is 1.0 / number of strategies)")
    backtest_options.add_argument(
        "-n", "--nlv",
        nargs="*",
        type=dict_str,
        metavar="CURRENCY:NLV",
        help="the NLV (net liquidation value, i.e. account balance) to assume for "
        "the backtest, expressed in each currency represented in the backtest (pass "
        "as 'currency:nlv')")
    backtest_options.add_argument(
        "-p", "--params",
        nargs="*",
        type=dict_str,
        metavar="PARAM:VALUE",
        help="one or more strategy params to set on the fly before backtesting "
        "(pass as 'param:value')")
    backtest_options.add_argument(
        "--no-cache",
        action="store_true",
        help="don't use cached files even if available. Using cached files speeds "
        "up backtests but may be undesirable if underlying data has changed. "
        "See http://qrok.it/h/mcache to learn more about caching in Moonshot.")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-d", "--details",
        action="store_true",
        help="return detailed results for all securities instead of aggregating to "
        "strategy level (only supported for single-strategy backtests)")
    outputs.add_argument(
        "--pdf",
        action="store_const",
        const="pdf",
        dest="output",
        help="return a PDF performance tear sheet (default is to return a CSV "
        "of performance results)")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="FILEPATH",
        dest="filepath_or_buffer",
        help="the location to write the results file (omit to write to stdout)")
    parser.set_defaults(func="quantrocket.moonshot._cli_backtest")

    examples="""
Run a parameter scan for one or more strategies.

By default returns a CSV of scan results but can also return a PDF tear sheet.

Examples:

Run a parameter scan for several different moving averages on a strategy
called trend-friend and return a PDF:

    quantrocket moonshot paramscan trend-friend -p MAVG_WINDOW -v 20 50 100 --pdf -o tearsheet.pdf

Run a 2-D parameter scan for multiple strategies and return a PDF:

    quantrocket moonshot paramscan strat1 strat2 strat3 -p MIN_STD -v 1 1.5 2 --param2 STD_WINDOW --vals2 20 50 100 200 --pdf -o tearsheet.pdf

Run a parameter scan in 1-year segments to reduce memory usage:

    quantrocket moonshot paramscan big-strategy -s 2000-01-01 -e 2018-01-01 --segment A -p MAVG_WINDOW -v 20 50 100 --pdf -o tearsheet.pdf
    """
    parser = _subparsers.add_parser(
        "paramscan",
        help="run a parameter scan for one or more strategies",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "strategies",
        nargs="+",
        metavar="CODE",
        help="one or more strategy codes")
    backtest_options = parser.add_argument_group("backtest options")
    backtest_options.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="the backtest start date (default is to use all available history)")
    backtest_options.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="the backtest end date (default is to use all available history)")
    backtest_options.add_argument(
        "-g", "--segment",
        metavar="FREQ",
        help="backtest in date segments of this size, to reduce memory usage "
        "(use Pandas frequency string, e.g. 'A' for annual segments or 'Q' "
        "for quarterly segments)")
    backtest_options.add_argument(
        "-p", "--param1",
        metavar="PARAM",
        type=str,
        required=True,
        help="the name of the parameter to test (a class attribute on the strategy)")
    backtest_options.add_argument(
        "-v", "--vals1",
        type=list_or_int_or_float_or_str,
        metavar="VALUE",
        nargs="+",
        required=True,
        help="parameter values to test (values can be integers, floats, strings, 'True', "
        "'False', 'None', or 'default' (to test current param value); for lists/tuples, "
        "use comma-separated values)")
    backtest_options.add_argument(
        "--param2",
        metavar="PARAM",
        type=str,
        help="name of a second parameter to test (for 2-D parameter scans)")
    backtest_options.add_argument(
        "--vals2",
        type=list_or_int_or_float_or_str,
        metavar="VALUE",
        nargs="*",
        help="values to test for parameter 2 (values can be integers, floats, strings, "
        "'True', 'False', 'None', or 'default' (to test current param value); for "
        "lists/tuples, use comma-separated values)")
    backtest_options.add_argument(
        "-l", "--allocations",
        type=dict_str,
        metavar="CODE:FLOAT",
        nargs="*",
        help="the allocation for each strategy, passed as 'code:allocation' (default "
        "allocation is 1.0 / number of strategies)")
    backtest_options.add_argument(
        "-n", "--nlv",
        nargs="*",
        type=dict_str,
        metavar="CURRENCY:NLV",
        help="the NLV (net liquidation value, i.e. account balance) to assume for "
        "the backtests, expressed in each currency represented in the backtest (pass "
        "as 'currency:nlv')")
    backtest_options.add_argument(
        "--params",
        nargs="*",
        type=dict_str,
        metavar="PARAM:VALUE",
        help="one or more strategy params to set on the fly before backtesting "
        "(pass as 'param:value')")
    backtest_options.add_argument(
        "--no-cache",
        action="store_true",
        help="don't use cached files even if available. Using cached files speeds "
        "up backtests but may be undesirable if underlying data has changed. "
        "See http://qrok.it/h/mcache to learn more about caching in Moonshot.")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "--pdf",
        action="store_const",
        const="pdf",
        dest="output",
        help="return a PDF tear sheet of results (default is to return a CSV)")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="FILEPATH",
        dest="filepath_or_buffer",
        help="the location to write the results file (omit to write to stdout)")
    parser.set_defaults(func="quantrocket.moonshot._cli_scan_parameters")

    examples = """
Run a walk-forward optimization of a machine learning strategy.

The date range will be split into segments of `--train` size. For each
segment, the model will be trained with the data, then the trained model will
be backtested on the following segment.

By default, uses scikit-learn's StandardScaler+SGDRegressor. Also supports other
scikit-learn models/pipelines and Keras models. To customize model, instantiate
the model locally, serialize it to disk, and pass the filename of the serialized
model as `--model`.

Supports expanding walk-forward optimizations (the default), which use an anchored start date
for model training, or rolling walk-forward optimizations (by specifying `--rolling-train`),
which use a rolling or non-anchored start date for model training.

Returns a backtest results CSV and a dump of the machine learning model
as of the end of the analysis.

Examples:

Run a walk-forward optimization using the default model and retrain the model annually,
writing the backtest results and trained model to demo_ml_results.csv and
demo_ml_trained_model.joblib, respectively:

    quantrocket moonshot ml-walkforward demo-ml -s 2007-01-01 -e 2018-12-31 --train A -o demo_ml*

Run a walk-forward optimization using a custom model (serialized with joblib), retrain the
model annually, don't perform backtesting until after 5 years of initial training,
and further split the training and backtesting into quarterly segments to reduce
memory usage:

    quantrocket moonshot ml-walkforward demo-ml -s 2007-01-01 -e 2018-12-31 --model my_model.joblib --train A --min-train 5Y --segment Q -o demo_ml*
    """
    parser = _subparsers.add_parser(
        "ml-walkforward",
        help="run a walk-forward optimization of a machine learning strategy",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "strategy",
        metavar="CODE",
        help="the strategy code")
    walkforward_options = parser.add_argument_group("walk-forward analysis options")
    walkforward_options.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        required=True,
        help="the analysis start date (note that model training will start on this date "
        "but backtesting will not start until after the initial training period)")
    walkforward_options.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        required=True,
        help="the analysis end date")
    walkforward_options.add_argument(
        "-t", "--train",
        metavar="FREQ",
        required=True,
        help="train model this frequently (use Pandas frequency string, e.g. 'A' "
        "for annual training or 'Q' for quarterly training)")
    walkforward_options.add_argument(
        "-m", "--min-train",
        metavar="FREQ",
        help="don't backtest until at least this much model training has occurred; "
        "defaults to the length of `--train` if not specified (use Pandas frequency "
        "string, e.g. '5Y' for 5 years of initial training)")
    walkforward_options.add_argument(
        "-r", "--rolling-train",
        metavar="FREQ",
        help="train model with a rolling window of this length; if omitted, train "
        "model with an expanding window (use Pandas frequency string, e.g. '3Y' for "
        "a 3-year rolling training window)")
    walkforward_options.add_argument(
        "-f", "--model",
        dest="model_filepath",
        help="filepath of serialized model to use, filename must end in '.joblib' or "
        "'.pkl' (if omitted, default model is scikit-learn's StandardScaler+SGDRegressor)")
    walkforward_options.add_argument(
        "--force-nonincremental",
        action="store_true",
        help="force the model to be trained non-incrementally (i.e. load entire training "
        "data set into memory) even if it supports incremental learning. Required "
        "in order to perform a rolling (as opposed to expanding) walk-forward optimization "
        "with a model that supports incremental learning.")
    backtest_options = parser.add_argument_group("backtest options")
    backtest_options.add_argument(
        "-g", "--segment",
        metavar="FREQ",
        help="train and backtest in date segments of this size, to reduce memory "
        "usage; must be smaller than `--train`/`--min-train` or will have no effect "
        "(use Pandas frequency string, e.g. 'A' for annual segments or 'Q' for "
        "quarterly segments)")
    backtest_options.add_argument(
        "-l", "--allocation",
        type=float,
        metavar="FLOAT",
        help="the allocation for the strategy (default 1.0)")
    backtest_options.add_argument(
        "-n", "--nlv",
        nargs="*",
        type=dict_str,
        metavar="CURRENCY:NLV",
        help="the NLV (net liquidation value, i.e. account balance) to assume for "
        "the backtest, expressed in each currency represented in the backtest (pass "
        "as 'currency:nlv')")
    backtest_options.add_argument(
        "-p", "--params",
        nargs="*",
        type=dict_str,
        metavar="PARAM:VALUE",
        help="one or more strategy params to set on the fly before backtesting "
        "(pass as 'param:value')")
    backtest_options.add_argument(
        "--no-cache",
        action="store_true",
        help="don't use cached files even if available. Using cached files speeds "
        "up backtests but may be undesirable if underlying data has changed. "
        "See http://qrok.it/h/mcache to learn more about caching in Moonshot.")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-d", "--details",
        action="store_true",
        help="return detailed results for all securities instead of aggregating")
    outputs.add_argument(
        "--progress",
        action="store_true",
        help="log status and Sharpe ratios of each walk-forward segment during "
        "analysis (default False)")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="FILEPATH",
        dest="filepath_or_buffer",
        help="the location to write the ZIP file to; or, if path ends with '*', the "
        "pattern to use for extracting the zipped files. For example, if the path is "
        "my_ml*, files will extracted to my_ml_results.csv and my_ml_trained_model.joblib.")
    parser.set_defaults(func="quantrocket.moonshot._cli_ml_walkforward")

    examples = """
Run one or more strategies and generate orders.

Allocations are read from configuration (quantrocket.moonshot.allocations.yml).

Examples:

Generate orders for a single strategy called umd-nyse:

    quantrocket moonshot trade umd-nyse -o orders.csv

Generate orders and automatically place them (if any) through the blotter:

    quantrocket moonshot trade umd-nyse | quantrocket blotter order -f -

Generate orders for multiple strategies for a particular account:

    quantrocket moonshot trade umd-japan hml-japan --accounts DU12345 -o orders.csv

Generate orders as if it were an earlier date (for prupose of review):

    quantrocket moonshot trade umd-nyse -o orders.csv --review-date 2018-05-11
    """
    parser = _subparsers.add_parser(
        "trade",
        help="run one or more strategies and generate orders.",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "strategies",
        nargs="+",
        metavar="CODE",
        help="one or more strategy codes")
    parser.add_argument(
        "-a", "--accounts",
        metavar="ACCOUNT",
        nargs="*",
        help="limit to these accounts")
    parser.add_argument(
         "-r", "--review-date",
         metavar="YYYY-MM-DD",
         help="generate orders as if it were this date, rather than using today's date")
    parser.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format orders as JSON (default is CSV)")
    parser.add_argument(
        "-o", "--outfile",
        metavar="FILEPATH",
        dest="filepath_or_buffer",
        help="the location to write the orders file (omit to write to stdout)")
    parser.set_defaults(func="quantrocket.moonshot._cli_trade")

    #parser = _subparsers.add_parser("walkforward", help="run walkforward analysis for one or more strategies")
    #parser.add_argument("start_date", metavar="YYYY-MM-DD", help="start date")
    #parser.add_argument("end_date", metavar="YYYY-MM-DD", help="end date")
    #parser.add_argument("-s", "--strategies", nargs="+", metavar="CODE", help="one or more strategies to backtest")
    #parser.add_argument("-l", "--allocations", type=float, metavar="FLOAT", nargs="*", help="the allocations for each strategy, if different from the registered allocation (must be the same length as -s/--strategies if provided)")
    #parser.add_argument("-i", "--interval", metavar="OFFSET", required=True, help="walkforward intervals as a Pandas offset string (e.g. MS, QS, 6MS, AS, see http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases)")
    #parser.add_argument("-p", "--param1", metavar="PARAM", type=str, required=True, help="name of the parameter to test")
    #parser.add_argument("-v", "--vals1", metavar="VALUE", nargs="+", help="parameter values to test")
    #parser.add_argument("-f", "--func1", metavar="PATH", help="dot-separated path of a function with which to transform the test values")
    #parser.add_argument("--rolling", action="store_true", help="use a rolling window to calculate performance (default is to use an expanding window)")
    #parser.add_argument("-r", "--rankby", choices=["cagr", "sharpe"], help="rank each period's performance by 'sharpe', 'cagr', or a 'blend' of both (default blend)")
    #parser.add_argument("-a", "--account", help="use the latest NLV of this account for modeling commissions, liquidity constraints, etc.")
    #parser.add_argument("--params", nargs="*", metavar="PARAM:VALUE", help="strategy params to set on the fly (distinct from the params to be scanned)")
    #parser.add_argument("-w", "--raw", action="store_true", help="return raw performance data instead of a performance tearsheet")
    #parser.set_defaults(func="quantrocket.moonshot.walkforward")

    #parser = _subparsers.add_parser("params", help="view params for one or more strategies")
    #parser.add_argument("codes", nargs="+", metavar="CODE", help="the strategies to include")
    #parser.add_argument("-d", "--diff", action="store_true", help="exclude params that are the same for all strategies")
    #parser.add_argument("-p", "--params", metavar="PARAM", nargs="*", help="limit to these params")
    #parser.add_argument("-g", "--group-by-class", choices=["parent", "child"], help="""
    #group by the topmost ("parent") or bottommost ("child") class in the strategy class hierarchy
    #where the param is defined (default no class grouping). (Use "parent" to group by category
    #and "child" to show where param would need to be edited.)""")
    #parser.add_argument("-s", "--groupsort", action="store_true", help="sort by origin class (requires -g/--group-by-class), otherwise sort by param name")
    #parser.set_defaults(func="quantrocket.moonshot.get_params")

    #parser = _subparsers.add_parser("shortfall", help="compare live and simulated results")
    #parser.add_argument("start_date", metavar="YYYY-MM-DD", help="start date")
    #parser.add_argument("end_date", nargs="?", metavar="YYYY-MM-DD", help="end date (optional)")
    #parser.add_argument("-s", "--strategies", nargs="+", metavar="CODE", help="one or more strategies to show shortfall for")
    #parser.add_argument("-l", "--allocations", type=float, metavar="FLOAT", nargs="*", help="the allocations for each strategy, if different from the registered allocation (must be the same length as -s/--strategies if provided)")
    #parser.add_argument("-a", "--account", help="the account to compare shortfall for (if not provided, the default account registered with the account service will be used)")
    #parser.add_argument("-p", "--params", nargs="*", metavar="PARAM:VALUE", help="strategy params to set on the fly")
    #parser.add_argument("-w", "--raw", action="store_true", help="return raw performance data instead of a performance tearsheet")
    #parser.set_defaults(func="quantrocket.moonshot.get_implementation_shortfall")
