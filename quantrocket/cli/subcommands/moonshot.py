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

from quantrocket.cli.utils.parse import parse_dict

def add_subparser(subparsers):
    _parser = subparsers.add_parser("moonshot", description="QuantRocket Moonshot CLI", help="quantrocket moonshot -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("backtest", help="backtest one or more strategies")
    parser.add_argument("start_date", metavar="YYYY-MM-DD", help="start date")
    parser.add_argument("end_date", metavar="YYYY-MM-DD", help="end date")
    parser.add_argument("-s", "--strategies", nargs="+", metavar="CODE", help="one or more strategies to backtest")
    parser.add_argument("-l", "--allocations", type=float, metavar="FLOAT", nargs="*", help="the allocations for each strategy, if different from the registered allocation (must be the same length as -s/--strategies if provided)")
    parser.add_argument("-a", "--account", help="use the latest NLV of this account for modeling commissions, liquidity constraints, etc.")
    parser.add_argument("-p", "--params", nargs="*", type=parse_dict, metavar="PARAM:VALUE", help="strategy params to set on the fly")
    parser.add_argument("-w", "--raw", action="store_true", help="return raw performance data instead of a performance tearsheet")
    parser.set_defaults(func="quantrocket.moonshot.backtest")

    parser = _subparsers.add_parser("paramscan", help="run a parameter scan for one or more strategies")
    parser.add_argument("start_date", metavar="YYYY-MM-DD", help="start date")
    parser.add_argument("end_date", metavar="YYYY-MM-DD", help="end date")
    parser.add_argument("-s", "--strategies", nargs="+", metavar="CODE", help="one or more strategies to backtest")
    parser.add_argument("-l", "--allocations", type=float, metavar="FLOAT", nargs="*", help="the allocations for each strategy, if different from the registered allocation (must be the same length as -s/--strategies if provided)")
    parser.add_argument("-p", "--param1", metavar="PARAM", type=str, required=True, help="name of the parameter to test")
    parser.add_argument("-v", "--vals1", metavar="VALUE", nargs="+", help="parameter values to test")
    parser.add_argument("-f", "--func1", metavar="PATH", help="dot-separated path of a function with which to transform the test values")
    parser.add_argument("--param2", metavar="PARAM", type=str, help="name of a second parameter to test")
    parser.add_argument("--vals2", metavar="VALUE", nargs="*", help="values to test for the second parameter")
    parser.add_argument("--func2", metavar="PATH", help="dot-separated path of a function with which to transform the test values for the second parameter")
    parser.add_argument("-a", "--account", help="use the latest NLV of this account for modeling commissions, liquidity constraints, etc.")
    parser.add_argument("--params", nargs="*", type=parse_dict, metavar="PARAM:VALUE", help="strategy params to set on the fly (distinct from the params to be scanned)")
    parser.add_argument("-w", "--raw", action="store_true", help="return raw performance data instead of a performance tearsheet")
    parser.set_defaults(func="quantrocket.moonshot.scan_parameters")

    parser = _subparsers.add_parser("walkforward", help="run walkforward analysis for one or more strategies")
    parser.add_argument("start_date", metavar="YYYY-MM-DD", help="start date")
    parser.add_argument("end_date", metavar="YYYY-MM-DD", help="end date")
    parser.add_argument("-s", "--strategies", nargs="+", metavar="CODE", help="one or more strategies to backtest")
    parser.add_argument("-l", "--allocations", type=float, metavar="FLOAT", nargs="*", help="the allocations for each strategy, if different from the registered allocation (must be the same length as -s/--strategies if provided)")
    parser.add_argument("-i", "--interval", metavar="OFFSET", required=True, help="walkforward intervals as a Pandas offset string (e.g. MS, QS, 6MS, AS, see http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases)")
    parser.add_argument("-p", "--param1", metavar="PARAM", type=str, required=True, help="name of the parameter to test")
    parser.add_argument("-v", "--vals1", metavar="VALUE", nargs="+", help="parameter values to test")
    parser.add_argument("-f", "--func1", metavar="PATH", help="dot-separated path of a function with which to transform the test values")
    parser.add_argument("--rolling", action="store_true", help="use a rolling window to calculate performance (default is to use an expanding window)")
    parser.add_argument("-r", "--rankby", choices=["cagr", "sharpe"], help="rank each period's performance by 'sharpe', 'cagr', or a 'blend' of both (default blend)")
    parser.add_argument("-a", "--account", help="use the latest NLV of this account for modeling commissions, liquidity constraints, etc.")
    parser.add_argument("--params", nargs="*", type=parse_dict, metavar="PARAM:VALUE", help="strategy params to set on the fly (distinct from the params to be scanned)")
    parser.add_argument("-w", "--raw", action="store_true", help="return raw performance data instead of a performance tearsheet")
    parser.set_defaults(func="quantrocket.moonshot.walkforward")

    parser = _subparsers.add_parser("params", help="view params for one or more strategies")
    parser.add_argument("codes", nargs="+", metavar="CODE", help="the strategies to include")
    parser.add_argument("-d", "--diff", action="store_true", help="exclude params that are the same for all strategies")
    parser.add_argument("-p", "--params", metavar="PARAM", nargs="*", help="limit to these params")
    parser.add_argument("-g", "--group-by-class", choices=["parent", "child"], help="""
    group by the topmost ("parent") or bottommost ("child") class in the strategy class hierarchy
    where the param is defined (default no class grouping). (Use "parent" to group by category
    and "child" to show where param would need to be edited.)""")
    parser.add_argument("-s", "--groupsort", action="store_true", help="sort by origin class (requires -g/--group-by-class), otherwise sort by param name")
    parser.set_defaults(func="quantrocket.moonshot.get_params")

    parser = _subparsers.add_parser("trade", help="run one or more strategies and generate orders")
    parser.add_argument("strategies", nargs="*", metavar="CODE", help="one or more strategies to trade")
    parser.add_argument("-q", "--quotes", action="store_true", help="get realtime quotes asap and append to price history")
    parser.add_argument("-t", "--quotes-at", metavar="HH:MM:SS TZ", help="get realtime quotes at the specified time and append to price history")
    parser.add_argument("-f", "--quotes-func", metavar="PATH", help="dot-separated path of a function through which the quotes should be passed before appending them to price history")
    parser.add_argument("-s", "--save-quotes", metavar="DB", help="instruct the realtime service to save the realtime quotes to this price history database")
    parser.add_argument("-r", "--review-date", metavar="YYYY-MM-DD", help="generate trades as if it were this date")
    parser.set_defaults(func="quantrocket.moonshot.trade")

    parser = _subparsers.add_parser("shortfall", help="compare live and simulated results")
    parser.add_argument("start_date", metavar="YYYY-MM-DD", help="start date")
    parser.add_argument("end_date", nargs="?", metavar="YYYY-MM-DD", help="end date (optional)")
    parser.add_argument("-s", "--strategies", nargs="+", metavar="CODE", help="one or more strategies to show shortfall for")
    parser.add_argument("-l", "--allocations", type=float, metavar="FLOAT", nargs="*", help="the allocations for each strategy, if different from the registered allocation (must be the same length as -s/--strategies if provided)")
    parser.add_argument("-a", "--account", help="the account to compare shortfall for (if not provided, the default account registered with the account service will be used)")
    parser.add_argument("-p", "--params", nargs="*", type=parse_dict, metavar="PARAM:VALUE", help="strategy params to set on the fly")
    parser.add_argument("-w", "--raw", action="store_true", help="return raw performance data instead of a performance tearsheet")
    parser.set_defaults(func="quantrocket.moonshot.get_implementation_shortfall")
