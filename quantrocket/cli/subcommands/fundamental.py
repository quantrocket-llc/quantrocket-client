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
    _parser = subparsers.add_parser("fundamental", description="QuantRocket fundamental data CLI", help="quantrocket fundamental -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Fetch Reuters fundamental data from IB and save to database.

Two report types are available:

- Financial statements: provides cash flow, balance sheet, income metrics
- Estimates and actuals: provides analyst estimates and actuals for a variety
of indicators

Examples:

Fetch all Reuters fundamental report types for a universe of Japanese banks:

    quantrocket fundamental fetch-reuters --universes 'japan-bank'

Fetch only the financial statements report for a particular security:

    quantrocket fundamental fetch-reuters --conids 123456 --reports 'statements'
    """
    parser = _subparsers.add_parser(
        "fetch-reuters",
        help="fetch Reuters fundamental data from IB and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-r", "--reports",
        nargs="*",
        choices=["statements", "estimates"],
        metavar="REPORT_TYPE",
        help="limit to these report types (default is to fetch all available). Possible "
        "choices: %(choices)s")
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes (must provide universes, conids, or both)")
    parser.add_argument(
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="limit to these conids (must provide universes, conids, or both)")
    parser.set_defaults(func="quantrocket.fundamental._cli_fetch_reuters_fundamentals")

    #parser = _subparsers.add_parser("wsh", help="download Wall Street Horizon calendar data from IB")
    #parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    #parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    #parser.set_defaults(func="quantrocket.fundamental.download_wsh_calendar")

    parser = _subparsers.add_parser("coa", help="query available Chart of Account (COA) codes from the Reuters statement database")
    parser.add_argument("-s", "--statement-type", dest="statement_type", nargs="*", help="filter by statement type")
    parser.set_defaults(func="quantrocket.fundamental.get_coa_codes")

    parser = _subparsers.add_parser("statements", help="query financial statements from the Reuters statement database")
    parser.add_argument("code", metavar="CODE", help="the Chart of Account (COA) code to query")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to on or after this available date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to on or before this available date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("--period-type", dest="period_type", nargs="*", help="filter by period type")
    parser.add_argument("--statement-type", dest="statement_type", nargs="*", help="filter by statement type")
    parser.set_defaults(func="quantrocket.fundamental.get_statements")

    parser = _subparsers.add_parser("estimates", help="query analyst estimates from the analyst estimates database")
    parser.add_argument("metric", metavar="METRIC", help="the metric code to query")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to on or after this period end date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to on or before this period end date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("--period-type", dest="period_type", metavar="TYPE", nargs="*", choices=["A", "Q"], help="filter by period type. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental.get_estimates")

    parser = _subparsers.add_parser("actuals", help="query actuals from the analyst estimates database")
    parser.add_argument("metric", metavar="METRIC", help="the metric code to query")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to on or after this period end date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to on or before this period end date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("--period-type", dest="period_type", nargs="*", metavar="TYPE", choices=["A", "Q"], help="filter by period type. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental.get_actuals")