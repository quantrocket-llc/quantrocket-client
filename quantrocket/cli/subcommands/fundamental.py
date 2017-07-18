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

def add_subparser(subparsers):
    _parser = subparsers.add_parser("fundamental", description="QuantRocket fundamental data CLI", help="quantrocket fundamental -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("reuters", help="download Reuters fundamental data from IB")
    parser.add_argument("-r", "--reports", nargs="*", choices=["summary", "statements", "estimates"], metavar="REPORT", help="download these reports. Possible choices: %(choices)s")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("-w", "--webhook", metavar="URL", help="post to this webhook when the downloads are complete")
    parser.set_defaults(func="quantrocket.fundamental.download_reuters_fundamentals")

    parser = _subparsers.add_parser("wsh", help="download Wall Street Horizon calendar data from IB")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("-w", "--webhook", metavar="URL", help="post to this webhook when the downloads are complete")
    parser.set_defaults(func="quantrocket.fundamental.download_wsh_calendar")

    parser = _subparsers.add_parser("eps", help="query EPS history from the Reuters summary database")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to on or after this date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to on or before this date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("--report-type", nargs="*", dest="report_type", help="filter by report type")
    parser.add_argument("--period", nargs="*", help="filter by period")
    parser.set_defaults(func="quantrocket.fundamental.get_eps")

    parser = _subparsers.add_parser("dividends", help="query dividend history from the Reuters summary database")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to on or after this date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to on or before this date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("--report-type", nargs="*", dest="report_type", help="filter by report type")
    parser.add_argument("--period", nargs="*", help="filter by period")
    parser.set_defaults(func="quantrocket.fundamental.get_dividends")

    parser = _subparsers.add_parser("revenue", help="query revenue history from the Reuters summary database")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to on or after this date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to on or before this date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("--report-type", nargs="*", dest="report_type", help="filter by report type")
    parser.add_argument("--period", nargs="*", help="filter by period")
    parser.set_defaults(func="quantrocket.fundamental.get_revenue")

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