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
Fetch Reuters financial statements from IB and save to database.

This data provides cash flow, balance sheet, and income metrics.

Examples:

Fetch Reuters financial statements for a universe of Japanese banks:

    quantrocket fundamental fetch-statements --universes 'japan-bank'

Fetch Reuters financial statements for a particular security:

    quantrocket fundamental fetch-statements --conids 123456
    """
    parser = _subparsers.add_parser(
        "fetch-statements",
        help="fetch Reuters financial statements from IB and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
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
    parser.set_defaults(func="quantrocket.fundamental._cli_fetch_reuters_statements")

    examples = """
Fetch Reuters estimates and actuals from IB and save to database.

This data provides analyst estimates and actuals for a variety of indicators.

Examples:

Fetch Reuters estimates and actuals for a universe of Japanese banks:

    quantrocket fundamental fetch-estimates --universes 'japan-bank'

Fetch Reuters estimates and actuals for a particular security:

    quantrocket fundamental fetch-estimates --conids 123456
    """
    parser = _subparsers.add_parser(
        "fetch-estimates",
        help="fetch Reuters estimates and actuals from IB and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
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
    parser.set_defaults(func="quantrocket.fundamental._cli_fetch_reuters_estimates")

    #parser = _subparsers.add_parser("wsh", help="download Wall Street Horizon calendar data from IB")
    #parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    #parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    #parser.set_defaults(func="quantrocket.fundamental.download_wsh_calendar")

    examples = """
Query Chart of Account (COA) codes from the Reuters financial statements database.

Note: you must fetch Reuters financial statements into the database before you can
query COA codes.

Examples:

List all COA codes:

    quantrocket fundamental coa

List COA codes for balance sheets only:

    quantrocket fundamental coa --statement-types BAL

List the description of a specific COA code:

    quantrocket fundamental coa --codes TIAT
    """
    parser = _subparsers.add_parser(
        "coa",
        help="query Chart of Account (COA) codes from the Reuters financial statements database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--codes",
        metavar="CODE",
        help="limit to these Chart of Account (COA) codes")
    parser.add_argument(
        "-t", "--statement-types",
        nargs="*",
        metavar="STATEMENT_TYPE",
        choices=["INC", "BAL", "CAS"],
        help="limit to these statement types. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental._cli_list_coa_codes")

    examples = """
Query financial statements from the Reuters statements database and
download to file.

You can query one or more COA codes. Run `quantrocket fundamental coa` to see
available codes.

Annual or interim/quarterly reports are available. Annual is the default and
provides deeper history.

By default restatements are excluded, but they can optionally be included.

Examples:

Query total revenue (COA code RTLR) for a universe of Australian stocks:

    quantrocket fundamental statements RTLR -u asx-stk -s 2014-01-01 -e 2017-01-01 -o rtlr.csv

Query net income (COA code NINC) from interim/quarterly reports for two securities
(identified by conid) and include restatements:

    quantrocket fundamental statements NINC -i 123456 234567 --interim --restatements -o ninc.csv

Query common and preferred shares outstanding (COA codes QTCO and QTPO) and return a
minimal set of fields (several required fields will always be returned)

    quantrocket fundamental statements QTCO QTPO -u nyse-stk --fields Amount -o nyse_float.csv
    """
    parser = _subparsers.add_parser(
        "statements",
        help="query financial statements from the Reuters statements database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the Chart of Account (COA) code(s) to query")
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to statements on or after this date (based on the "
        "fiscal period end date if including restatements, otherwise the "
        "filing date)")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to statements on or before this date (based on the "
        "fiscal period end date if including restatements, otherwise the "
        "filing date)")
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
    filters.add_argument(
        "-q", "--interim",
        action="store_true",
        help="return interim/quarterly reports (default is to return annual reports, "
        "which provide deeper history)")
    filters.add_argument(
        "-r", "--restatements",
        action="store_true",
        help="include restatements (default is to exclude them)")
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
    output_format_group.add_argument(
        "-p", "--pretty",
        action="store_const",
        const="txt",
        dest="output",
        help="format output in human-readable format (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_reuters_statements")

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