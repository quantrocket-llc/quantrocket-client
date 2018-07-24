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
    _parser = subparsers.add_parser("fundamental", description="QuantRocket fundamental data CLI", help="Fetch and query fundamental data")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Fetch Reuters financial statements from IB and save to database.

This data provides cash flow, balance sheet, and income metrics.

Examples:

Fetch Reuters financial statements for a universe of Japanese banks:

    quantrocket fundamental fetch-financials --universes 'japan-bank'

Fetch Reuters financial statements for a particular security:

    quantrocket fundamental fetch-financials --conids 123456
    """
    parser = _subparsers.add_parser(
        "fetch-financials",
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
    parser.set_defaults(func="quantrocket.fundamental._cli_fetch_reuters_financials")

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

    examples = """
List available Chart of Account (COA) codes from the Reuters financials database
and/or indicator codes from the Reuters estimates/actuals database

Note: you must fetch Reuters financials into the database before you can
list COA codes.

Examples:

List all codes:

    quantrocket fundamental codes

List COA codes for balance sheets only:

    quantrocket fundamental codes --report-types financials --statement-types BAL

List the description of a specific COA code:

    quantrocket fundamental codes --codes TIAT
    """
    parser = _subparsers.add_parser(
        "codes",
        help="list available Chart of Account (COA) codes from the Reuters financials "
        "database and/or indicator codes from the Reuters estimates/actuals "
        "database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--codes",
        nargs="*",
        metavar="CODE",
        help="limit to these Chart of Account (COA) or indicator codes")
    parser.add_argument(
        "-r", "--report-types",
        nargs="*",
        metavar="REPORT_TYPE",
        choices=["financials", "estimates"],
        help="limit to these report types. Possible choices: %(choices)s")
    parser.add_argument(
        "-s", "--statement-types",
        nargs="*",
        metavar="STATEMENT_TYPE",
        choices=["INC", "BAL", "CAS"],
        help="limit to these statement types. Only applies to financials, not estimates. "
        "Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental._cli_list_reuters_codes")

    examples = """
Query financial statements from the Reuters financials database and
download to file.

You can query one or more COA codes. Run `quantrocket fundamental codes` to see
available codes.

Annual or interim reports are available. Annual is the default and provides
deeper history.

Examples:

Query total revenue (COA code RTLR) for a universe of Australian stocks:

    quantrocket fundamental financials RTLR -u asx-stk -s 2014-01-01 -e 2017-01-01 -o rtlr.csv

Query net income (COA code NINC) from interim reports for two securities
(identified by conid) and exclude restatements:

    quantrocket fundamental financials NINC -i 123456 234567 --interim --exclude-restatements -o ninc.csv

Query common and preferred shares outstanding (COA codes QTCO and QTPO) and return a
minimal set of fields (several required fields will always be returned)

    quantrocket fundamental financials QTCO QTPO -u nyse-stk --fields Amount -o nyse_float.csv
    """
    parser = _subparsers.add_parser(
        "financials",
        help="query financial statements from the Reuters financials database and download to file",
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
        help="limit to statements on or after this fiscal period end date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to statements on or before this fiscal period end date")
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
        help="return interim reports (default is to return annual reports, "
        "which provide deeper history)")
    filters.add_argument(
        "-r", "--exclude-restatements",
        action="store_true",
        help="exclude restatements (default is to include them)")
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
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_reuters_financials")

    examples = """
Query estimates and actuals from the Reuters estimates database and
download to file.

You can query one or more indicator codes. Run `quantrocket fundamental codes` to see
available codes.

Examples:

Query EPS estimates and actuals for a universe of Australian stocks:

    quantrocket fundamental estimates EPS -u asx-stk -s 2014-01-01 -e 2017-01-01 -o eps_estimates.csv
    """
    parser = _subparsers.add_parser(
        "estimates",
        help="query estimates and actuals from the Reuters estimates database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the indicator code(s) to query")
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to estimates and actuals on or after this fiscal period end date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to estimates and actuals on or before this fiscal period end date")
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
        "-t", "--period-types",
        nargs="*",
        choices=["A", "Q", "S"],
        metavar="PERIOD_TYPE",
        help="limit to these fiscal period types. Possible choices: %(choices)s, where "
        "A=Annual, Q=Quarterly, S=Semi-Annual")
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
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_reuters_estimates")

    examples = """
Fetch IB shortable shares data and save to database.

Data is organized by country and updated every 15 minutes. Historical
data is available from April 15, 2018.

Examples:

Fetch shortable shares data for US stocks:

    quantrocket fundamental fetch-shortshares --countries usa

Fetch shortable shares data for all stocks:

    quantrocket fundamental fetch-shortshares
    """
    parser = _subparsers.add_parser(
        "fetch-shortshares",
        help="fetch IB shortable shares data and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        help="limit to these countries (pass '?' or any invalid country to see "
        "available countries)")
    parser.set_defaults(func="quantrocket.fundamental._cli_fetch_shortable_shares")

    examples = """
Fetch IB borrow fees data and save to database.

Data is organized by country and updated every 15 minutes. Historical
data is available from April 15, 2018.

Examples:

Fetch borrow fees for US stocks:

    quantrocket fundamental fetch-shortfees --countries usa

Fetch borrow fees for all stocks:

    quantrocket fundamental fetch-shortfees
    """
    parser = _subparsers.add_parser(
        "fetch-shortfees",
        help="fetch IB borrow fees data and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        help="limit to these countries (pass '?' or any invalid country to see "
        "available countries)")
    parser.set_defaults(func="quantrocket.fundamental._cli_fetch_borrow_fees")

    examples = """
Query shortable shares from the stockloan database and download to file.

Data timestamps are UTC.

Examples:

Query shortable shares for a universe of Australian stocks:

    quantrocket fundamental shortshares -u asx-stk -o asx_shortables.csv
    """
    parser = _subparsers.add_parser(
        "shortshares",
        help="query shortable shares from the stockloan database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this date")
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
    parser.set_defaults(func="quantrocket.fundamental._cli_download_shortable_shares")

    examples = """
Query borrow fees from the stockloan database and download to file.

Data timestamps are UTC.

Examples:

Query borrow fees for a universe of Australian stocks:

    quantrocket fundamental shortfees -u asx-stk -o asx_borrow_fees.csv
    """
    parser = _subparsers.add_parser(
        "shortfees",
        help="query borrow fees from the stockloan database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this date")
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
    parser.set_defaults(func="quantrocket.fundamental._cli_download_borrow_fees")
