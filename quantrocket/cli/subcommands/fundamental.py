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
    _parser = subparsers.add_parser("fundamental", description="QuantRocket fundamental data CLI", help="Collect and query fundamental data")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Collect Reuters financial statements from IB and save to database.

This data provides cash flow, balance sheet, and income metrics.

Examples:

Collect Reuters financial statements for a universe of Japanese banks:

    quantrocket fundamental collect-financials --universes 'japan-bank'

Collect Reuters financial statements for a particular security:

    quantrocket fundamental collect-financials --conids 123456
    """
    parser = _subparsers.add_parser(
        "collect-financials",
        help="collect Reuters financial statements from IB and save to database",
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
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_reuters_financials")

    examples = """
Collect Reuters estimates and actuals from IB and save to database.

This data provides analyst estimates and actuals for a variety of indicators.

Examples:

Collect Reuters estimates and actuals for a universe of Japanese banks:

    quantrocket fundamental collect-estimates --universes 'japan-bank'

Collect Reuters estimates and actuals for a particular security:

    quantrocket fundamental collect-estimates --conids 123456
    """
    parser = _subparsers.add_parser(
        "collect-estimates",
        help="collect Reuters estimates and actuals from IB and save to database",
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
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_reuters_estimates")

    examples = """
List available Chart of Account (COA) codes from the Reuters financials database
and/or indicator codes from the Reuters estimates/actuals database

Note: you must collect Reuters financials into the database before you can
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
Collect Sharadar US Fundamentals (SF1) and save to database.

Before collecting Sharadar fundamentals, you must collect Sharadar listings
into the securities master database:

    quantrocket master collect-sharadar

You can collect fundamentals for all Sharadar listings in the securities
master database, or for a subset of universes or conids. If specifying
a subset, you must provide the --domain option to indicate which domain
your universes/conids refer to (sharadar or main).

Examples:

Collect Sharadar fundamentals for all listings in
quantrocket.master.sharadar.sqlite:

    quantrocket fundamental collect-sf1

Collect Sharadar fundamentals for a particular universe defined in
quantrocket.master.sharadar.sqlite:

    quantrocket fundamental collect-sf1 --universes 'us-banks' --domain sharadar

Collect Sharadar fundamentals for a particular conid defined in
quantrocket.master.main.sqlite:

    quantrocket fundamental collect-sf1 --conids 12345 --domain main

Re-collect complete Sharadar fundamentals history after upgrading your Sharadar
subscription to obtain deeper history:

    quantrocket fundamental collect-sf1 --rebuild
    """
    parser = _subparsers.add_parser(
        "collect-sf1",
        help="collect Sharadar US Fundamentals (SF1) and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes")
    parser.add_argument(
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="limit to these conids")
    parser.add_argument(
        "--domain",
        choices=["main","sharadar"],
        help="the domain of the universes and/or conids (required if universes "
        "or conids are provided, otherwise not allowed. Possible choices: "
        "%(choices)s)")
    parser.add_argument(
        "--rebuild",
        help="collect complete history from Sharadar (default is to collect only "
        "the updated history since the last collection). Use this option after "
        "upgrading your Sharadar SF1 subscription.")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_sharadar_sf1")

    examples = """
List available indicators from the Sharadar US Fundamentals (SF1) database.

Indicator descriptions are also available at https://www.quandl.com/databases/SF1

Examples:

List all SF1 codes:

    quantrocket fundamental sharadar-codes

List all SF1 codes from the income statement:

    quantrocket fundamental sharadar-codes -t 'Income Statement'

    """
    parser = _subparsers.add_parser(
        "sharadar-codes",
        help="list available indicators from the Sharadar US Fundamentals (SF1) "
        "database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--codes",
        nargs="*",
        metavar="CODE",
        help="limit to these indicator codes")
    parser.add_argument(
        "-t", "--indicator-types",
        nargs="*",
        metavar="INDICATOR_TYPE",
        choices=["Income Statement", "Cash Flow Statement", "Balance Sheet",
                 "Metrics", "Entity"],
        help="limit to these indicator types. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental._cli_list_sharadar_codes")

    examples = """
Query Sharadar US Fundamentals (SF1) from the local database and download to file.

The query results can be returned with IB conids or Sharadar conids, depending
on the `--domain` option, which can be "main" (the default) or "sharadar". The
`--domain` option also determines whether the `--universes` and `--conids`
options, if provided, are interpreted as referring to IB conids or Sharadar conids.

Examples:

Query as-reported trailing twelve month (ART) fundamentals for all indicators for
a particular IB conid:

    quantrocket fundamental sf1 -i 265598 -d ART -o aapl_fundamentals.csv

Query as-reported quarterly (ARQ) fundamentals for select indicators for a universe
defined in the sharadar domain:

    quantrocket fundamental sf1 -u sharadar-usa-stk --domain sharadar --dimensions ARQ -f REVENUE EPS -o sharadar_fundamentals.csv
    """
    parser = _subparsers.add_parser(
        "sf1",
        help="query Sharadar US Fundamentals (SF1) from the local database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to fundamentals on or after this fiscal period end date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to fundamentals on or before this fiscal period end date")
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
        "-d", "--dimensions",
        nargs="*",
        choices=["ARQ", "ARY", "ART", "MRQ", "MRY", "MRT"],
        help="limit to these dimensions. Possible choices: %(choices)s. "
        "AR=As Reported, MR=Most Recent Reported, Q=Quarterly, Y=Annual, "
        "T=Trailing Twelve Month. See "
        "https://www.quandl.com/databases/SF1/documentation/dimensions "
        "for more details.")
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
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields, or see `quantrocket fundamental sharadar-codes`))")
    outputs.add_argument(
        "--domain",
        choices=["main","sharadar"],
        help="the domain of the conids in which to return the results, as well as "
        "the domain which the provided universes or conids, if any, refer to. "
        "Default is 'main', which corresponds to IB conids. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_sharadar_sf1")

    examples = """
Collect IB shortable shares data and save to database.

Data is organized by country and updated every 15 minutes. Historical
data is available from April 15, 2018.

Examples:

Collect shortable shares data for US stocks:

    quantrocket fundamental collect-shortshares --countries usa

Collect shortable shares data for all stocks:

    quantrocket fundamental collect-shortshares
    """
    parser = _subparsers.add_parser(
        "collect-shortshares",
        help="collect IB shortable shares data and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        help="limit to these countries (pass '?' or any invalid country to see "
        "available countries)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_shortable_shares")

    examples = """
Collect IB borrow fees data and save to database.

Data is organized by country and updated every 15 minutes. Historical
data is available from April 15, 2018.

Examples:

Collect borrow fees for US stocks:

    quantrocket fundamental collect-shortfees --countries usa

Collect borrow fees for all stocks:

    quantrocket fundamental collect-shortfees
    """
    parser = _subparsers.add_parser(
        "collect-shortfees",
        help="collect IB borrow fees data and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        help="limit to these countries (pass '?' or any invalid country to see "
        "available countries)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_borrow_fees")

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

    examples = """
Collect Reuters financial statements from IB and save to database

[DEPRECATED] `fetch-financials` is deprecated and will be removed in a future release,
please use `collect-financials` instead.
    """
    parser = _subparsers.add_parser(
        "fetch-financials",
        help="[DEPRECATED] collect Reuters financial statements from IB and save to database",
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
Collect Reuters estimates and actuals from IB and save to database.

[DEPRECATED] `fetch-estimates` is deprecated and will be removed in a future release,
please use `collect-estimates` instead.
    """
    parser = _subparsers.add_parser(
        "fetch-estimates",
        help="[DEPRECATED] collect Reuters estimates and actuals from IB and save to database",
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
