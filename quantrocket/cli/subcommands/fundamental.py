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
Collect fundamental data from AtomicFin and save to database.

Examples:

    quantrocket fundamental collect-atomicfin-fundamentals
    """
    parser = _subparsers.add_parser(
        "collect-atomicfin-fundamentals",
        help="collect fundamental data from AtomicFin and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect fundamentals for. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_atomicfin_fundamentals")

    examples = """
Collect insider holdings data from AtomicFin and save to database.

Examples:

    quantrocket fundamental collect-atomicfin-insiders
    """
    parser = _subparsers.add_parser(
        "collect-atomicfin-insiders",
        help="collect insider holdings data from AtomicFin and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect insider holdings data for. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_atomicfin_insiders")

    examples = """
Collect institutional investor data from AtomicFin and save to database.

Examples:

Collect institutional investor data aggregated by security:

    quantrocket fundamental collect-atomicfin-institutions

Collect detailed institutional investor data (not aggregated by security):

    quantrocket fundamental collect-atomicfin-institutions -d
    """
    parser = _subparsers.add_parser(
        "collect-atomicfin-institutions",
        help="collect institutional investor data from AtomicFin and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect institutional investor data for. Possible choices: %(choices)s")
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="collect detailed investor data (separate record per "
        "investor per security per quarter). If omitted, "
        "collect data aggregated by security (separate record per "
        "security per quarter)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_atomicfin_institutions")

    examples = """
Collect SEC Form 8-K events from AtomicFin and save to database.

Examples:

    quantrocket fundamental collect-atomicfin-sec8
    """
    parser = _subparsers.add_parser(
        "collect-atomicfin-sec8",
        help="collect SEC Form 8-K events from AtomicFin and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect events data for. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_atomicfin_sec8")

    examples = """
Collect historical S&P 500 index constituents from AtomicFin and save to
database.

Examples:

    quantrocket fundamental collect-atomicfin-sp500
    """
    parser = _subparsers.add_parser(
        "collect-atomicfin-sp500",
        help="collect historical S&P 500 index constituents from AtomicFin and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect S&P 500 constituents data for. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_atomicfin_sp500")

    examples = """
Collect Reuters financial statements from Interactive Brokers and save to
database.

This data provides cash flow, balance sheet, and income metrics.

Examples:

Collect Reuters financial statements for a universe of Japanese banks:

    quantrocket fundamental collect-reuters-financials --universes 'japan-bank'

Collect Reuters financial statements for a particular security:

    quantrocket fundamental collect-reuters-financials --sids FIBBG123456
    """
    parser = _subparsers.add_parser(
        "collect-reuters-financials",
        help="collect Reuters financial statements from Interactive Brokers and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes (must provide universes, sids, or both)")
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids (must provide universes, sids, or both)")
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        help="collect financials for all securities even if they were collected "
        "recently (default is to skip securities that were updated in the last "
        "12 hours)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_reuters_financials")

    examples = """
Collect Reuters estimates and actuals from Interactive Brokers and save to database.

This data provides analyst estimates and actuals for a variety of indicators.

Examples:

Collect Reuters estimates and actuals for a universe of Japanese banks:

    quantrocket fundamental collect-reuters-estimates --universes 'japan-bank'

Collect Reuters estimates and actuals for a particular security:

    quantrocket fundamental collect-reuters-estimates --sids FIBBG123456
    """
    parser = _subparsers.add_parser(
        "collect-reuters-estimates",
        help="collect Reuters estimates and actuals from Interactive Brokers and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes (must provide universes, sids, or both)")
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids (must provide universes, sids, or both)")
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        help="collect estimates for all securities even if they were collected "
        "recently (default is to skip securities that were updated in the last "
        "12 hours)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_reuters_estimates")

    examples = """
Collect Wall Street Horizon upcoming earnings announcement dates from Interactive
Brokers and save to database.

Examples:

Collect upcoming earnings dates for a universe of US stocks:

    quantrocket fundamental collect-wsh --universes 'usa-stk'

Collect upcoming earnings dates for a particular security:

    quantrocket fundamental collect-wsh --sids FIBBG123456
    """
    parser = _subparsers.add_parser(
        "collect-wsh",
        help=("collect Wall Street Horizon upcoming earnings announcement dates from "
        "Interactive Brokers and save to database"),
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes (must provide universes, sids, or both)")
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids (must provide universes, sids, or both)")
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        help="collect earnings dates for all securities even if they were collected "
        "recently (default is to skip securities that were updated in the last "
        "12 hours)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_wsh_earnings_dates")

    examples = """
Collect IBKR shortable shares data and save to database.

Data is organized by country and updated every 15 minutes. Historical
data is available from April 15, 2018.

Examples:

Collect shortable shares data for US stocks:

    quantrocket fundamental collect-ibkr-shortshares --countries usa

Collect shortable shares data for all stocks:

    quantrocket fundamental collect-ibkr-shortshares
    """
    parser = _subparsers.add_parser(
        "collect-ibkr-shortshares",
        help="collect IBKR shortable shares data and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        help="limit to these countries (pass '?' or any invalid country to see "
        "available countries)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_ibkr_shortable_shares")

    examples = """
Collect IBKR borrow fees data and save to database.

Data is organized by country and updated every 15 minutes. Historical
data is available from April 15, 2018.

Examples:

Collect borrow fees for US stocks:

    quantrocket fundamental collect-ibkr-shortfees --countries usa

Collect borrow fees for all stocks:

    quantrocket fundamental collect-ibkr-shortfees
    """
    parser = _subparsers.add_parser(
        "collect-ibkr-shortfees",
        help="collect IBKR borrow fees data and save to database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        help="limit to these countries (pass '?' or any invalid country to see "
        "available countries)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_ibkr_borrow_fees")

    examples = """
Query AtomicFin Fundamentals from the local database and download to file.

Examples:

Query as-reported trailing twelve month (ART) fundamentals for all indicators for
a particular sid:

    quantrocket fundamental atomicfin-fundamentals -i FIBBG12345 --dimensions ART -o aapl_fundamentals.csv

Query as-reported quarterly (ARQ) fundamentals for select indicators for a universe:

    quantrocket fundamental atomicfin-fundamentals -u usa-stk --dimensions ARQ -f REVENUE EPS -o atomicfin_fundamentals.csv
    """
    parser = _subparsers.add_parser(
        "atomicfin-fundamentals",
        help="query AtomicFin Fundamentals from the local database and download to file",
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
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
    filters.add_argument(
        "-m", "--dimensions",
        nargs="*",
        choices=["ARQ", "ARY", "ART", "MRQ", "MRY", "MRT"],
        help="limit to these dimensions. Possible choices: %(choices)s. "
        "AR=As Reported, MR=Most Recent Reported, Q=Quarterly, Y=Annual, "
        "T=Trailing Twelve Month.")
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
        "available fields))")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_atomicfin_fundamentals")

    examples = """
Query AtomicFin insider holdings data from the local database and download
to file.

Examples:

Query insider holdings data for a particular sid:

    quantrocket fundamental atomicfin-insiders -i FIBBG000B9XRY4 -o aapl_insiders.csv
    """
    parser = _subparsers.add_parser(
        "atomicfin-insiders",
        help="query AtomicFin insider holdings data from the local database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this filing date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this filing date")
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
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
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_atomicfin_insiders")

    examples = """
Query AtomicFin institutional investor data from the local database and
download to file.

Examples:

Query institutional investor data aggregated by security:

    quantrocket fundamental atomicfin-institutions -u usa-stk -s 2019-01-01 -o institutions.csv
    """
    parser = _subparsers.add_parser(
        "atomicfin-institutions",
        help="query AtomicFin institutional investor data from the local database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this quarter end date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this quarter end date")
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
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
        "available fields)")
    outputs.add_argument(
        "-d", "--detail",
        action="store_true",
        help="query detailed investor data (separate record per "
        "investor per security per quarter). If omitted, "
        "query data aggregated by security (separate record per "
        "security per quarter)"
    )
    parser.set_defaults(func="quantrocket.fundamental._cli_download_atomicfin_institutions")

    examples = """
Query AtomicFin SEC Form 8-K events data from the local database and download
to file.

Examples:

Query event code 13 (Bankruptcy) for a universe of securities:

    quantrocket fundamental atomicfin-sec8 -u usa-stk --event-codes 13 -o bankruptcies.csv
    """
    parser = _subparsers.add_parser(
        "atomicfin-sec8",
        help="query AtomicFin SEC Form 8-K events data from the local database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this filing date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this filing date")
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
    filters.add_argument(
        "-c", "--event-codes",
        nargs="*",
        type=int,
        metavar="INT",
        help="limit to these event codes")
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
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_atomicfin_sec8")

    examples = """
Query AtomicFin S&P 500 index changes (additions and removals) from the
local database and download to file.

Examples:

Query S&P 500 index changes since 2010:

    quantrocket fundamental atomicfin-sp500 -s 2010-01-01 -o sp500_changes.csv
    """
    parser = _subparsers.add_parser(
        "atomicfin-sp500",
        help="query AtomicFin S&P 500 index changes (additions and removals) from the local database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to index changes on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to index changes on or before this date")
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
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
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_atomicfin_sp500")

    examples = """
List available Chart of Account (COA) codes from the Reuters financials database
and/or indicator codes from the Reuters estimates/actuals database

Note: you must collect Reuters financials into the database before you can
list COA codes.

Examples:

List all codes:

    quantrocket fundamental reuters-codes

List COA codes for balance sheets only:

    quantrocket fundamental reuters-codes --report-types financials --statement-types BAL

List the description of a specific COA code:

    quantrocket fundamental reuters-codes --codes TIAT
    """
    parser = _subparsers.add_parser(
        "reuters-codes",
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

You can query one or more COA codes. Run `quantrocket fundamental reuters-codes` to see
available codes.

Annual or interim reports are available. Annual is the default and provides
deeper history.

Examples:

Query total revenue (COA code RTLR) for a universe of Australian stocks:

    quantrocket fundamental reuters-financials RTLR -u asx-stk -s 2014-01-01 -e 2017-01-01 -o rtlr.csv

Query net income (COA code NINC) from interim reports for two securities
(identified by sid) and exclude restatements:

    quantrocket fundamental reuters-financials NINC -i FIBBG123456 FIBBG234567 --interim --exclude-restatements -o ninc.csv

Query common and preferred shares outstanding (COA codes QTCO and QTPO) and return a
minimal set of fields (several required fields will always be returned)

    quantrocket fundamental reuters-financials QTCO QTPO -u nyse-stk --fields Amount -o nyse_float.csv
    """
    parser = _subparsers.add_parser(
        "reuters-financials",
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
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
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

You can query one or more indicator codes. Run `quantrocket fundamental reuters-codes` to see
available codes.

Examples:

Query EPS estimates and actuals for a universe of Australian stocks:

    quantrocket fundamental reuters-estimates EPS -u asx-stk -s 2014-01-01 -e 2017-01-01 -o eps_estimates.csv
    """
    parser = _subparsers.add_parser(
        "reuters-estimates",
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
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
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
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_reuters_estimates")

    examples = """
Query earnings announcement dates from the Wall Street Horizon
announcements database and download to file.

Examples:

Query earnings dates for a universe of US stocks:

    quantrocket fundamental wsh -u usa-stk -s 2019-01-01 -e 2019-04-01 -o announcements.csv
    """
    parser = _subparsers.add_parser(
        "wsh",
        help="query earnings announcement dates from the Wall Street Horizon announcements database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to announcements on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to announcements on or before this date")
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
    filters.add_argument(
        "-t", "--statuses",
        nargs="*",
        choices=["Confirmed", "Unconfirmed"],
        metavar="STATUS",
        help="limit to these confirmation statuses. Possible choices: %(choices)s")
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
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_wsh_earnings_dates")

    examples = """
Query IBKR shortable shares from the local database and download to file.

Data timestamps are UTC.

Examples:

Query shortable shares for a universe of Australian stocks:

    quantrocket fundamental ibkr-shortshares -u asx-stk -o asx_shortables.csv
    """
    parser = _subparsers.add_parser(
        "ibkr-shortshares",
        help="query IBKR shortable shares from the local database and download to file",
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
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
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
    parser.set_defaults(func="quantrocket.fundamental._cli_download_ibkr_shortable_shares")

    examples = """
Query IBKR borrow fees from the local database and download to file.

Data timestamps are UTC.

Examples:

Query borrow fees for a universe of Australian stocks:

    quantrocket fundamental ibkr-shortfees -u asx-stk -o asx_borrow_fees.csv
    """
    parser = _subparsers.add_parser(
        "ibkr-shortfees",
        help="query IBKR borrow fees from the local database and download to file",
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
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes")
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids")
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
    parser.set_defaults(func="quantrocket.fundamental._cli_download_ibkr_borrow_fees")
