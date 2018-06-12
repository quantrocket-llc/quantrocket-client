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
    _parser = subparsers.add_parser("master", description="QuantRocket securities master CLI", help="Manage and query the securities master database")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True


    examples = """
List exchanges by security type and country as found on the IB website.

Examples:

List all exchanges:

    quantrocket master exchanges

List stock exchanges in North America:

    quantrocket master exchanges --regions north_america --sec-types STK
    """
    parser = _subparsers.add_parser(
        "exchanges",
        help="list exchanges by security type and country as found on the IB website",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-r", "--regions",
        nargs="*",
        choices=["north_america", "europe", "asia", "global"],
        metavar="REGION",
        help="limit to these regions. Possible choices: %(choices)s")
    parser.add_argument(
        "-t", "--sec-types",
        nargs="*",
        choices=["STK", "ETF", "FUT", "CASH", "IND"],
        metavar="SEC_TYPE",
        help="limit to these security types. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.master._cli_list_exchanges")

    examples = """
Fetch securities listings from IB into securities master database, either by
exchange or by universes/conids.

Specify an exchange (optionally filtering by security type, currency, and/or
symbol) to fetch listings from the IB website and fetch associated contract
details from the IB API. Or, specify universes or conids to fetch details from
the IB API, bypassing the website.

Examples:

Fetch all Toronto Stock Exchange stock listings:

    quantrocket master listings --exchange TSE --sec-types STK

Fetch all NYSE ARCA ETF listings:

    quantrocket master listings --exchange ARCA --sec-types ETF

Fetch specific symbols from Nasdaq:

    quantrocket master listings --exchange NASDAQ --symbols AAPL GOOG NFLX

Re-fetch contract details for an existing universe called "japan-fin":

    quantrocket master listings --universes "japan-fin"
    """
    parser = _subparsers.add_parser(
        "listings",
        help="fetch securities listings from IB into securities master database, either by "
        "exchange or by universes/conids",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-e", "--exchange",
        metavar="EXCHANGE",
        help="the exchange code to fetch listings for (required unless providing universes "
        "or conids)")
    parser.add_argument(
        "-t", "--sec-types",
        nargs="*",
        metavar="SEC_TYPE",
        choices=["STK", "ETF", "FUT", "CASH", "IND"],
        help="limit to these security types. Possible choices: %(choices)s")
    parser.add_argument(
        "-c", "--currencies",
        nargs="*",
        metavar="CURRENCY",
        help="limit to these currencies")
    parser.add_argument(
        "-s", "--symbols",
        nargs="*",
        metavar="SYMBOL",
        help="limit to these symbols")
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
    parser.set_defaults(func="quantrocket.master._cli_fetch_listings")

    examples = """
Fetch option chains for underlying securities.

Note: option chains often consist of hundreds, sometimes thousands of options
per underlying security. Be aware that requesting option chains for large
universes of underlying securities, such as all stocks on the NYSE, can take
numerous hours to complete, add hundreds of thousands of rows to the
securities master database, increase the database file size by several
hundred megabytes, and potentially add latency to database queries.

Examples:

Fetch option chains for several underlying securities:

    quantrocket master options --conids 8314 208813720 107113386

Fetch option chains for NQ futures:

    quantrocket master get -e GLOBEX -s NQ -t FUT | quantrocket master options -f -

Fetch option chains for a large universe of stocks called "nyse-stk" (see note above):

    quantrocket master options -u "nyse-stk"
    """
    parser = _subparsers.add_parser(
        "options",
        help="fetch option chains for underlying securities",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="fetch options for these universes of underlying securities")
    parser.add_argument(
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="fetch options for these underlying conids")
    parser.add_argument(
        "-f", "--infile",
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="fetch options for the conids in this file (specify '-' to read "
        "file from stdin)")
    parser.set_defaults(func="quantrocket.master._cli_fetch_option_chains")

    examples = """
Query security details from the securities master database and download to
file.

Examples:

Download a CSV of all securities in a universe called "mexi-fut" to a file
called mexi.csv:

    quantrocket master get --universes "mexi-fut" -o mexi.csv

Download a CSV of all ARCA ETFs and use it to create a universe called
"arca-etf":

    quantrocket master get --exchanges ARCA --sec-types ETF | quantrocket master universe "arca-etf" --infile -

Pretty print the exchange and currency for all listings of AAPL:

    quantrocket master get --symbols AAPL --fields PrimaryExchange Currency --pretty
    """
    parser = _subparsers.add_parser(
        "get",
        help="query security details from the securities master database and download to file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-e", "--exchanges",
        nargs="*",
        metavar="EXCHANGE",
        help="limit to these exchanges")
    filters.add_argument(
        "-t", "--sec-types",
        nargs="*",
        metavar="SEC_TYPE",
        choices=["STK", "ETF", "FUT", "CASH", "IND", "OPT", "FOP"],
        help="limit to these security types. Possible choices: %(choices)s")
    filters.add_argument(
        "-c", "--currencies",
        nargs="*",
        metavar="CURRENCY",
        help="limit to these currencies")
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes")
    filters.add_argument(
        "-s", "--symbols",
        nargs="*",
        metavar="SYMBOL",
        help="limit to these symbols")
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
        "--sectors",
        nargs="*",
        metavar="SECTOR",
        help="limit to these sectors")
    filters.add_argument(
        "--industries",
        nargs="*",
        metavar="INDUSTRY",
        help="limit to these industries")
    filters.add_argument(
        "--categories",
        nargs="*",
        metavar="CATEGORY",
        help="limit to these categories")
    filters.add_argument(
        "-d", "--delisted",
        action="store_true",
        default=False,
        help="include delisted securities")
    filters.add_argument(
        "-m", "--frontmonth",
        action="store_true",
        default=False,
        help="exclude backmonth and expired futures contracts")
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
    parser.set_defaults(func="quantrocket.master._cli_download_master_file")

    examples = """
Flag security details that have changed in IB's system since the time they
were last loaded into the securities master database.

Diff can be run synchronously or asynchronously (asynchronous is the default
and is recommended if diffing more than a handful of securities).

Examples:

Asynchronously generate a diff for all securities in a universe called
"italy-stk" and log the results, if any, to flightlog:

    quantrocket master diff -u "italy-stk"

Asynchronously generate a diff for all securities in a universe called
"italy-stk", looking only for sector or industry changes:

    quantrocket master diff -u "italy-stk" --fields Sector Industry

Synchronously get a diff for specific securities by conid:

    quantrocket master diff --conids 123456 234567 --wait

Synchronously get a diff for specific securities without knowing their conids:

    quantrocket master get -e NASDAQ -t STK -s AAPL FB GOOG | quantrocket master diff --wait --infile -

Asynchronously generate a diff for all securities in a universe called
"nasdaq-sml" and auto-delist any symbols that are no longer available from IB
or that are now associated with the PINK exchange:

    quantrocket master diff -u "nasdaq-sml" --delist-missing --delist-exchanges PINK
    """
    parser = _subparsers.add_parser(
        "diff",
        help="flag security details that have changed in IB's system since the time "
        "they were last loaded into the securities master database",
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
        "-n", "--infile",
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="limit to the conids in this file (specify '-' to read file from stdin)")
    parser.add_argument(
        "-f", "--fields",
        nargs="*",
        metavar="FIELD",
        help="only diff these fields")
    parser.add_argument(
        "--delist-missing",
        action="store_true",
        default=False,
        help="auto-delist securities that are no longer available from IB")
    parser.add_argument(
        "--delist-exchanges",
        metavar="EXCHANGE",
        nargs="*",
        help="auto-delist securities that are associated with these exchanges")
    parser.add_argument(
        "-w", "--wait",
        action="store_true",
        default=False,
        help="run the diff synchronously and return the diff (otherwise run "
        "asynchronously and log the results, if any, to flightlog")
    parser.set_defaults(func="quantrocket.master._cli_diff_securities")

    examples = """
List universes and their size.

Examples:

List all universes and their size:

    quantrocket master list-universes
    """
    parser = _subparsers.add_parser(
        "list-universes",
        help="list universes and their size",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.master._cli_list_universes")

    examples = """
Create a universe of securities.

Examples:

Download a CSV of Italian stocks then upload it to create a universe called
"italy-stk":

    quantrocket master get --exchanges BVME --sec-types STK -f italy.csv
    quantrocket master universe "italy-stk" -f italy.csv

In one line, download a CSV of all ARCA ETFs and append to a universe called
"arca-etf":

    quantrocket master get --exchanges ARCA --sec-types ETF | quantrocket master universe "arca-etf" --append --infile -

Create a universe consisting of several existing universes:

    quantrocket master universe "asx" --from-universes "asx-sml" "asx-mid" "asx-lrg"

Copy a universe but exclude delisted securities:

    quantrocket master universe "hong-kong-active" --from-universes "hong-kong" --exclude-delisted
    """
    parser = _subparsers.add_parser(
        "universe",
        help="create a universe of securities",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the universe (lowercase alphanumerics and hyphens only)")
    parser.add_argument(
        "-f", "--infile",
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="create the universe from the conids in this file (specify '-' to read file "
        "from stdin)")
    parser.add_argument(
        "--from-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="create the universe from these existing universes")
    parser.add_argument(
        "--exclude-delisted",
        action="store_true",
        help="exclude delisted securities that would otherwise be included (default is to "
        "include them)")
    on_conflict_group = parser.add_mutually_exclusive_group()
    on_conflict_group.add_argument(
        "-a", "--append",
        action="store_true",
        help="append to universe if universe already exists")
    on_conflict_group.add_argument(
        "-r", "--replace",
        action="store_true",
        help="replace universe if universe already exists")
    parser.set_defaults(func="quantrocket.master._cli_create_universe")

    examples = """
Delete a universe. (The listings details of the member securities won't be
deleted, only their grouping as a universe).

Examples:

Delete the universe called "italy-stk" (the listings details of the member
securities won't be deleted, only their grouping as a universe):

    quantrocket master delete-universe "italy-stk"
    """
    parser = _subparsers.add_parser(
        "delete-universe",
        help="delete a universe (the listings details of the member securities won't "
        "be deleted, only their grouping as a universe)",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "code",
        help="the universe code")
    parser.set_defaults(func="quantrocket.master._cli_delete_universe")

    examples = """
Upload a new rollover rules config, or return the current rollover rules.

Examples:

Upload a new rollover config (replaces current config):

    quantrocket master rollrules myrolloverrules.yml

Show current rollover config:

    quantrocket master rollrules
    """
    parser = _subparsers.add_parser(
        "rollrules",
        help="upload a new rollover rules config, or return the current rollover rules",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "filename",
        nargs="?",
        metavar="FILENAME",
        help="the rollover rules YAML config file to upload (if omitted, return the current config)")
    parser.set_defaults(func="quantrocket.master._cli_load_or_show_rollrules")

    examples = """
Mark a security as delisted.

The security can be specified by conid or a combination of other parameters
(for example, symbol + exchange). As a precaution, the request will fail if
the parameters match more than one security.

Examples:

Delist a security by conid:

    quantrocket master delist -i 123456

Delist a security by symbol + exchange:

    quantrocket master delist -s ABC -e NYSE
    """
    parser = _subparsers.add_parser(
        "delist",
        help="mark a security as delisted",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-i", "--conid",
        type=int,
        help="the conid of the security to be delisted")
    parser.add_argument(
        "-s", "--symbol",
        help="the symbol to be delisted (if conid not provided)")
    parser.add_argument(
        "-e", "--exchange",
        help="the exchange of the security to be delisted (if needed to disambiguate)")
    parser.add_argument(
        "-c", "--currency",
        help="the currency of the security to be delisted (if needed to disambiguate)")
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=["STK", "ETF", "FUT", "CASH", "IND"],
        help="the security type of the security to be delisted (if needed to disambiguate). Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.master._cli_delist_security")

    examples = """
Fetch upcoming trading hours for exchanges and save to securites master database.

Examples:

Fetch trading hours for all exchanges in securities master database:

    quantrocket master fetch-calendar
    """
    parser = _subparsers.add_parser(
        "fetch-calendar",
        help="fetch upcoming trading hours for exchanges and save to securites master database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-e", "--exchanges",
        nargs="*",
        metavar="EXCHANGE",
        help="limit to these exchanges")
    parser.set_defaults(func="quantrocket.master._cli_fetch_calendar")

    examples = """
Check whether exchanges are open or closed.

Examples:

Check whether NYSE is open or closed now:

    quantrocket master calendar NYSE

Check whether the Tokyo Stock Exchange was open or closed 5 hours ago:

    quantrocket master calendar TSEJ --ago 5h

Check whether GLOBEX will be open or closed in 30 minutes:

    quantrocket master calendar GLOBEX --in 30min
    """
    parser = _subparsers.add_parser(
        "calendar",
        help="check whether exchanges are open or closed",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "exchanges",
        metavar="EXCHANGE",
        nargs="+",
        help="the exchange(s) to check")
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=["STK", "FUT", "CASH", "OPT"],
        help="the security type, if needed to disambiguate for exchanges that "
        "trade multiple security types. Possible choices: %(choices)s")
    timedelta_group = parser.add_mutually_exclusive_group()
    timedelta_group.add_argument(
        "-i", "--in",
        metavar="TIMEDELTA",
        dest="in_",
        help="check whether exchanges will be open or closed at this point in the "
        "future (use Pandas timedelta string, e.g. 2h or 30min or 1d)")
    timedelta_group.add_argument(
        "-a", "--ago",
        metavar="TIMEDELTA",
        help="check whether exchanges were open or closed this long ago "
        "(use Pandas timedelta string, e.g. 2h or 30min or 1d)")
    parser.add_argument(
        "-o", "--outside-rth",
        action="store_true",
        help="check extended hours calendar (default is to check regular "
        "trading hours calendar)")
    parser.set_defaults(func="quantrocket.master._cli_list_calendar_statuses")

    examples = """
Assert that one or more exchanges are open and exit non-zero if closed.

Intended to be used as a conditional for running other commands.

Examples:

Place Moonshot orders if NYSE is open now:

    quantrocket master isopen NYSE && quantrocket moonshot orders my-strategy | quantrocket blotter order -f -

Fetch historical data for Australian stocks if the exchange was open 4 hours ago:

    quantrocket master isopen ASX --ago 4h && quantrocket history fetch asx-stk-1d

Log a message if the London Stock Exchange will be open in 30 minutes:

    quantrocket master isopen LSE --in 30min && quantrocket flightlog log 'the market opens soon!'
    """
    parser = _subparsers.add_parser(
        "isopen",
        help="assert that one or more exchanges are open and exit non-zero if closed",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "exchanges",
        metavar="EXCHANGE",
        nargs="+",
        help="the exchange(s) to check")
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=["STK", "FUT", "CASH", "OPT"],
        help="the security type, if needed to disambiguate for exchanges that "
        "trade multiple security types. Possible choices: %(choices)s")
    timedelta_group = parser.add_mutually_exclusive_group()
    timedelta_group.add_argument(
        "-i", "--in",
        metavar="TIMEDELTA",
        dest="in_",
        help="assert that exchanges will be open at this point in the "
        "future (use Pandas timedelta string, e.g. 2h or 30min or 1d)")
    timedelta_group.add_argument(
        "-a", "--ago",
        metavar="TIMEDELTA",
        help="assert that exchanges were open this long ago "
        "(use Pandas timedelta string, e.g. 2h or 30min or 1d)")
    sinceuntil_group = parser.add_mutually_exclusive_group()
    sinceuntil_group.add_argument(
        "-s", "--since",
        metavar="FREQ",
        help="assert that exchanges have been opened (as of --in or --ago if "
        "applicable) since at least this time (use Pandas frequency string, "
        "e.g. 'W' (week end), 'M' (month end), 'Q' (quarter end), 'A' (year end))")
    sinceuntil_group.add_argument(
        "-u", "--until",
        metavar="FREQ",
        help="assert that exchanges will be opened (as of --in or --ago if "
        "applicable) until at least this time (use Pandas frequency string, "
        "e.g. 'W' (week end), 'M' (month end), 'Q' (quarter end), 'A' (year end))")
    parser.add_argument(
        "-o", "--outside-rth",
        action="store_true",
        help="check extended hours calendar (default is to check regular "
        "trading hours calendar)")
    parser.set_defaults(func="quantrocket.master._cli_isopen")

    examples = """
Assert that one or more exchanges are closed and exit non-zero if open.

Intended to be used as a conditional for running other commands.

For --since/--until options, pass a Pandas frequency string, i.e. any string that
is a valid `freq` argument to `pd.date_range`. See:
https://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
https://pandas.pydata.org/pandas-docs/stable/timeseries.html#anchored-offsets

Examples:

Place Moonshot orders if the NYSE will be closed NYSE in 1 hour:

    quantrocket master isclosed NYSE --in 1h && quantrocket moonshot orders my-strategy | quantrocket blotter order -f -

Fetch historical data for Australian stocks if the exchange is closed now but was
open 4 hours ago:

    quantrocket master isclosed ASX && quantrocket master isopen ASX --ago 4h && quantrocket history fetch asx-stk-1d

Place Moonshot orders if the NYSE has been closed since month end:

    quantrocket master isclosed NYSE --since M && quantrocket moonshot orders monthly-rebalancing-strategy | quantrocket blotter order -f -

Place Moonshot orders if the NYSE will be closed in 1 hour and remain closed through quarter end:

    quantrocket master isclosed NYSE --in 1H --until Q && quantrocket moonshot orders end-of-quarter-strategy | quantrocket blotter order -f -
    """
    parser = _subparsers.add_parser(
        "isclosed",
        help="assert that one or more exchanges are closed and exit non-zero if open",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "exchanges",
        metavar="EXCHANGE",
        nargs="+",
        help="the exchange(s) to check")
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=["STK", "FUT", "CASH", "OPT"],
        help="the security type, if needed to disambiguate for exchanges that "
        "trade multiple security types. Possible choices: %(choices)s")
    timedelta_group = parser.add_mutually_exclusive_group()
    timedelta_group.add_argument(
        "-i", "--in",
        metavar="TIMEDELTA",
        dest="in_",
        help="assert that exchanges will be closed at this point in the "
        "future (use Pandas timedelta string, e.g. 2h or 30min or 1d)")
    timedelta_group.add_argument(
        "-a", "--ago",
        metavar="TIMEDELTA",
        help="assert that exchanges were closed this long ago "
        "(use Pandas timedelta string, e.g. 2h or 30min or 1d)")
    sinceuntil_group = parser.add_mutually_exclusive_group()
    sinceuntil_group.add_argument(
        "-s", "--since",
        metavar="FREQ",
        help="assert that exchanges have been closed (as of --in or --ago if "
        "applicable) since at least this time (use Pandas frequency string, "
        "e.g. 'W' (week end), 'M' (month end), 'Q' (quarter end), 'A' (year end))")
    sinceuntil_group.add_argument(
        "-u", "--until",
        metavar="FREQ",
        help="assert that exchanges will be closed (as of --in or --ago if "
        "applicable) until at least this time (use Pandas frequency string, "
        "e.g. 'W' (week end), 'M' (month end), 'Q' (quarter end), 'A' (year end))")
    parser.add_argument(
        "-o", "--outside-rth",
        action="store_true",
        help="check extended hours calendar (default is to check regular "
        "trading hours calendar)")
    parser.set_defaults(func="quantrocket.master._cli_isclosed")

    examples = """
Round prices in a CSV file to valid tick sizes.

CSV should contain columns `ConId`, `Exchange`, and the columns to be rounded
(e.g. `LmtPrice`). Additional columns will be ignored and returned unchanged.

Examples:

Round the LmtPrice column in a CSV of orders and return a new CSV:

    quantrocket master ticksize -f orders.csv --round LmtPrice -o rounded_orders.csv

Round the StopPrice column in a CSV of orders and append the tick size as a
new column (called StopPriceTickSize):

    quantrocket master ticksize -f orders.csv -r StopPrice --append-ticksize -o rounded_orders.csv

Round the LmtPrice column in a CSV of Moonshot orders then place the orders:

    quantrocket moonshot orders umd-japan | quantrocket master ticksize -f - -r LmtPrice | quantrocket blotter order -f -
    """
    parser = _subparsers.add_parser(
        "ticksize",
        help="round prices in a CSV to valid tick sizes",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-f", "--infile",
        required=True,
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="CSV file with prices to be rounded (specify '-' to read file from stdin)")
    parser.add_argument(
        "-r", "--round",
        nargs="+",
        required=True,
        metavar="FIELD",
        dest="round_fields",
        help="columns to be rounded")
    parser.add_argument(
        "-d", "--how",
        metavar="DIRECTION",
        choices=["up", "down", "nearest"],
        help="which direction to round to. Possible choices: up, down, nearest "
        "(default is 'nearest')")
    parser.add_argument(
        "-a", "--append-ticksize",
        action="store_true",
        help="append a column of tick sizes for each field to be rounded")
    parser.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="outfilepath_or_buffer",
        help="filename to write the data to (default is stdout)")
    parser.set_defaults(func="quantrocket.master._cli_round_to_tick_sizes")
