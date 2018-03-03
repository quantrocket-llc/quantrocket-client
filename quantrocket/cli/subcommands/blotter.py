# Copyright 2018 QuantRocket - All Rights Reserved
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
from quantrocket.cli.utils.parse import dict_str

def add_subparser(subparsers):
    _parser = subparsers.add_parser("blotter", description="QuantRocket blotter CLI", help="Place orders and track executions")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Place one or more orders.

Returns a list of order IDs, which can be used to cancel the orders or check
their status.

Examples:

Place orders from a CSV file.

    quantrocket blotter order -f orders.csv

Place orders from a JSON file.

    quantrocket blotter order -f orders.json

Place an order by specifying the order parameters on the command line:

    quantrocket blotter order --params ConId:123456 Action:BUY Exchange:SMART TotalQuantity:100 OrderType:MKT Tif:Day Account:DU12345 OrderRef:my-strategy
    """
    parser = _subparsers.add_parser(
        "order",
        help="place one or more orders",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "-f", "--infile",
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="place orders from this CSV or JSON file (specify '-' to read file "
            "from stdin)")
    source_group.add_argument(
        "-p", "--params",
        nargs="*",
        type=dict_str,
        metavar="PARAM:VALUE",
        help="order details as multiple key-value pairs (pass as 'param:value', for "
        "example OrderType:MKT)")
    parser.set_defaults(func="quantrocket.blotter._cli_place_orders")

    examples = """
Cancel one or more orders by order ID, conid, or order ref.

Examples:

Cancel orders by order ID:

    quantrocket blotter cancel -o 6002:45 6001:46

Cancel orders by conid:

    quantrocket blotter cancel -i 123456

Cancel orders by order ref:

    quantrocket blotter cancel --order-refs my-strategy

Cancel all open orders:

    quantrocket blotter cancel --all
    """
    parser = _subparsers.add_parser(
        "cancel",
        help="cancel one or more orders by order ID, conid, or order ref",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-o", "--order-ids",
        metavar="ORDER_ID",
        nargs="*",
        help="cancel these order IDs")
    parser.add_argument(
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="cancel orders for these conids")
    parser.add_argument(
        "-r", "--order-refs",
        nargs="*",
        metavar="ORDER_REF",
        help="cancel orders for these order refs")
    parser.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="cancel orders for these accounts")
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        dest="cancel_all",
        help="cancel all open orders")
    parser.set_defaults(func="quantrocket.blotter._cli_cancel_orders")

    examples = """
List order status for one or more orders by order ID, conid, order ref, or account.

Examples:

List order status by order ID:

    quantrocket blotter status -o 6002:45 6001:46

List order status for all open orders:

    quantrocket blotter status --open

List order status of open orders by conid:

    quantrocket blotter status -i 123456 --open

List order status of open orders by order ref:

    quantrocket blotter status --order-refs my-strategy --open
    """
    parser = _subparsers.add_parser(
        "status",
        help="List order status for one or more orders by order ID, conid, "
        "order ref, or account",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-o", "--order-ids",
        metavar="ORDER_ID",
        nargs="*",
        help="limit to these order IDs")
    parser.add_argument(
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="limit to orders for these conids")
    parser.add_argument(
        "-r", "--order-refs",
        nargs="*",
        metavar="ORDER_REF",
        help="limit to orders for these order refs")
    parser.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to orders for these accounts")
    parser.add_argument(
        "--open",
        action="store_true",
        dest="open_orders",
        help="limit to open orders (default False, must be True if order_ids not provided)")
    parser.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="return these fields in addition to the default fields (pass '?' or any invalid "
        "fieldname to see available fields)")
    parser.set_defaults(func="quantrocket.blotter._cli_list_order_statuses")

    examples = """
Query current positions.

Examples:

Query current positions in human-readable format:

    quantrocket blotter positions --pretty

Save current positions to CSV file:

    quantrocket blotter positions --outfile positions.csv

Query positions for a single order ref:

    quantrocket blotter positions --order-refs my-strategy
    """
    parser = _subparsers.add_parser(
        "positions",
        help="query current positions",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="limit to these conids")
    filters.add_argument(
        "-r", "--order-refs",
        nargs="*",
        metavar="ORDER_REF",
        help="limit to these order refs")
    filters.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to these accounts")
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
    parser.set_defaults(func="quantrocket.blotter._cli_download_positions")

    examples = """
Query executions from the executions database.

Examples:

Get a CSV of all executions:

    quantrocket blotter executions -o executions.csv
    """
    parser = _subparsers.add_parser(
        "executions",
        help="query executions from the executions database",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="limit to these conids")
    filters.add_argument(
        "-r", "--order-refs",
        nargs="*",
        metavar="ORDER_REF",
        help="limit to these order refs")
    filters.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to these accounts")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to executions on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to executions on or before this date")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)")
    parser.set_defaults(func="quantrocket.blotter._cli_download_executions")

    examples = """
Query trading performance and return a PDF tearsheet or CSV of results.

Trading performance is broken down by account and order ref and optionally by
conid.

Examples:

Get a Moonchart PDF of all trading performance PNL:

    quantrocket blotter pnl -o pnl.pdf

Get a PDF for a single account and order ref, broken down by conid:

    quantrocket blotter pnl --accounts U12345 --order-refs mystrategy1 --details -o pnl_details.pdf

Get a CSV of performance results for a particular date range:

    quantrocket blotter pnl -s 2018-03-01 -e 2018-06-30 --csv -o pnl_2018Q2.csv

Calculate daily performance as of 4PM Eastern time (instead of the default 11:59:59 UTC):

    quantrocket blotter pnl --time '16:00:00 America/New_York' -o pnl.pdf
    """
    parser = _subparsers.add_parser(
        "pnl",
        help="query trading performance and return a PDF tearsheet or CSV of results",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-i", "--conids",
        type=int,
        nargs="*",
        metavar="CONID",
        help="limit to these conids")
    filters.add_argument(
        "-r", "--order-refs",
        nargs="*",
        metavar="ORDER_REF",
        help="limit to these order refs")
    filters.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to these accounts")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to history on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to history on or before this date")
    filters.add_argument(
        "-t", "--time",
        metavar="HH:MM:SS [TZ]",
        help="time of day with optional timezone to calculate daily PNL (default is "
        "11:59:59 UTC)")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-d", "--details",
        action="store_true",
        help="return detailed results for all securities instead of aggregating to "
        "account/order ref level (only supported for a single account and order ref "
        "at a time)")
    outputs.add_argument(
        "--csv",
        action="store_true",
        help="return a CSV of PNL (default is to return a PDF "
        "performance tear sheet)")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)")
    parser.set_defaults(func="quantrocket.blotter._cli_download_pnl")

    #parser = _subparsers.add_parser("rollover", help="generate orders to rollover futures contracts based on rollover rules")
    #parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies")
    #parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    #parser.add_argument("-r", "--rules", nargs="*", metavar="KEY:VALUE", help="rollover rules as multiple key-value pairs in relativedelta format (e.g. days=-8) (omit to use rollover rules defined in master service)")
    #parser.set_defaults(func="quantrocket.blotter.rollover_positions")

    #parser = _subparsers.add_parser("close", help="generate orders to close positions")
    #parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies")
    #parser.add_argument("-c", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    #parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    #parser.add_argument("-o", "--order", nargs="+", metavar="FIELD:VALUE", help="order details as JSON or as multiple key-value pairs (e.g. orderType:MKT tif:DAY)")
    #parser.add_argument("--oca", dest="oca_suffix", metavar="SUFFIX", help="create OCA group containing client ID, order ID, and this suffix (run this command multiple times with this option to create OCA orders)")
    #parser.set_defaults(func="quantrocket.blotter.close_positions")
