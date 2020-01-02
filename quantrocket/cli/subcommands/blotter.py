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

    quantrocket blotter order --params Sid:FIBBG123456 Action:BUY Exchange:SMART TotalQuantity:100 OrderType:MKT Tif:Day Account:DU12345 OrderRef:my-strategy
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
Cancel one or more orders by order ID, sid, or order ref.

Examples:

Cancel orders by order ID:

    quantrocket blotter cancel -d 6002:45 6001:46

Cancel orders by sid:

    quantrocket blotter cancel -i FIBBG123456

Cancel orders by order ref:

    quantrocket blotter cancel --order-refs my-strategy

Cancel all open orders:

    quantrocket blotter cancel --all
    """
    parser = _subparsers.add_parser(
        "cancel",
        help="cancel one or more orders by order ID, sid, or order ref",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-d", "--order-ids",
        metavar="ORDER_ID",
        nargs="*",
        help="cancel these order IDs")
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="cancel orders for these sids")
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
Download order statuses.

Examples:

Download order status by order ID and save to file:

    quantrocket blotter status -d 6002:45 6001:46 -o statuses.csv

Download order status for all open orders and display in terminal:

    quantrocket blotter status --open | csvlook

Download order status with extra fields and display as YAML:

    quantrocket blotter status --open --fields Exchange LmtPrice --json | json2yaml

Download order status of open orders by sid:

    quantrocket blotter status -i FIBBG123456 --open

Download order status of open orders by order ref:

    quantrocket blotter status --order-refs my-strategy --open
    """
    parser = _subparsers.add_parser(
        "status",
        help="download order statuses",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-d", "--order-ids",
        metavar="ORDER_ID",
        nargs="*",
        help="limit to these order IDs")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to orders for these sids")
    filters.add_argument(
        "-r", "--order-refs",
        nargs="*",
        metavar="ORDER_REF",
        help="limit to orders for these order refs")
    filters.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to orders for these accounts")
    filters.add_argument(
        "--open",
        action="store_true",
        dest="open_orders",
        help="limit to open orders")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to orders submitted on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to orders submitted on or before this date")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="return these fields in addition to the default fields (pass '?' or any invalid "
        "fieldname to see available fields)")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)")
    outputs.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    parser.set_defaults(func="quantrocket.blotter._cli_download_order_statuses")

    examples = """
Query current positions.

There are two ways to view positions: blotter view (default) and broker view.

The default "blotter view" returns positions by account, sid, and order ref. Positions
are tracked based on execution records saved to the blotter database.

"Broker view" (using the `--broker` option) returns positions by account and sid (but
not order ref) as reported directly by the broker.

Examples:

Query current positions:

    quantrocket blotter positions

Save current positions to CSV file:

    quantrocket blotter positions --outfile positions.csv

Query positions for a single order ref:

    quantrocket blotter positions --order-refs my-strategy

Query positions using broker view:

    quantrocket blotter positions --broker
    """
    parser = _subparsers.add_parser(
        "positions",
        help="query current positions",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
    filters.add_argument(
        "-r", "--order-refs",
        nargs="*",
        metavar="ORDER_REF",
        help="limit to these order refs (not supported with broker view)")
    filters.add_argument(
        "-a", "--accounts",
        nargs="*",
        metavar="ACCOUNT",
        help="limit to these accounts")
    filters.add_argument(
        "--diff",
        action="store_true",
        help="limit to positions where the blotter quantity and broker quantity "
        "disagree (requires --broker)")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "--broker",
        action="store_const",
        dest="view",
        const="broker",
        help="return 'broker' view of positions (by account and sid) instead "
        "of default 'blotter' view (by account, sid, and order ref)")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)")
    outputs.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    parser.set_defaults(func="quantrocket.blotter._cli_download_positions")

    examples = """
Generate orders to close positions.

Doesn't actually place any orders but returns an orders file that can be placed
separately. Additional order parameters can be appended with the `--params` option.

Examples:

Generate MKT orders to close positions for a particular strategy:

    quantrocket blotter close --order-refs my-strategy --params OrderType:MKT Tif:DAY Exchange:SMART

Generate orders and also place them:

    quantrocket blotter close -r my-strategy -p OrderType:MKT Tif:DAY Exchange:SMART | quantrocket blotter order -f -
    """
    parser = _subparsers.add_parser(
        "close",
        help="generate orders to close positions",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
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
    outputs.add_argument(
        "-p", "--params",
        nargs="*",
        type=dict_str,
        metavar="PARAM:VALUE",
        help="additional parameters to append to each row in output "
        "(pass as 'param:value', for example OrderType:MKT)")
    outputs.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    parser.set_defaults(func="quantrocket.blotter._cli_close_positions")

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
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
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
sid.

Examples:

Get a Moonchart PDF of all trading performance PNL:

    quantrocket blotter pnl -o pnl.pdf --pdf

Get a PDF for a single account and order ref, broken down by sid:

    quantrocket blotter pnl --accounts U12345 --order-refs mystrategy1 --details --pdf -o pnl_details.pdf

Get a CSV of performance results for a particular date range:

    quantrocket blotter pnl -s 2018-03-01 -e 2018-06-30 -o pnl_2018Q2.csv
    """
    parser = _subparsers.add_parser(
        "pnl",
        help="query trading performance and return a PDF tearsheet or CSV of results",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids")
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
        help="limit to pnl on or after this date")
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to pnl on or before this date")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-d", "--details",
        action="store_true",
        help="return detailed results for all securities instead of aggregating to "
        "account/order ref level (only supported for a single account and order ref "
        "at a time)")
    outputs.add_argument(
        "-t", "--timezone",
        help="return execution times in this timezone (default UTC)")
    outputs.add_argument(
        "--pdf",
        action="store_const",
        const="pdf",
        dest="output",
        help="return a PDF tear sheet of PNL (default is to return a CSV)")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)")
    parser.set_defaults(func="quantrocket.blotter._cli_download_pnl")
