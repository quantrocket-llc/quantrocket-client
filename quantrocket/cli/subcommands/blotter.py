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
    _parser = subparsers.add_parser("blotter", description="QuantRocket blotter CLI", help="quantrocket blotter -h")
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

    quantrocket blotter order --params ConId:123456 Action:BUY TotalQuantity:100 OrderType:MKT Tif:Day Account:DU12345 OrderRef:my-strategy
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
    parser.set_defaults(func="quantrocket.blotter._cli_list_order_statuses")

    #parser = _subparsers.add_parser("active", help="list active orders")
    #parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies (= order refs)")
    #parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    #parser.add_argument("--diff-positions", action="store_true", help="only show orders which don't match up to an existing position")
    #parser.set_defaults(func="quantrocket.blotter.get_active_orders")

    #parser = _subparsers.add_parser("positions", help="get current positions from the blotter database")
    #parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies")
    #parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    #parser.add_argument("--diff-orders", action="store_true", help="only show positions which don't match up to one or more existing orders")
    #parser.set_defaults(func="quantrocket.blotter.get_positions")

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

    #parser = _subparsers.add_parser("pnl", help="query live trading results from the blotter database")
    #parser.add_argument("start_date", metavar="YYYY-MM-DD", help="start date")
    #parser.add_argument("end_date", nargs="?", metavar="YYYY-MM-DD", help="end date (optional)")
    #parser.add_argument("-s", "--strategies", nargs="+", metavar="CODE", help="one or more strategies to show performance for")
    #parser.add_argument("-a", "--account", help="the account to show performance for (if not provided, the default account registered with the account service will be used)")
    #parser.add_argument("-w", "--raw", action="store_true", help="return raw performance data instead of a performance tearsheet")
    #parser.set_defaults(func="quantrocket.blotter.get_pnl")
