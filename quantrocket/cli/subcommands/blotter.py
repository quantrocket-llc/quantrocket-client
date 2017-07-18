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

from quantrocket.cli.utils.parse import parse_dict

def add_subparser(subparsers):
    _parser = subparsers.add_parser("blotter", description="QuantRocket blotter CLI", help="quantrocket blotter -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("order", help="place an order and return an order ID")
    parser.add_argument("conid", metavar="CONID", help="the contract ID")
    parser.add_argument("order", nargs="+", metavar="FIELD:VALUE", help="order details as JSON or as multiple key-value pairs (e.g. orderType:MKT tif:DAY)")
    parser.set_defaults(func="quantrocket.blotter.place_order")

    parser = _subparsers.add_parser("ordermany", help="place a batch of orders from file or stdin")
    parser.add_argument("filename", metavar="FILE", help="CSV file of orders (can also be passed on stdin)")
    parser.add_argument("-c", "--hold-child-orders-for", metavar="TIMEDELTA", help="hold child orders for up to this long (use Pandas timedelta string, e.g. 30m) and submit them as parent orders are filled (default is to submit parent and child orders together and let IB handle it)")
    parser.add_argument("--completed-statuses", nargs="*", metavar="STATUS", choices=["PendingCancel", "Cancelled", "Filled"], help="override which order statuses to treat as completed statuses. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.blotter.place_batch_orders")

    parser = _subparsers.add_parser("cancel", help="cancel an order by order ID, conid, or strategy")
    parser.add_argument("-o", "--order-id", metavar="ORDERID", help="cancel this order ID")
    parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies")
    parser.add_argument("-c", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.set_defaults(func="quantrocket.blotter.cancel_order")

    parser = _subparsers.add_parser("status", help="check an order status")
    parser.add_argument("order_id", metavar="ORDER_ID", help="the order ID")
    parser.set_defaults(func="quantrocket.blotter.get_order_status")

    parser = _subparsers.add_parser("active", help="list active orders")
    parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies (= order refs)")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    parser.add_argument("--diff-positions", action="store_true", help="only show orders which don't match up to an existing position")
    parser.set_defaults(func="quantrocket.blotter.get_active_orders")

    parser = _subparsers.add_parser("monitor", help="start monitoring order statuses and executions in real time")
    parser.set_defaults(func="quantrocket.blotter.monitor")

    parser = _subparsers.add_parser("unmonitor", help="stop monitoring order statuses and executions")
    parser.set_defaults(func="quantrocket.blotter.unmonitor")

    parser = _subparsers.add_parser("download", help="download execution details for the current day")
    parser.set_defaults(func="quantrocket.blotter.download_executions")

    parser = _subparsers.add_parser("match", help="generate round-trip trades by matching executions")
    parser.set_defaults(func="quantrocket.blotter.match_trades")

    parser = _subparsers.add_parser("positions", help="get current positions from the blotter database")
    parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    parser.add_argument("--diff-orders", action="store_true", help="only show positions which don't match up to one or more existing orders")
    parser.set_defaults(func="quantrocket.blotter.get_positions")

    parser = _subparsers.add_parser("rollover", help="generate orders to rollover futures contracts based on rollover rules")
    parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    parser.add_argument("-r", "--rules", nargs="*", metavar="KEY:VALUE", help="rollover rules as multiple key-value pairs in relativedelta format (e.g. days=-8) (omit to use rollover rules defined in master service)")
    parser.set_defaults(func="quantrocket.blotter.rollover_positions")

    parser = _subparsers.add_parser("close", help="generate orders to close positions")
    parser.add_argument("-s", "--strategies", nargs="*", metavar="CODE", help="limit to these strategies")
    parser.add_argument("-c", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("-a", "--accounts", nargs="*", metavar="ACCOUNT", help="limit to these accounts")
    parser.add_argument("-o", "--order", nargs="+", metavar="FIELD:VALUE", help="order details as JSON or as multiple key-value pairs (e.g. orderType:MKT tif:DAY)")
    parser.add_argument("--oca", dest="oca_suffix", metavar="SUFFIX", help="create OCA group containing client ID, order ID, and this suffix (run this command multiple times with this option to create OCA orders)")
    parser.set_defaults(func="quantrocket.blotter.close_positions")

    parser = _subparsers.add_parser("pnl", help="query live trading results from the blotter database")
    parser.add_argument("start_date", metavar="YYYY-MM-DD", help="start date")
    parser.add_argument("end_date", nargs="?", metavar="YYYY-MM-DD", help="end date (optional)")
    parser.add_argument("-s", "--strategies", nargs="+", metavar="CODE", help="one or more strategies to show performance for")
    parser.add_argument("-a", "--account", help="the account to show performance for (if not provided, the default account registered with the account service will be used)")
    parser.add_argument("-w", "--raw", action="store_true", help="return raw performance data instead of a performance tearsheet")
    parser.set_defaults(func="quantrocket.blotter.get_pnl")
