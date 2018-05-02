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
    _parser = subparsers.add_parser("launchpad", description="QuantRocket IB Gateway service CLI", help="Start and stop IB Gateway")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Query statuses of IB Gateway services.

Examples:

List the status of all gateways:

    quantrocket launchpad status

Get a list of gateways that are running and have permission for NYSE market
data and Reuters research (multiple search criteria are ANDed together)

    quantrocket launchpad status --exchanges NYSE --research reuters --status running
    """
    parser = _subparsers.add_parser(
        "status",
        help=" query statuses of IB Gateway services",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-e", "--exchanges",
        metavar="EXCHANGE",
        nargs="*",
        help="limit to IB Gateway services with market data permission for these exchanges")
    parser.add_argument(
        "-t", "--sec-type",
        dest="sec_type",
        metavar="SEC_TYPE",
        choices=["STK", "FUT", "CASH", "OPT", "IND"],
        help="limit to IB Gateway services with market data permission for this securitiy "
        "type (useful for disambiguating permissions for exchanges that trade multiple asset "
        "classes). Possible choices: %(choices)s")
    parser.add_argument(
        "-r", "--research",
        metavar="VENDOR",
        dest="research_vendors",
        nargs="*",
        choices=["reuters", "wsh"],
        help="limit to IB Gateway services with permission for these research vendors. Possible choices: %(choices)s")
    parser.add_argument(
        "-s", "--status",
        choices=["running", "stopped", "error"],
        help="limit to IB Gateway services in this status. Possible choices: %(choices)s")
    parser.add_argument(
        "-g", "--gateways",
        metavar="SERVICE_NAME",
        nargs="*",
        help="limit to these IB Gateway services")
    parser.set_defaults(func="quantrocket.launchpad._cli_list_gateway_statuses")

    examples = """
Start one or more IB Gateway services.

Examples:

Asynchronously start all gateways (that aren't already running):

    quantrocket launchpad start

Start specific gateways and wait for them to come up:

    quantrocket launchpad start --gateways ibg1 ibg3 --wait

Start gateways with Japan stock permissions:

    quantrocket launchpad start --exchanges TSEJ --wait

Restart all gateways:

    quantrocket launchpad stop --wait && quantrocket launchpad start
    """
    parser = _subparsers.add_parser(
        "start",
        help="start one or more IB Gateway services",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-e", "--exchanges",
        metavar="EXCHANGE",
        nargs="*",
        help="limit to IB Gateway services with market data permission for these exchanges")
    parser.add_argument(
        "-t", "--sec-type",
        dest="sec_type",
        metavar="SEC_TYPE",
        choices=["STK", "FUT", "CASH", "OPT", "IND"],
        help="limit to IB Gateway services with market data permission for this securitiy "
        "type (useful for disambiguating permissions for exchanges that trade multiple asset "
        "classes). Possible choices: %(choices)s")
    parser.add_argument(
        "-r", "--research",
        metavar="VENDOR",
        dest="research_vendors",
        nargs="*",
        choices=["reuters", "wsh"],
        help="limit to IB Gateway services with permission for these research vendors. Possible choices: %(choices)s")
    parser.add_argument(
        "-g", "--gateways",
        metavar="SERVICE_NAME",
        nargs="*",
        help="limit to these IB Gateway services")
    parser.add_argument(
        "-w", "--wait",
        action="store_true",
        default=False,
        help="wait for the IB Gateway services to start before returning (default is to "
        "start the gateways asynchronously)")
    parser.set_defaults(func="quantrocket.launchpad._cli_start_gateways")

    examples = """
Stop one or more IB Gateway services.

Examples:

Stop all gateways (that aren't already stopped):

    quantrocket launchpad stop

Stop specific gateways and wait for them to stop:

    quantrocket launchpad stop --gateways ibg1 ibg3 --wait

Stop gateways with Japan stock permissions:

    quantrocket launchpad stop --exchanges TSEJ --wait
    """
    parser = _subparsers.add_parser(
        "stop",
        help="stop one or more IB Gateway service",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-e", "--exchanges",
        metavar="EXCHANGE",
        nargs="*",
        help="limit to IB Gateway services with market data permission for these exchanges")
    parser.add_argument(
        "-t", "--sec-type",
        dest="sec_type",
        metavar="SEC_TYPE",
        choices=["STK", "FUT", "CASH", "OPT", "IND"],
        help="limit to IB Gateway services with market data permission for this securitiy "
        "type (useful for disambiguating permissions for exchanges that trade multiple asset "
        "classes). Possible choices: %(choices)s")
    parser.add_argument(
        "-r", "--research",
        metavar="VENDOR",
        dest="research_vendors",
        nargs="*",
        choices=["reuters", "wsh"],
        help="limit to IB Gateway services with permission for these research vendors. Possible choices: %(choices)s")
    parser.add_argument(
        "-g", "--gateways",
        metavar="SERVICE_NAME",
        nargs="*",
        help="limit to these IB Gateway services")
    parser.add_argument(
        "-w", "--wait",
        action="store_true",
        default=False,
        help="wait for the IB Gateway services to stop before returning (default is to stop "
        "the gateways asynchronously)")
    parser.set_defaults(func="quantrocket.launchpad._cli_stop_gateways")

    examples = """
Upload a new config, or return the current configuration.

Examples:

Upload a new config (replaces current config):

    quantrocket launchpad config myconfig.yml

Show current config:

    quantrocket launchpad config
    """
    parser = _subparsers.add_parser(
        "config",
        help="upload a new config, or return the current configuration",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "filename",
        nargs="?",
        metavar="FILENAME",
        help="the config file to upload (if omitted, return the current config)")
    parser.set_defaults(func="quantrocket.launchpad._cli_load_or_show_config")

    examples = """
Set IB username/password and trading mode (paper/live) for IB Gateway, or view
current username and trading mode.

Can be used to set new credentials or switch between paper and live trading
(must have previously entered live credentials). Setting new credentials will
restart IB Gateway and takes a moment to complete.

Examples:

View current credentials for IB Gateway service named ibg1 (shows username and
trading mode only):

    quantrocket launchpad credentials ibg1

Set credentials for ibg1 (will prompt for password):

    quantrocket launchpad credentials ibg1 -u myuser --paper

Leave credentials as-is but switch to live trading (must have previously entered
live credentials):

    quantrocket launchpad credentials ibg1 --live
    """
    parser = _subparsers.add_parser(
        "credentials",
        help="set IB username/password and trading mode (paper/live)",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "gateway",
        metavar="SERVICE_NAME",
        help="name of IB Gateway service to set credentials for (for example, "
        "'ibg1')")
    parser.add_argument(
        "-u", "--username",
        metavar="USERNAME",
        help="IB username (optional if only modifying trading mode)")
    parser.add_argument(
        "-p", "--password",
        metavar="PASSWORD",
        help="IB password (if omitted and user is provided, will be prompted "
        "for password)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--paper",
        action="store_const",
        dest="trading_mode",
        const="paper",
        help="set trading mode to paper trading")
    group.add_argument(
        "--live",
        action="store_const",
        dest="trading_mode",
        const="live",
        help="set trading mode to live trading")
    parser.set_defaults(func="quantrocket.launchpad._cli_get_or_set_credentials")

    examples = """
Access the IB Gateway GUI in a web browser.

Note: IB Gateway must already be running.

Examples:

Access the IB Gateway GUI for all running ibg services:

    quantrocket launchpad gui

Access a particular IB Gateway GUI:

    quantrocket launchpad gui -g ibg2
    """
    parser = _subparsers.add_parser(
        "gui",
        help="access the IB Gateway GUI in a web browser",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-g", "--gateways",
        metavar="SERVICE_NAME",
        nargs="*",
        help="limit to these IB Gateway services (default all IB Gateway services)")
    parser.set_defaults(func="quantrocket.launchpad._cli_open_ibg_gui")
