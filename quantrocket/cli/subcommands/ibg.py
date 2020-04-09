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
    _parser = subparsers.add_parser("ibg", description="QuantRocket IB Gateway service CLI", help="Start and stop IB Gateway")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Set username/password and trading mode (paper/live) for IB Gateway, or view
current username and trading mode.

Can be used to set new credentials or switch between paper and live trading
(must have previously entered live credentials). Setting new credentials will
restart IB Gateway and takes a moment to complete.

Credentials are encrypted at rest and never leave your deployment.

Examples:

View current credentials for IB Gateway service named ibg1 (shows username and
trading mode only):

    quantrocket ibg credentials ibg1

Set credentials for ibg1 (will prompt for password):

    quantrocket ibg credentials ibg1 -u myuser --paper

Leave credentials as-is but switch to live trading (must have previously entered
live credentials):

    quantrocket ibg credentials ibg1 --live
    """
    parser = _subparsers.add_parser(
        "credentials",
        help="set username/password and trading mode (paper/live) for IB Gateway",
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
        help="IBKR username (optional if only modifying trading mode)")
    parser.add_argument(
        "-p", "--password",
        metavar="PASSWORD",
        help="IBKR password (if omitted and user is provided, will be prompted "
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
    parser.set_defaults(func="quantrocket.ibg._cli_get_or_set_credentials")

    examples = """
Query statuses of IB Gateways.

Examples:

List the status of all gateways:

    quantrocket ibg status

Get a list of gateways that are running:

    quantrocket ibg status --status running
    """
    parser = _subparsers.add_parser(
        "status",
        help=" query statuses of IB Gateways",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-s", "--status",
        choices=["running", "stopped", "error"],
        help="limit to IB Gateways in this status. Possible choices: %(choices)s")
    parser.add_argument(
        "-g", "--gateways",
        metavar="SERVICE_NAME",
        nargs="*",
        help="limit to these IB Gateways")
    parser.set_defaults(func="quantrocket.ibg._cli_list_gateway_statuses")

    examples = """
Start one or more IB Gateways.

Examples:

Asynchronously start all gateways (that aren't already running):

    quantrocket ibg start

Start specific gateways and wait for them to come up:

    quantrocket ibg start --gateways ibg1 ibg3 --wait

Restart all gateways:

    quantrocket ibg stop --wait && quantrocket ibg start
    """
    parser = _subparsers.add_parser(
        "start",
        help="start one or more IB Gateways",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-g", "--gateways",
        metavar="SERVICE_NAME",
        nargs="*",
        help="limit to these IB Gateways")
    parser.add_argument(
        "-w", "--wait",
        action="store_true",
        default=False,
        help="wait for the IB Gateway  to start before returning (default is to "
        "start the gateways asynchronously)")
    parser.set_defaults(func="quantrocket.ibg._cli_start_gateways")

    examples = """
Stop one or more IB Gateways.

Examples:

Stop all gateways (that aren't already stopped):

    quantrocket ibg stop

Stop specific gateways and wait for them to stop:

    quantrocket ibg stop --gateways ibg1 ibg3 --wait
    """
    parser = _subparsers.add_parser(
        "stop",
        help="stop one or more IB Gateways",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-g", "--gateways",
        metavar="SERVICE_NAME",
        nargs="*",
        help="limit to these IB Gateways")
    parser.add_argument(
        "-w", "--wait",
        action="store_true",
        default=False,
        help="wait for the IB Gateway to stop before returning (default is to stop "
        "the gateways asynchronously)")
    parser.set_defaults(func="quantrocket.ibg._cli_stop_gateways")

    examples = """
Upload a new IB Gateway permissions config, or return the current configuration.

Permission configs are only necessary when running multiple IB Gateways with
differing market data permissions.

Examples:

Upload a new config (replaces current config):

    quantrocket ibg config myconfig.yml

Show current config:

    quantrocket ibg config
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
    parser.set_defaults(func="quantrocket.ibg._cli_load_or_show_config")
