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
    _parser = subparsers.add_parser("license", description="QuantRocket license service CLI", help="Query license details")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Return the current license profile.

Examples:

View the current license profile:

    quantrocket license get
    """
    parser = _subparsers.add_parser(
        "get",
        help="return the current license profile",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="refresh the license profile before returning it (default is to "
        "return the cached profile, which is refreshed every few minutes)")
    parser.set_defaults(func="quantrocket.license._cli_get_license_profile")

    examples = """
Set QuantRocket license key.

Examples:

    quantrocket license set XXXXXXXXXX
    """
    parser = _subparsers.add_parser(
        "set",
        help="set QuantRocket license key",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "key",
        metavar="LICENSEKEY",
        help="the license key for your account")
    parser.set_defaults(func="quantrocket.license._cli_set_license")

    examples = """
Set Alpaca API key, or view the current API key.

Your credentials are encrypted at rest and never leave
your deployment.

Examples:

View current live and paper API keys:

    quantrocket license alpaca-key

Set Alpaca live API key (will prompt for secret key):

    quantrocket license alpaca-key --api-key AK123 --live

Set Alpaca paper API key (will prompt for secret key):

    quantrocket license alpaca-key --api-key PK123 --paper
    """
    parser = _subparsers.add_parser(
        "alpaca-key",
        help="set Alpaca API key, or view the current API key",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-a", "--api-key",
        metavar="API_KEY",
        help="Alpaca API key ID")
    parser.add_argument(
        "-s", "--secret-key",
        metavar="SECRET_KEY",
        help="Alpaca secret key (if omitted, will be prompted for secret key)")
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
    parser.set_defaults(func="quantrocket.license._cli_get_or_set_alpaca_key")

    examples = """
Set Polygon API key, or view the current API key.

Your credentials are encrypted at rest and never leave
your deployment.

Examples:

View current API key:

    quantrocket license polygon-key

Set Polygon API key:

    quantrocket license polygon-key K123
    """
    parser = _subparsers.add_parser(
        "polygon-key",
        help="set Polygon API key, or view the current API key",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "api_key",
        nargs="?",
        metavar="API_KEY",
        help="Polygon API key")
    parser.set_defaults(func="quantrocket.license._cli_get_or_set_polygon_key")

    examples = """
Set Quandl API key, or view the current API key.

Your credentials are encrypted at rest and never leave
your deployment.

Examples:

View current API key:

    quantrocket license quandl-key

Set Polygon API key:

    quantrocket license quandl-key K123
    """
    parser = _subparsers.add_parser(
        "quandl-key",
        help="set Quandl API key, or view the current API key",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "api_key",
        nargs="?",
        metavar="API_KEY",
        help="Quandl API key")
    parser.set_defaults(func="quantrocket.license._cli_get_or_set_quandl_key")
