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
