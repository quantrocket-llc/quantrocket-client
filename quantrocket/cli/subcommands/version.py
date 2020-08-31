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

def add_subparser(subparsers):
    examples = """
Show the QuantRocket version number.

Examples:

Show the version number:

    quantrocket version

Show both the services and client version numbers:

    quantrocket version -d
    """
    parser = subparsers.add_parser(
        "version",
        description="show the QuantRocket version number",
        help="show the QuantRocket version number",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="show the services version number and "
        "also the version number of the client library making "
        "this API call. Default is to only show the services "
        "version number, which is the main QuantRocket version "
        "number.")
    parser.set_defaults(func="quantrocket.version._cli_get_version")
