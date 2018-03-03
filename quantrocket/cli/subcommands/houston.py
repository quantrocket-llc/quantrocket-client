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
    _parser = subparsers.add_parser("houston", description="QuantRocket Houston API Gateway CLI", help="Connect to houston API gateway")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Ping houston.

Examples:

    quantrocket houston ping
    """
    parser = _subparsers.add_parser(
        "ping",
        help="ping houston",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.houston._cli_ping")
