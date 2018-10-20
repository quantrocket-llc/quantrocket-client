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
    _parser = subparsers.add_parser("countdown", description="QuantRocket cron service CLI", help="Manage crontabs")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Upload a new crontab, or return the current crontab.

Examples:

Upload a new crontab to a service called countdown-australia (replaces
current crontab):

    quantrocket countdown crontab mycron.crontab -s countdown-australia

Show current crontab for a service called countdown-australia:

    quantrocket countdown crontab -s countdown-australia
    """
    parser = _subparsers.add_parser(
        "crontab",
        help="upload a new crontab, or return the current crontab",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "filename",
        nargs="?",
        metavar="FILENAME",
        help="the crontab file to upload (if omitted, return the current crontab)")
    parser.add_argument(
        "-s", "--service",
        metavar="SERVICE_NAME",
        help="the name of the countdown service (default 'countdown')")
    parser.set_defaults(func="quantrocket.countdown._load_or_show_crontab")

    examples = """
Set or show the countdown service timezone.

Examples:

Set the timezone of the countdown service to America/New_York:

    quantrocket countdown timezone America/New_York

Show the current timezone of the countdown service:

    quantrocket countdown timezone

Show the timezone for a service called countdown-australia:

    quantrocket countdown timezone -s countdown-australia
    """
    parser = _subparsers.add_parser(
        "timezone",
        help="set or show the countdown service timezone",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "tz",
        nargs="?",
        metavar="TZ",
        help="the timezone to set (pass a partial timezone string such as 'newyork' "
        "or 'europe' to see close matches, or pass '?' to see all choices)")
    parser.add_argument(
        "-s", "--service",
        metavar="SERVICE_NAME",
        help="the name of the countdown service, (default 'countdown')")
    parser.set_defaults(func="quantrocket.countdown._cli_get_or_set_timezone")
