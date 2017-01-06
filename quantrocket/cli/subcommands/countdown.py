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

from quantrocket.countdown import get_crontab, get_timezone

def add_subparser(subparsers):
    _parser = subparsers.add_parser("countdown", description="QuantRocket Countdown CLI", help="quantrocket countdown -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("crontab", help="show the countdown service crontab")
    parser.add_argument("service", metavar="SERVICE_NAME", help="The name of the countdown service, e.g. countdown-usa")
    parser.set_defaults(func=get_crontab)

    parser = _subparsers.add_parser("timezone", help="show the countdown service timezone")
    parser.add_argument("service", metavar="SERVICE_NAME", help="The name of the countdown service, e.g. countdown-usa")
    parser.set_defaults(func=get_timezone)
