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

def add_subparser(subparsers):
    _parser = subparsers.add_parser("calendar", description="QuantRocket calendar CLI", help="quantrocket calendar -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("isopen", help="assert that one or more exchanges is open and return a non-zero exit code if closed")
    parser.add_argument("exchanges", metavar="EXCHANGE", nargs="+", help="the exchange(s) to check")
    parser.add_argument("-i", "--in", metavar="TIMEDELTA", help="assert that the exchange(s) will be open in the future (use Pandas timedelta string, e.g. 2h)")
    parser.add_argument("-a", "--ago", metavar="TIMEDELTA", help="assert that the exchange(s) was open in the past (use Pandas timedelta string, e.g. 2h)")
    parser.set_defaults(func="quantrocket.calendar.is_open")

    parser = _subparsers.add_parser("isclosed", help="assert that one or more exchanges is closed and return a non-zero exit code if open")
    parser.add_argument("exchanges", metavar="EXCHANGE", nargs="+", help="the exchange(s) to check")
    parser.add_argument("-i", "--in", metavar="TIMEDELTA", help="assert that the exchange(s) will be closed in the future (use Pandas timedelta string)")
    parser.add_argument("-a", "--ago", metavar="TIMEDELTA", help="assert that the exchange(s) was closed in the past (use Pandas timedelta string)")
    parser.set_defaults(func="quantrocket.calendar.is_closed")

    parser = _subparsers.add_parser("closings", help="get exchange closings")
    parser.add_argument("-x", "--exchanges", metavar="EXCHANGE", help="limit to these exchanges")
    parser.add_argument("-s", "--start-date", metavar="YYYY-MM-DD", help="limit to closings on or after this date")
    parser.add_argument("-e", "--end-date", metavar="YYYY-MM-DD", help="limit to closings on or before this date")
    parser.add_argument("-a", "--after", metavar="TIMEDELTA", help="limit to closings on or after this many days/hours/etc in the past (use Pandas timedelta string, e.g. 7d)")
    parser.add_argument("-b", "--before", metavar="TIMEDELTA", help="limit to closings on or before this many days/hours/etc in the future (use Pandas timedelta string, e.g. 7d)")
    parser.add_argument("-t", "--types", choices=["full", "half"], metavar="TYPE", help="limit to these closing types. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.calendar.get_closings")

    parser = _subparsers.add_parser("load", help="load exchange closings from file")
    parser.add_argument("filename", type=str, help="CSV containing with columns 'date', 'exchange', and optionally 'type' ('full' or 'half') ")
    parser.set_defaults(func="quantrocket.calendar.load_closings")
