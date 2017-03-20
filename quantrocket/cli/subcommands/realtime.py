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
    _parser = subparsers.add_parser("realtime", description="QuantRocket realtime data CLI", help="quantrocket realtime -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("quote", help="get realtime quotes for securities")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these IB conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("-f", "--fields", nargs="*", metavar="FIELD", help="limit to these fields")
    parser.add_argument("-w", "--window", metavar="HH:MM:SS", help="limit to this historical window (use Pandas timedelta string)")
    parser.add_argument("-s", "--snapshot", action="store_true", help="return a snapshot of the latest quotes")
    parser.add_argument("--save", metavar="DB", help="save the quotes asynchronously to the named database after returning them")
    parser.add_argument("--disable-backmonth-filter", action="store_true", help="don't filter out back month contracts (default is to filter out everything except front month)")
    parser.set_defaults(func="quantrocket.realtime.get_quotes")

    parser = _subparsers.add_parser("add", help="add securities to the realtime data stream")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these IB conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.add_argument("-c", "--cancel-in", metavar="HH:MM:SS", help="automatically cancel the securities after this much time (use Pandas timedelta string)")
    parser.add_argument("--disable-backmonth-filter", action="store_true", help="don't filter out back month contracts (default is to filter out everything except front month)")
    parser.set_defaults(func="quantrocket.realtime.stream_securities")

    parser = _subparsers.add_parser("cancel", help="remove securities from the realtime data stream")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these IB conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.set_defaults(func="quantrocket.realtime.cancel_stream")
