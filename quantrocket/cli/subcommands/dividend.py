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

from quantrocket.cli.utils.parse import parse_date, parse_datetime, parse_date_offset, parse_dict

def add_subparser(subparsers):
    _parser = subparsers.add_parser("dividend", description="QuantRocket dividend CLI", help="quantrocket dividend -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("get", help="query dividends from the dividend database")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to dividends on or after this ex date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to dividends on or before this ex date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids")
    parser.set_defaults(func="quantrocket.dividends.get_dividends")

    parser = _subparsers.add_parser("download", help="download dividends from one or more vendors and optionally clean dividend clusters")
    parser.add_argument("-v", "--vendors", nargs="*", choices=["reuters", "ib", "yahoo"], metavar="VENDOR", help="download dividends from these vendors. Possible choices: %(choices)s")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="download dividends beginning with this ex date (applies to Yahoo only)")
    parser.add_argument("-w", "--webhook", metavar="URL", help="post to this webhook when the downloads are complete")
    group = parser.add_argument_group("dividend cluster cleaning options")
    group.add_argument("-x", "--proximity", default=20, type=int, metavar="DAYS", help="consider similarly priced dividends fewer than this many days apart to be duplicates")
    group.add_argument("-f", "--fuzziness", default=0.05, type=float, metavar="FLOAT", help="consider clustered dividends whose amounts differ by less than this percentage to be duplicates")
    group.add_argument("--lookahead", dest="end_date", type=parse_date_offset, metavar="DAYS", help="limit cluster cleaning to dividends with ex dates less than this many days from today")
    group.add_argument("--odd-man-out", dest="odd_man_out", action="store_true", help="purge clustered dividends that disagree with the majority opinion of the cluster")
    group.add_argument("--prefer-vendors", dest="prefer_vendors", type=parse_dict, nargs="*", metavar="PREF:UNPREF", help="purge clustered dividends by favoring a preferred over an unpreferred vendor")
    group.add_argument("--dedupe", action="store_true", help="purge all but the first dividend in a single-vendor cluster")
    parser.set_defaults(func="quantrocket.dividends.download_dividends")

    parser = _subparsers.add_parser("reuters", help="download dividends from Reuters, via IB")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.set_defaults(func="quantrocket.dividends.download_reuters_dividends")

    parser = _subparsers.add_parser("yahoo", help="download dividends from Yahoo")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("-s", "--start-date", required=True, dest="start_date", metavar="DATE", help="download dividends beginning with this ex date")
    parser.set_defaults(func="quantrocket.dividends.download_yahoo_dividends")

    parser = _subparsers.add_parser("ib", help="download dividends from IB")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.set_defaults(func="quantrocket.dividends.download_ib_dividends")

    parser = _subparsers.add_parser("cluster", help="find and/or dividend clusters")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("-x", "--proximity", default=10, type=int, metavar="DAYS", help="consider similarly priced dividends fewer than this many days apart to be duplicates")
    parser.add_argument("-f", "--fuzziness", default=0.05, type=float, metavar="FLOAT", help="consider clustered dividends whose amounts differ by less than this percentage to be duplicates")
    parser.add_argument("-s", "--start-date", dest="start_date", type=parse_date, help="limit to dividends on or after this ex date")
    parser.add_argument("--lookahead", dest="end_date", type=parse_date_offset, metavar="DAYS", help="limit to dividends with ex dates less than this many days from today")
    parser.add_argument("--lookbehind", dest="start_date", type=parse_date_offset, metavar="DAYS", help="limit to dividends with ex dates greater than this many days from today (use negative number for past)")
    parser.add_argument("-o", "--odd-man-out", dest="odd_man_out", action="store_true", help="purge clustered dividends that disagree with the majority opinion of the cluster")
    parser.add_argument("-v", "--prefer-vendors", dest="prefer_vendors", type=parse_dict, nargs="*", metavar="PREF:UNPREF", help="purge clustered dividends by favoring a preferred over an unpreferred vendor")
    parser.add_argument("-p", "--dedupe", action="store_true", help="purge all but the first dividend in a single-vendor cluster")
    parser.add_argument("-d", "--dry-run", dest="dry_run", action="store_true", default=False, help="show what would be purged but don't purge it")
    parser.set_defaults(func="quantrocket.dividend.find_or_purge_clusters")

    parser = _subparsers.add_parser("purgestale", help="purge stale dividends")
    parser.add_argument("-b", "--created-before", dest="created_before", required=True, metavar="DATETIME", type=parse_datetime, help="purge dividends created before this datetime")
    parser.add_argument("-a", "--exdate-after", dest="exdate_after", metavar="DATE", help="limit to dividends falling on or after this ex date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids")
    parser.add_argument("-v", "--vendors", nargs="*", choices=["reuters", "ib", "yahoo"], metavar="VENDOR", help="limit to these vendors. Possible choices: %(choices)s")
    parser.set_defaults(func="quantrocket.dividend.purge_stale_dividends")
