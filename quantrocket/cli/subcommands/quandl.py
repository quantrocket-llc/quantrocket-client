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

SUPPORTED_DBS = ["ZEP", "NS1"]

def add_subparser(subparsers):

    # not implemented
    return
    _parser = subparsers.add_parser("quandl", description="QuantRocket Quandl data CLI", help="Manage and query Quandl datasets")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("download", help="download a database from Quandl")
    parser.add_argument("database_code", metavar="DATABASE_CODE", choices=SUPPORTED_DBS, help="the code of the database to download")
    parser.set_defaults(func="quantrocket.quandl.download_from_quandl")

    parser = _subparsers.add_parser("ns1", help="query the FinSentS News Sentiment database")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to on or after this date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to on or before this date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups (will be translated to NS1 tickers)")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these IB conids (will be translated to NS1 tickers)")
    parser.add_argument("-t", "--tickers", nargs="*", metavar="TICKER", help="limit to these NS1 tickers")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups (will be translated to NS1 tickers)")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids (will be translated to NS1 tickers)")
    parser.set_defaults(func="quantrocket.quandl.get_ns1")

    parser = _subparsers.add_parser("zep", help="query the Zacks Equity Prices database")
    parser.add_argument("-s", "--start-date", dest="start_date", metavar="DATE", help="limit to on or after this date")
    parser.add_argument("-e", "--end-date", dest="end_date", metavar="DATE", help="limit to on or before this date")
    parser.add_argument("-g", "--groups", nargs="*", metavar="GROUP", help="limit to these groups (will be translated to ZEP tickers)")
    parser.add_argument("-i", "--conids", nargs="*", metavar="CONID", help="limit to these conids (will be translated to ZEP tickers)")
    parser.add_argument("-t", "--tickers", nargs="*", metavar="TICKER", help="limit to these ZEP tickers")
    parser.add_argument("--exclude-groups", nargs="*", metavar="GROUP", help="exclude these groups (will be translated to ZEP tickers)")
    parser.add_argument("--exclude-conids", nargs="*", metavar="CONID", help="exclude these conids (will be translated to ZEP tickers)")
    parser.add_argument("--exclude-delisted", action="store_true", dest="exclude_delisted", help="exclude delisted tickers")
    parser.set_defaults(func="quantrocket.quandl.get_zep")
