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
import textwrap

def add_subparser(subparsers):
    _parser = subparsers.add_parser("db", description="QuantRocket database service CLI", help="quantrocket db -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("list", help="list databases")
    parser.add_argument("service", nargs="?", metavar="SERVICE", help="only list databases for this service")
    parser.set_defaults(func="quantrocket.db.list_databases")

    parser = _subparsers.add_parser(
        "get", help="download a database from the db service", epilog=textwrap.dedent("""\
        Example:
            Download a database called quantrocket.history.nyse.sqlite:

                quantrocket db get quantrocket.history.nyse.sqlite"""),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("database", metavar="DATABASE", help="the filename of the database (as returned by the list command)")
    parser.set_defaults(func="quantrocket.db.download_database")

    parser = _subparsers.add_parser(
        "s3push", help="backup database(s) to Amazon S3", epilog=textwrap.dedent("""\
        Example:
            Push a database called quantrocket.history.nyse.sqlite:

                quantrocket db s3push history nyse"""),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("service", metavar="SERVICE", help="only backup databases for this service (specify 'all' to backup all services)")
    parser.add_argument("codes", nargs="*", metavar="DATABASE_CODE", help="only backup databases identified by these codes (omit to backup all databases for service)")
    parser.set_defaults(func="quantrocket.db.s3_push_databases")

    parser = _subparsers.add_parser(
        "s3pull", help="restore database(s) from Amazon S3 to the db service",
        epilog=textwrap.dedent("""\
        Example:
            Pull a database stored on S3 as quantrocket.history.nyse.sqlite.gz:

                quantrocket db s3pull history nyse"""),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("service", metavar="SERVICE", help="only restore databases for this service (specify 'all' to restore all services)")
    parser.add_argument("code", nargs="*", metavar="DATABASE_CODE", help="only restore databases identified by these codes (omit to restore all databases for service)")
    parser.add_argument("-f", "--force", action="store_true", default=False, help="overwrite existing database if one exists (default is to fail if one exists)")
    parser.set_defaults(func="quantrocket.db.s3_pullases")

    parser = _subparsers.add_parser("optimize", help="optimize database file(s) to improve performance")
    parser.add_argument("service", metavar="SERVICE", help="only optimize databases for this service (specify 'all' to optimize all services)")
    parser.add_argument("code", nargs="*", metavar="DATABASE_CODE", help="only optimize databases identified by these codes (omit to optimize all databases for service)")
    parser.set_defaults(func="quantrocket.db.optimize_databases")
