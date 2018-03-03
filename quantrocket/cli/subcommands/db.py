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
    _parser = subparsers.add_parser("db", description="QuantRocket database service CLI", help="Manage, download, and backup databases")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
List databases.

Examples:

List all databases:

    quantrocket db list

List history databases:

    quantrocket db list history
    """
    parser = _subparsers.add_parser(
        "list",
        help="list databases",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "service",
        nargs="?",
        metavar="SERVICE",
        help="only list databases for this service")
    parser.set_defaults(func="quantrocket.db._cli_list_databases")

    examples = """
Download a database from the db service and write to a local file.

Examples:

Download a database called quantrocket.history.nyse.sqlite:

    quantrocket db get quantrocket.history.nyse.sqlite /path/to/localdir/quantrocket.history.nyse.sqlite
    """
    parser = _subparsers.add_parser(
        "get",
        help="download a database from the db service and write to a local file",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "database",
        metavar="DATABASE",
        help="the filename of the database (as returned by the list command)")
    parser.add_argument(
        "outfile",
        metavar="OUTFILE",
        help="filename to write the database to")
    parser.set_defaults(func="quantrocket.db._cli_download_database")

    examples = """
Push database(s) to Amazon S3.

Examples:

Push all databases:

    quantrocket db s3push all

Push all databases for the history service:

    quantrocket db s3push history

Push a database called quantrocket.history.nyse.sqlite:

    quantrocket db s3push history nyse
    """
    parser = _subparsers.add_parser(
        "s3push",
        help="push database(s) to Amazon S3",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "service",
        metavar="SERVICE",
        help="only push databases for this service (specify 'all' to push all services)")
    parser.add_argument(
        "codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="only push databases identified by these codes (omit to push all databases "
        "for service)")
    parser.set_defaults(func="quantrocket.db._cli_s3_push_databases")

    examples = """
Pull database(s) from Amazon S3 to the db service.

Examples:

Pull a database stored on S3 as quantrocket.history.nyse.sqlite.gz:

    quantrocket db s3pull history nyse
    """
    parser = _subparsers.add_parser(
        "s3pull",
        help="pull database(s) from Amazon S3 to the db service",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "service",
        metavar="SERVICE",
        help="only pull databases for this service (specify 'all' to pull all services)")
    parser.add_argument(
        "codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="only pull databases identified by these codes (omit to pull all databases "
        "for service)")
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        default=False,
        help="overwrite existing database if one exists (default is to fail if one exists)")
    parser.set_defaults(func="quantrocket.db._cli_s3_pull_databases")

    examples = """
Optimize database file(s) to improve performance.

Examples:

Optimize all databases:

    quantrocket db optimize all
    """
    parser = _subparsers.add_parser(
        "optimize",
        help="optimize database file(s) to improve performance",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "service",
        metavar="SERVICE",
        help="only optimize databases for this service (specify 'all' to optimize all services)")
    parser.add_argument(
        "codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="only optimize databases identified by these codes (omit to optimize all databases "
        "for service)")
    parser.set_defaults(func="quantrocket.db._cli_optimize_databases")
