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

List all databases as a flat list:

    quantrocket db list

List all history databases and include details such as file size:

    quantrocket db list history --detail

List details for a sharded history database called usa-stk-15min
and list each shard individually:

    quantrocket db list history usa-stk-15min --detail --expand
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
    parser.add_argument(
        "codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="only list databases identified by these codes (omit "
        "to list all databases "
        "for service)")
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="return database statistics (default is to return a "
        "flat list of database names)")
    parser.add_argument(
        "-e", "--expand",
        action="store_true",
        help="expand sharded databases to include individual shards "
        "(default is to list sharded databases as a single database)")
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
Set or show Amazon S3 configuration for pushing and pulling databases to and
from S3.

See http://qrok.it/h/dbs3 to learn more.

Examples:

Configure S3 (will prompt for secret access key):

    quantrocket db s3config --access-key-id XXXXXXXX --bucket my-bucket

Preserve existing credentials but point to a new bucket:

    quantrocket db s3config --bucket my-other-bucket

Show current configuration:

    quantrocket db s3config
    """
    parser = _subparsers.add_parser(
        "s3config",
        help="set or show Amazon S3 configuration for pushing and "
        "pulling databases to and from S3",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-a", "--access-key-id",
        help="AWS access key ID")
    parser.add_argument(
        "-s", "--secret-access-key",
        help="AWS secret access key (if omitted and access-key-id is provided, "
        "will be prompted for secret-access-key)")
    parser.add_argument(
        "-b", "--bucket",
        help="the S3 bucket name to push to/pull from")
    parser.set_defaults(func="quantrocket.db._cli_get_or_set_s3_config")

    examples = """
Push database(s) to Amazon S3.

See http://qrok.it/h/dbs3 to learn more.

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

See http://qrok.it/h/dbs3 to learn more.

Examples:

Pull a database stored on S3 as quantrocket.history.nyse.sqlite.gz:

    quantrocket db s3pull history nyse
    """
    parser = _subparsers.add_parser(
        "s3pull",
        help="pull database(s) from Amazon S3",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "service",
        metavar="SERVICE",
        help="only pull databases for this service (specify 'all' "
        "to pull all services)")
    parser.add_argument(
        "codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="only pull databases identified by these codes (omit to "
        "pull all databases "
        "for service)")
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        default=False,
        help="overwrite existing database if one exists (default is to "
        "fail if one exists)")
    parser.set_defaults(func="quantrocket.db._cli_s3_pull_databases")

    examples = """
Optimize database file(s) to improve performance.

This runs SQLite's 'VACUUM' command, which defragments the .sqlite file
and reclaims disk space.

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
        help="only optimize databases for this service (specify 'all' "
        "to optimize all services)")
    parser.add_argument(
        "codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="only optimize databases identified by these codes (omit "
        "to optimize all databases "
        "for service)")
    parser.set_defaults(func="quantrocket.db._cli_optimize_databases")
