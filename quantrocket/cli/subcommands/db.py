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
    _parser = subparsers.add_parser("db", description="QuantRocket database service CLI", help="Backup, restore, and manage databases")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
List databases.

Examples:

List all databases:

    quantrocket db list

List all history databases and include details such as file size:

    quantrocket db list --services history --detail

List details for a sharded history database called usa-stk-15min
and list each shard individually:

    quantrocket db list --services history --codes usa-stk-15min --detail --expand
    """
    parser = _subparsers.add_parser(
        "list",
        help="list databases",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-s", "--services",
        nargs="*",
        metavar="SERVICE",
        help="limit to these services")
    parser.add_argument(
        "-c", "--codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="limit to these codes")
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
Set or show Amazon S3 configuration for pushing and pulling databases to and
from S3.

See http://qrok.it/h/dbs3 to learn more.

Credentials are encrypted at rest and never leave your deployment.

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
    parser.add_argument(
        "-r", "--region",
        help="the AWS region in which to create the bucket (default us-east-1). "
        "Ignored if the bucket already exists.")
    parser.set_defaults(func="quantrocket.db._cli_get_or_set_s3_config")

    examples = """
Push database(s) to Amazon S3.

See http://qrok.it/h/dbs3 to learn more.

Examples:

Push all databases:

    quantrocket db s3push

Push all databases for the history service:

    quantrocket db s3push --services history

Push a database called quantrocket.history.nyse.sqlite:

    quantrocket db s3push --services history --codes nyse
    """
    parser = _subparsers.add_parser(
        "s3push",
        help="push database(s) to Amazon S3",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-s", "--services",
        nargs="*",
        metavar="SERVICE",
        help="limit to these services")
    parser.add_argument(
        "-c", "--codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="limit to these codes")
    parser.set_defaults(func="quantrocket.db._cli_s3_push_databases")

    examples = """
Pull database(s) from Amazon S3 to the db service.

See http://qrok.it/h/dbs3 to learn more.

Examples:

Pull a database stored on S3 as quantrocket.history.nyse.sqlite.gz:

    quantrocket db s3pull --services history --codes nyse
    """
    parser = _subparsers.add_parser(
        "s3pull",
        help="pull database(s) from Amazon S3",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-s", "--services",
        nargs="*",
        metavar="SERVICE",
        help="limit to these services")
    parser.add_argument(
        "-c", "--codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="limit to these codes")
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        default=False,
        help="overwrite existing database if one exists (default is to "
        "fail if one exists)")
    parser.set_defaults(func="quantrocket.db._cli_s3_pull_databases")

    examples = """
Optimize databases to improve performance.

This runs the 'VACUUM' command, which defragments the database and
reclaims disk space.

Examples:

Optimize all blotter databases:

    quantrocket db optimize --services blotter
    """
    parser = _subparsers.add_parser(
        "optimize",
        help="optimize databases to improve performance",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-s", "--services",
        nargs="*",
        metavar="SERVICE",
        help="limit to these services")
    parser.add_argument(
        "-c", "--codes",
        nargs="*",
        metavar="DATABASE_CODE",
        help="limit to these codes")
    parser.set_defaults(func="quantrocket.db._cli_optimize_databases")
