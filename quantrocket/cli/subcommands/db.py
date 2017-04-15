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
    _parser = subparsers.add_parser("db", description="QuantRocket database service CLI", help="quantrocket db -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("list", help="list databases")
    parser.add_argument("service", nargs="?", metavar="SERVICE", help="only list databases for this service")
    parser.set_defaults(func="quantrocket.db.list_databases")

    parser = _subparsers.add_parser("get", help="download a database from the db service")
    parser.add_argument("service", metavar="SERVICE", help="the service the database is associated with")
    parser.add_argument("database", nargs="?",  metavar="DATABASE", help="the name of the database (if service has multiple)")
    parser.set_defaults(func="quantrocket.db.download_database")

    parser = _subparsers.add_parser("s3push", help="backup database(s) to Amazon S3")
    parser.add_argument("service", metavar="SERVICE", help="only backup databases for this service (specify 'all' to backup all services)")
    parser.add_argument("databases", nargs="*", metavar="DATABASE", help="only backup these databases (omit to backup all databases for service)")
    parser.set_defaults(func="quantrocket.db.s3_backup_databases")

    parser = _subparsers.add_parser("s3pull", help="restore database(s) from Amazon S3 to the db service")
    parser.add_argument("service", metavar="SERVICE", help="only restore databases for this service (specify 'all' to restore all services)")
    parser.add_argument("databases", nargs="*", metavar="DATABASE", help="only restore these databases (omit to restore all databases for service)")
    parser.add_argument("-f", "--force", action="store_true", default=False, help="overwrite existing database if one exists (default is to fail if one exists)")
    parser.set_defaults(func="quantrocket.db.s3_restore_databases")

    parser = _subparsers.add_parser("optimize", help="optimize database file(s) to improve performance")
    parser.add_argument("service", metavar="SERVICE", help="only optimize databases for this service (specify 'all' to optimize all services)")
    parser.add_argument("databases", nargs="*", metavar="DATABASE", help="only optimize these databases (omit to optimize all databases for service)")
    parser.set_defaults(func="quantrocket.db.optimize_databases")
