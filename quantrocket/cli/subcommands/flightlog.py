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
    _parser = subparsers.add_parser("flightlog", description="QuantRocket logging service CLI", help="Monitor and download logs")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Stream application logs, `tail -f` style.

Examples:

Stream application logs:

    quantrocket flightlog stream

Stream detailed logs:

    quantrocket flightlog stream --detail
    """
    parser = _subparsers.add_parser(
        "stream",
        help="stream application logs, `tail -f` style",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="show detailed logs from logspout, otherwise show log messages from flightlog only")
    parser.add_argument(
        "--hist",
        type=int,
        metavar="NUM_LINES",
        help="number of log lines to show right away (ignored if showing detailed logs)")
    parser.add_argument(
        "--nocolor",
        action="store_false",
        dest="color",
        help="don't colorize the logs")
    parser.set_defaults(func="quantrocket.flightlog._cli_stream_logs")

    examples = """
Download the logfile.

Examples:

Download application logs:

    quantrocket flightlog get /path/to/localdir/app.log

Download detailed logs:

    quantrocket flightlog get --detail /path/to/localdir/system.log
    """
    parser = _subparsers.add_parser(
        "get",
        help="download the logfile",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "outfile",
        metavar="OUTFILE",
        help="filename to write the logfile to")
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="download detailed logs from logspout, otherwise download log messages from "
        "flightlog only")
    parser.set_defaults(func="quantrocket.flightlog._cli_download_logfile")

    examples = """
Log a message.

Examples:

Log a message under the name "myapp":

    quantrocket flightlog log "this is a test" --name myapp --level INFO

Log the output from another command:

    quantrocket account balance --below-cushion 0.02 | quantrocket flightlog log --name quantrocket.account --level CRITICAL
    """
    parser = _subparsers.add_parser(
        "log",
        help="log a message",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "msg",
        nargs="?",
        default="-",
        help="the message to be logged")
    parser.add_argument(
        "-l", "--level",
        default="INFO",
        metavar="LEVEL",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="the log level for the message. Possible choices: %(choices)s")
    parser.add_argument(
        "-n", "--name",
        dest="logger_name",
        default="quantrocket.cli",
        help="the logger name")
    parser.set_defaults(func="quantrocket.flightlog._cli_log_message")
