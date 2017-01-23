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

from quantrocket.cli.utils.stream import stream

def add_subparser(subparsers):
    _parser = subparsers.add_parser("flightlog", description="QuantRocket logging service CLI", help="quantrocket flightlog -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("stream", help="stream application logs, `tail -f` style")
    parser.add_argument("-d", "--detail", action="store_true", help="show detailed logs from logspout, otherwise show log messages from flightlog only")
    parser.add_argument("--hist", type=int, metavar="NUM_LINES", help="number of log lines to show right away (ignored if showing detailed logs)")
    parser.add_argument("--nocolor", action="store_false", dest="color", help="don't colorize the logs")
    parser.set_defaults(func=stream("quantrocket.flightlog.stream_logs"))

    parser = _subparsers.add_parser("log", help="log a message")
    parser.add_argument("msg", nargs="?", default="-", help="the message to be logged")
    parser.add_argument("-l", "--level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
                            help="the log level for the message")
    parser.add_argument("-n", "--name", dest="logger_name", default="quantrocket.cli", help="the logger name")
    parser.set_defaults(func="quantrocket.flightlog._log_message")
