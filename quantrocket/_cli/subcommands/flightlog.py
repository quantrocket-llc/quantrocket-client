# Copyright 2017-2024 QuantRocket - All Rights Reserved
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

from quantrocket._cli.utils.parse import HelpFormatter
from quantrocket._cli.utils import completers

def add_subparser(subparsers):
    _parser = subparsers.add_parser("flightlog", description="QuantRocket logging service CLI", help="Monitor and download logs")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Stream application logs, `tail -f` style.

Notes
-----
Usage Guide:

* Logging: https://qrok.it/dl/qr/logging

Examples
--------

Stream application logs:

.. code-block:: bash

    quantrocket flightlog stream

Stream detailed logs:

.. code-block:: bash

    quantrocket flightlog stream --detail
    """
    parser = _subparsers.add_parser(
        "stream",
        help="stream application logs, `tail -f` style",
        epilog=examples,
        formatter_class=HelpFormatter)
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

Notes
-----
Usage Guide:

* Logging: https://qrok.it/dl/qr/logging

Examples
--------

Download application logs:

.. code-block:: bash

    quantrocket flightlog get app.log

Download detailed logs:

.. code-block:: bash

    quantrocket flightlog get --detail sys.log

Download detailed logs for the history service:

.. code-block:: bash

    quantrocket flightlog get --detail --match quantrocket_history sys.log
    """
    parser = _subparsers.add_parser(
        "get",
        help="download the logfile",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "outfile",
        metavar="OUTFILE",
        help="filename to write the logfile to").completer = completers.outfile_completer(
            ["txt"], outfile_prefix="logs")
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="download detailed logs from the logspout service, otherwise download "
        "the standard logs from the flightlog service")
    parser.add_argument(
        "-m", "--match",
        metavar="PATTERN",
        help="filter the logfile to lines containing this string"
        ).completer = completers.example_completer("string_to_search_for")
    parser.set_defaults(func="quantrocket.flightlog._cli_download_logfile")

    examples = r"""
Wait for a message to appear in the logs.

Searches can be performed against the standard or detailed log file.
When searching the detailed logs, note that the log file uses the
syslog format, which differs from the format used when streaming
detailed logs. Download the detailed log file to see the exact format
your search will run against.

Notes
-----
Usage Guide:

* Wait for Log Messages: https://qrok.it/dl/qr/logging-wait

Examples
--------

Wait up to 10 minutes for a message to appear indicating that data
ingestion has finished:

.. code-block:: bash

    quantrocket flightlog wait '[usstock-1min] Completed ingesting data' --timeout 10m

Using a regular expression, wait up to 1 hour for a message to appear
indicating that data collection has finished:

.. code-block:: bash

    quantrocket flightlog wait '\[usstock-1d\] Collected [0-9]+ monthly files' --regex --timeout 1h
    """
    parser = _subparsers.add_parser(
        "wait",
        help="wait for a message to appear in the logs",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "message",
        help="the log message to search for").completer = completers.example_completer("string_to_search_for")
    parser.add_argument(
        "-r", "--regex",
        action="store_true",
        help="if True, treat the `message` argument as a regular expression "
        "(default is to treat it as a plain string)")
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="if True, search the detailed logs from the logspout service (default "
        "is to search the standard logs from the flightlog service)")
    parser.add_argument(
        "--tail",
        type=int,
        help="search the most recent N lines of the logs in addition to searching "
        "future logs (default is to only search future logs)").completer = completers.example_completer("10")
    parser.add_argument(
        "--timeout",
        help="fail if the message is not found after this much time (use Pandas "
        "timedelta string, e.g. 30sec or 5min or 2h; default is to wait "
        "indefinitely)").completer = completers.timedelta_completer
    parser.set_defaults(func="quantrocket.flightlog._cli_wait_for_message")

    examples = """
Log a message.

Notes
-----
Usage Guide:

* Logging: https://qrok.it/dl/qr/logging

Examples
--------

Log a message under the name "myapp":

.. code-block:: bash

    quantrocket flightlog log "this is a test" --name myapp --level INFO

Log the output from another command:

.. code-block:: bash

    quantrocket account balance --below-cushion 0.02 | quantrocket flightlog log --name quantrocket.account --level CRITICAL
    """
    parser = _subparsers.add_parser(
        "log",
        help="log a message",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "msg",
        nargs="?",
        default="-",
        help="the message to be logged").completer = lambda *args, **kwargs: {"-": "stdin", "string_to_log": "example"}
    parser.add_argument(
        "-l", "--level",
        default="INFO",
        metavar="LEVEL",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="the log level for the message. Possible choices: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    parser.add_argument(
        "-n", "--name",
        dest="logger_name",
        default="quantrocket.cli",
        help="the logger name").completer = completers.example_completer("quantrocket.cli")
    parser.set_defaults(func="quantrocket.flightlog._cli_log_message")

    examples = """
Set or show the flightlog timezone.

Notes
-----
Usage Guide:

* Logging: https://qrok.it/dl/qr/logging

Examples
--------

Set the flightlog timezone to America/New_York:

.. code-block:: bash

    quantrocket flightlog timezone America/New_York

Show the current flightlog timezone:

.. code-block:: bash

    quantrocket flightlog timezone
    """
    parser = _subparsers.add_parser(
        "timezone",
        help="set or show the flightlog timezone",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "tz",
        nargs="?",
        metavar="TZ",
        help="the timezone to set (pass a partial timezone string such as 'newyork' "
        "or 'europe' to see close matches, or pass '?' to see all choices)"
        ).completer = completers.timezone_completer
    parser.set_defaults(func="quantrocket.flightlog._cli_get_or_set_timezone")

    examples = """
Set or show the Papertrail log configuration.

Notes
-----
Usage Guide:

* Papertrail Integration: https://qrok.it/dl/qr/papertrail

Examples
--------

Set the Papertrail host and port to log to:

.. code-block:: bash

    quantrocket flightlog papertrail --host logs.papertrailapp.com --port 55555

Show the current papertrail config:

.. code-block:: bash

    quantrocket flightlog papertrail
    """
    parser = _subparsers.add_parser(
        "papertrail",
        help="set or show the Papertrail log configuration",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "--host",
        metavar="HOST",
        help="the Papertrail host to log to")
    parser.add_argument(
        "--port",
        metavar="PORT",
        type=int,
        help="the Papertrail port to log to")
    parser.set_defaults(func="quantrocket.flightlog._cli_get_or_set_papertrail_config")
