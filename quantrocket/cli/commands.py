#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
#
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

# to make argcomplete perky, limit imports to the minimum here and in
# subcommand modules
import sys
import six
import argparse
import pkgutil
from . import subcommands

def import_func(path):
    '''
    Imports and returns a function or class from a dot separated path.
    '''
    parts = path.split('.')
    module_path = parts[:-1]
    func_name = parts[-1]

    module = __import__('.'.join(module_path), fromlist=module_path[:-1])
    func = getattr(module, func_name)
    return func

def add_subcommands(subparsers):
    """
    Adds subparsers for each of the service modules in the subcommands package.
    """
    for _, service, _ in pkgutil.iter_modules(subcommands.__path__):
        func = import_func("quantrocket.cli.subcommands.{0}.add_subparser".format(service))
        func(subparsers)

def handle_error(msg):
    import logging
    import subprocess
    import os.path
    from ..flightlog import FlightlogHandler
    servicename_path = "/opt/conda/bin/servicename"
    if os.path.exists(servicename_path):
        servicename = subprocess.check_output([servicename_path], universal_newlines=True).strip()
    else:
        servicename = "cli"
    logger = logging.getLogger("quantrocket.{0}".format(servicename))
    handler = FlightlogHandler(background=False)
    logger.addHandler(handler)
    logger.error("Error running {0}".format(" ".join(sys.argv)))
    if not isinstance(msg, six.string_types):
        msg = repr(msg)
    for l in msg.splitlines():
        logger.error(l)

class ArgumentParser(argparse.ArgumentParser):
    """
    ArgumentParser that logs parsing errors to flightlog if not a tty.
    """
    def error(self, message):
        if not sys.stdin.isatty():
            handle_error(message)
        return super(ArgumentParser, self).error(message)

def get_parser():
    parser = ArgumentParser(description="QuantRocket command line interface")
    subparsers = parser.add_subparsers(title="commands", dest="command", help="for specific help type: quantrocket <subcommand> -h")
    subparsers.required = True
    add_subcommands(subparsers)
    return parser

def main():
    parser = get_parser()
    if sys.stdin.isatty():
        try:
            import argcomplete
        except ImportError:
            pass
        else:
            argcomplete.autocomplete(parser)
    args = parser.parse_args()
    args = vars(args)
    args.pop("command")
    args.pop("subcommand", None)
    func_name = args.pop("func")
    func = import_func(func_name)
    try:
        result, exit_code = func(**args)
    except:
        if not sys.stdin.isatty():
            import traceback
            msg = traceback.format_exc()
            handle_error(msg)
        raise
    else:
        if result:
            # nonzero exit codes for non-interactive commands should be
            # logged
            if exit_code > 0 and not sys.stdin.isatty() and not sys.stdout.isatty():
                handle_error(result)
            # otherwise print
            else:
                print(result)

        return exit_code

if __name__ == '__main__':
    sys.exit(main())
