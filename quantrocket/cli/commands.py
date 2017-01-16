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

import os.path
import subprocess
import sys
import argparse
import traceback
import pkgutil
from . import subcommands

def add_subcommands(subparsers):
    """
    Adds subparsers for each of the service modules in the subcommands package.
    """
    for _, service, _ in pkgutil.iter_modules(subcommands.__path__):
        module_path = "quantrocket.cli.subcommands.{0}".format(service)
        module = __import__(module_path, fromlist="quantrocket.cli.subcommands")
        func = getattr(module, "add_subparser")
        func(subparsers)

def handle_error(msg):
    import logging
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
    for l in msg.split("\n"):
        logger.error(l)

def get_parser():
    parser = argparse.ArgumentParser(description="QuantRocket CLI")
    subparsers = parser.add_subparsers(title="commands", dest="command", help="for command-specific help type:")
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
    func = args.pop("func")
    try:
        result = func(**args)
    except:
        if not sys.stdin.isatty():
            msg = traceback.format_exc()
            handle_error(msg)
        raise
    else:
        if result:
            print(result)

if __name__ == '__main__':
    sys.exit(main())
