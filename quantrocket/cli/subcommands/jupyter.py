# Copyright 2018 QuantRocket - All Rights Reserved
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
    examples = """
Access the Jupyter environment in a web browser.

Examples:

Access the Jupyter environment:

    quantrocket jupyter
    """
    parser = subparsers.add_parser(
        "jupyter",
        description="QuantRocket Jupyter CLI",
        help="Access JupyterLab in a web browser",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.set_defaults(func="quantrocket.jupyter._cli_open_jupyter")
