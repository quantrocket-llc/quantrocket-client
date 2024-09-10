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
    _parser = subparsers.add_parser("codeload", description="QuantRocket code management CLI", help="Clone and manage code")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Clone files from a Git repository.

Only the files are copied, not the Git metadata. Can be run multiple
times to clone files from multiple repositories. Won't overwrite
any existing files unless the `--replace` option is used.

Notes
-----
Usage Guide:

* Code Management: https://qrok.it/dl/qr/codeload

Examples
--------

Clone QuantRocket's "umd" demo repository:

.. code-block:: bash

    quantrocket codeload clone umd

Clone a GitHub repo and skip files that already exist locally:

.. code-block:: bash

    quantrocket codeload clone myuser/myrepo --skip-existing

Clone a Bitbucket repo:

.. code-block:: bash

    quantrocket codeload clone https://bitbucket.org/myuser/myrepo.git

Clone a private repo by including username and app password (Bitbucket) or
personal access token (GitHub) in the URL:

.. code-block:: bash

    quantrocket codeload clone https://myuser:myapppassword@bitbucket.org/myuser/myrepo.git
    """
    parser = _subparsers.add_parser(
        "clone",
        help="clone files from a Git repository",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "repo",
        help="the name or URL of the repo. Can be the name of a QuantRocket demo "
        "repo (e.g. 'umd'), a GitHub username/repo (e.g. 'myuser/myrepo'), or the "
        "URL of any Git repository").completer = completers.codeload_repo_completer
    parser.add_argument(
        "-b", "--branch",
        help="the branch to clone (default 'master' or 'main')").completer = completers.example_completer(["main"])
    on_conflict_group = parser.add_mutually_exclusive_group()
    on_conflict_group.add_argument(
        "-r", "--replace",
        action="store_true",
        help="if a file already exists locally, replace it with the remote file "
        "(mutually exclusive with --skip-existing)")
    on_conflict_group.add_argument(
        "-s", "--skip-existing",
        action="store_true",
        help="if a file already exists locally, skip it (mutually exclusive with "
        "--replace)")
    parser.add_argument(
        "-d", "--target-dir",
        help="the directory into which files should be cloned. Default is '/codeload'"
    ).completer = completers.directory_completer
    parser.set_defaults(func="quantrocket.codeload._cli_clone")
