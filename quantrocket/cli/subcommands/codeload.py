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
    _parser = subparsers.add_parser("codeload", description="QuantRocket code management CLI", help="Clone and manage code")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Clone files from a Git repository.

Only the files are copied, not the Git metadata. Can be run multiple
times to clone files from multiple repositories. Won't overwrite
any existing files unless the `--replace` option is used.

Examples:

Clone QuantRocket's "umd" demo repository:

    quantrocket codeload clone umd

Clone a GitHub repo and skip files that already exist locally:

    quantrocket codeload clone myuser/myrepo --skip-existing

Clone a Bitbucket repo:

    quantrocket codeload clone https://bitbucket.org/myuser/myrepo.git

Clone a private GitHub repo by including authentication credentials in the URL
(also works for Bitbucket):

    quantrocket codeload clone https://myuser:mypassword@github.com/myuser/myrepo.git
    """
    parser = _subparsers.add_parser(
        "clone",
        help="clone files from a Git repository",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "repo",
        help="the name or URL of the repo. Can be the name of a QuantRocket demo "
        "repo (e.g. 'umd'), a GitHub username/repo (e.g. 'myuser/myrepo'), or the "
        "URL of any Git repository")
    parser.add_argument(
        "-b", "--branch",
        help="the branch to clone (default 'master')")
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
    parser.set_defaults(func="quantrocket.codeload._cli_clone")
