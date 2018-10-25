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

from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli

def clone(repo, branch=None, replace=None, skip_existing=None):
    """
    Clone files from a Git repository.

    Only the files are copied, not the Git metadata. Can be run multiple
    times to clone files from multiple repositories. Won't overwrite
    any existing files unless `replace=True`.

    Parameters
    ----------
    repo : str, required
        the name or URL of the repo. Can be the name of a QuantRocket demo
        repo (e.g. 'umd'), a GitHub username/repo (e.g. 'myuser/myrepo'),
        or the URL of any Git repository

    branch : str, optional
        the branch to clone (default 'master')

    replace : bool, optional
        if a file already exists locally, replace it with the remote file
        (mutually exclusive with skip_existing)

    skip_existing : bool, optional
        if a file already exists locally, skip it (mutually exclusive with
        replace)

    Returns
    -------
    dict
        status message

    Examples
    --------
    Clone QuantRocket's "umd" demo repository:

    >>> clone("umd")

    Clone a GitHub repo and skip files that already exist locally:

    >>> clone("myuser/myrepo", skip_existing=True)

    Clone a Bitbucket repo:

    >>> clone("https://bitbucket.org/myuser/myrepo.git")

    Clone a private GitHub repo by including authentication credentials
    in the URL (also works for Bitbucket):

    >>> clone("https://myuser:mypassword@github.com/myuser/myrepo.git")
    """
    data = {
        "repo": repo
    }
    if branch:
        data["branch"] = branch
    if replace:
        data["replace"] = replace
    if skip_existing:
        data["skip_existing"] = skip_existing

    response = houston.post("/codeload/repo", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_clone(*args, **kwargs):
    return json_to_cli(clone, *args, **kwargs)
