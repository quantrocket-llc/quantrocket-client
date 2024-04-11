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
"""
Functions for loading code.

Functions
---------
clone
    Clone files from a Git repository.

Notes
-----
Usage Guide:

* Code Management: https://qrok.it/dl/qr/codeload
"""
import os
from quantrocket.houston import houston
from quantrocket._cli.utils.output import json_to_cli

__all__ = [
    "clone",
]

def clone(
    repo: str,
    branch: str = None,
    replace: bool = None,
    skip_existing: bool = None,
    target_dir: str = None
    ) -> dict[str, str]:
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

    target_dir : str, optional
        the directory into which files should be cloned. Default is '/codeload'

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Code Management: https://qrok.it/dl/qr/codeload

    Examples
    --------
    Clone QuantRocket's "umd" demo repository:

    >>> clone("umd")

    Clone a GitHub repo and skip files that already exist locally:

    >>> clone("myuser/myrepo", skip_existing=True)

    Clone a Bitbucket repo:

    >>> clone("https://bitbucket.org/myuser/myrepo.git")

    Clone a private repo by including username and app password (Bitbucket)
    or personal access token (GitHub) in the URL:

    >>> clone("https://myuser:myapppassword@bitbucket.org/myuser/myrepo.git")
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
    if target_dir:
        data["target_dir"] = target_dir

    response = houston.post("/codeload/repo", data=data)
    houston.raise_for_status_with_json(response)
    status = response.json()

    # If we're running in a Jupyter notebook, display a link
    # to the Introduction file
    try:
        from IPython import get_ipython
        from IPython.display import display, FileLink
        ipython_installed = True
    except ImportError:
        ipython_installed = False

    if (
        ipython_installed
        and os.environ.get("YOU_ARE_INSIDE_JUPYTER", False)
        and get_ipython() is not None):

        files = status['files'].get('added', status['files'].get('updated', []))
        intro_file = next((f for f in files if f.lower().endswith("introduction.ipynb")), None)
        if intro_file:
            root = os.environ.get("JUPYTER_SERVER_ROOT", "/codeload")
            abs_path = f"{root}/{intro_file}"
            display(
                FileLink(
                    # IPython requires a relative path to open the file in JupyterLab
                    # (absolute paths open in a new browser tab)
                    os.path.relpath(abs_path),
                    result_html_prefix="Start here: "
                )
            )

    return status

def _cli_clone(*args, **kwargs):
    return json_to_cli(clone, *args, **kwargs)
