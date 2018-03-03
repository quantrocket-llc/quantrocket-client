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

import os
import webbrowser
from quantrocket.houston import houston
from quantrocket.exceptions import UnavailableInsideJupyter
from quantrocket.cli.utils.output import json_to_cli

def open_jupyter():
    """
    Access the Jupyter environment in a web browser.

    Returns
    -------
    None
    """
    if os.environ.get("YOU_ARE_INSIDE_JUPYTER", False):
        raise UnavailableInsideJupyter("""Cannot open Jupyter

You can't open jupyter because you are already inside jupyter!
""")

    url = "{0}/jupyter/lab".format(houston.base_url)
    webbrowser.open(url)

def _cli_open_jupyter(*args, **kwargs):
    return json_to_cli(open_jupyter, *args, **kwargs)