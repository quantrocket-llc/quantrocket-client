# Copyright 2020 QuantRocket - All Rights Reserved
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
from quantrocket import __version__
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli

def get_version(detail=False):
    """
    Show the QuantRocket version number.

    Parameters
    ----------
    detail : bool
        if True, show the services version number and
        also the version number of the client library making
        this API call. Default is to only show the services
        version number, which is the main QuantRocket version
        number.

    Returns
    -------
    str or dict
        services version number, or dict of services and client
        version numbers
    """

    response = houston.get("/version")
    houston.raise_for_status_with_json(response)
    version = response.json()
    if detail:
        version["client"] = __version__
        return version
    else:
        return version["services"]

def _cli_get_version(detail=False):
    if detail:
        return json_to_cli(get_version, detail=detail)
    else:
        return get_version(), 0
