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

from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli

def fetch_reuters_statements(universes=None, conids=None):
    """
    Fetch Reuters financial statements from IB and save to database.

    This data provides cash flow, balance sheet, and income metrics.

    Parameters
    ----------
    universes : list of str, optional
        limit to these universes (must provide universes, conids, or both)

    conids : list of int, optional
        limit to these conids (must provide universes, conids, or both)

    Returns
    -------
    dict
        status message

    """
    params = {}
    if universes:
        params["universes"] = universes
    if conids:
        params["conids"] = conids
    response = houston.post("/fundamental/reuters/statements", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_fetch_reuters_statements(*args, **kwargs):
    return json_to_cli(fetch_reuters_statements, *args, **kwargs)

def fetch_reuters_estimates(universes=None, conids=None):
    """
    Fetch Reuters estimates and actuals from IB and save to database.

    This data provides analyst estimates and actuals for a variety of indicators.

    Parameters
    ----------
    universes : list of str, optional
        limit to these universes (must provide universes, conids, or both)

    conids : list of int, optional
        limit to these conids (must provide universes, conids, or both)

    Returns
    -------
    dict
        status message

    """
    params = {}
    if universes:
        params["universes"] = universes
    if conids:
        params["conids"] = conids
    response = houston.post("/fundamental/reuters/estimates", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_fetch_reuters_estimates(*args, **kwargs):
    return json_to_cli(fetch_reuters_estimates, *args, **kwargs)