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

def list_exchanges(regions=None, sec_types=None):
    """
    List exchanges by security type and country as found on the IB website.

    Parameters
    ----------
    regions : list of str, optional
        limit to these regions. Possible choices: north_america, europe, asia, global

    sec_types : list of str, optional
        limit to these securitiy types. Possible choices: STK, ETF, FUT, CASH, IND

    Returns
    -------
    dict
    """
    params = {}
    if sec_types:
        params["sec_types"] = sec_types
    if regions:
        params["regions"] = regions

    response = houston.get("/master/exchanges", params=params)
    return houston.json_if_possible(response)

def _cli_list_exchanges(*args, **kwargs):
    return json_to_cli(list_exchanges, *args, **kwargs)

def download_listings(exchange=None, sec_types=None, currencies=None, symbols=None,
                        groups=None, conids=None):
    """
    Download securities listings from IB into securities master database, either by exchange or by groups/conids.


    Specify an exchange (optionally filtering by security type, currency, and/or symbol) to fetch
    listings from the IB website and download associated contract details from the IB API. Or, specify groups
    or conids to download details from the IB API, bypassing the website.

    Parameters
    ----------
    exchange : str
        the exchange code to download listings for (required unless providing groups or conids)

    sec_types : list of str, optional
        limit to these security types. Possible choices: STK, ETF, FUT, CASH, IND

    currencies : list of str, optional
        limit to these currencies

    symbols : list of str, optional
        limit to these symbols

    groups : list of str, optional
        limit to these groups

    conids : list of int, optional
        limit to these conids

    Returns
    -------
    dict
        status message

    """
    params = {}
    if exchange:
        params["exchange"] = exchange
    if sec_types:
        params["sec_types"] = sec_types
    if currencies:
        params["currencies"] = currencies
    if symbols:
        params["symbols"] = symbols
    if groups:
        params["groups"] = groups
    if conids:
        params["conids"] = conids

    response = houston.post("/master/listings", params=params)
    return houston.json_if_possible(response)

def _cli_download_listings(*args, **kwargs):
    return json_to_cli(download_listings, *args, **kwargs)
