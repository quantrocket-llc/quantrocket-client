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
Functions for setting and viewing software licenses and third-party API
keys.

Functions
---------
get_license_profile
    Return the current license profile.

set_license
    Set QuantRocket license key.

get_alpaca_key
    Returns the current API key(s) for Alpaca.

set_alpaca_key
    Set Alpaca API key.

get_polygon_key
    Returns the current API key for Polygon.

set_polygon_key
    Set Polygon API key.

get_quandl_key
    Returns the current API key for Quandl.

set_quandl_key
    Set Quandl API key.

Notes
-----
Usage Guide:

* License Key: https://qrok.it/dl/qr/license
* Broker and Data Connections: https://qrok.it/dl/qr/connect
"""
import getpass
from quantrocket.houston import houston
from quantrocket._cli.utils.output import json_to_cli
from typing import Literal

__all__ = [
    "get_license_profile",
    "set_license",
    "get_alpaca_key",
    "set_alpaca_key",
    "get_polygon_key",
    "set_polygon_key",
    "get_quandl_key",
    "set_quandl_key",
]

def get_license_profile(force_refresh: bool = False) -> dict[str, str]:
    """
    Return the current license profile.

    Parameters
    ----------
    force_refresh : bool
        refresh the license profile before returning it (default is to
        return the cached profile, which is refreshed every few minutes)

    Returns
    -------
    dict
        license profile

    Notes
    -----
    Usage Guide:

    * License Key: https://qrok.it/dl/qr/license
    """
    params = {}
    if force_refresh:
        params["force_refresh"] = force_refresh

    response = houston.get("/license-service/license", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_license_profile(*args, **kwargs):
    return json_to_cli(get_license_profile, *args, **kwargs)

def set_license(key: str) -> dict[str, str]:
    """
    Set QuantRocket license key.

    Parameters
    ----------
    key : str, required
        the license key for your account

    Returns
    -------
    dict
        license profile

    Notes
    -----
    Usage Guide:

    * License Key: https://qrok.it/dl/qr/license
    """
    response = houston.put("/license-service/license/{0}".format(key))
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_set_license(*args, **kwargs):
    return json_to_cli(set_license, *args, **kwargs)

def get_alpaca_key() -> dict[str, str]:
    """
    Returns the current API key(s) for Alpaca.

    Returns
    -------
    dict
        credentials

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    response = houston.get("/license-service/credentials/alpaca")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_alpaca_key(
    api_key: str,
    trading_mode: Literal["paper", "live"],
    secret_key: str = None,
    realtime_data: Literal["iex", "sip"] = "iex"
    ) -> dict[str, str]:
    """
    Set Alpaca API key.

    Your credentials are encrypted at rest and never leave
    your deployment.

    Parameters
    ----------
    api_key : str, required
        Alpaca API key ID

    trading_mode : str, required
        the trading mode of this API key ('paper' or 'live')

    secret_key : str, optional
        Alpaca secret key (if omitted, will be prompted for secret key)

    realtime_data : str, optional
        the real-time data feed to which this API key is subscribed. Possible
        choices: 'iex', 'sip'. Default is 'iex'.

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    if not secret_key:
        secret_key = getpass.getpass(prompt="Enter Alpaca secret key: ")

    data = {}
    data["api_key"] = api_key
    data["secret_key"] = secret_key
    data["trading_mode"] = trading_mode
    data["realtime_data"] = realtime_data

    response = houston.put("/license-service/credentials/alpaca", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_alpaca_key(*args, **kwargs):
    if any(kwargs.values()):
        return json_to_cli(set_alpaca_key, *args, **kwargs)
    else:
        return json_to_cli(get_alpaca_key)

def get_polygon_key() -> dict[str, str]:
    """
    Returns the current API key for Polygon.

    Returns
    -------
    dict
        credentials

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    response = houston.get("/license-service/credentials/polygon")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_polygon_key(api_key: str) -> dict[str, str]:
    """
    Set Polygon API key.

    Your credentials are encrypted at rest and never leave
    your deployment.

    Parameters
    ----------
    api_key : str, required
        Polygon API key

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    data = {}
    data["api_key"] = api_key

    response = houston.put("/license-service/credentials/polygon", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_polygon_key(*args, **kwargs):
    if any(kwargs.values()):
        return json_to_cli(set_polygon_key, *args, **kwargs)
    else:
        return json_to_cli(get_polygon_key)

def get_quandl_key() -> dict[str, str]:
    """
    Returns the current API key for Quandl.

    Returns
    -------
    dict
        credentials

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    response = houston.get("/license-service/credentials/quandl")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_quandl_key(api_key: str) -> dict[str, str]:
    """
    Set Quandl API key.

    Your credentials are encrypted at rest and never leave
    your deployment.

    Parameters
    ----------
    api_key : str, required
        Quandl API key

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    data = {}
    data["api_key"] = api_key

    response = houston.put("/license-service/credentials/quandl", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_quandl_key(*args, **kwargs):
    if any(kwargs.values()):
        return json_to_cli(set_quandl_key, *args, **kwargs)
    else:
        return json_to_cli(get_quandl_key)
