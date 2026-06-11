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

get_snaptrade_credentials
    Returns the current credentials for SnapTrade.

set_snaptrade_credentials
    Set SnapTrade credentials.

get_massive_key
    Returns the current API key for Massive.

set_massive_key
    Set Massive API key.

get_nasdaq_key
    Returns the current API key for Nasdaq Data Link.

set_nasdaq_key
    Set Nasdaq Data Link API key.

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
    "get_snaptrade_credentials",
    "set_snaptrade_credentials",
    "get_massive_key",
    "set_massive_key",
    "get_nasdaq_key",
    "set_nasdaq_key",
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

def get_snaptrade_credentials() -> dict[str, str]:
    """
    Returns the current credentials for SnapTrade.

    Returns
    -------
    dict
        credentials

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    response = houston.get("/license-service/credentials/snaptrade")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_snaptrade_credentials(
    client_id: str,
    consumer_key: str,
    user_id: str,
    user_secret_key: str
) -> dict[str, str]:
    """
    Set SnapTrade credentials.

    Your credentials are encrypted at rest and never leave
    your deployment.

    Parameters
    ----------
    client_id : str, required
        SnapTrade client ID

    consumer_key : str, required
        SnapTrade consumer key (if omitted, will be prompted for consumer key)

    user_id : str, required
        SnapTrade user ID

    user_secret_key : str, required
        SnapTrade user secret key (if omitted, will be prompted for user secret key)

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    if not consumer_key:
        consumer_key = getpass.getpass(prompt="Enter SnapTrade consumer key: ")

    if not user_secret_key:
        user_secret_key = getpass.getpass(prompt="Enter SnapTrade user secret key: ")

    data = {}
    data["client_id"] = client_id
    data["consumer_key"] = consumer_key
    data["api_key"] = user_id
    data["secret_key"] = user_secret_key

    response = houston.put("/license-service/credentials/snaptrade", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_snaptrade_credentials(*args, **kwargs):
    if any(kwargs.values()):
        return json_to_cli(set_snaptrade_credentials, *args, **kwargs)
    else:
        return json_to_cli(get_snaptrade_credentials)

def get_massive_key() -> dict[str, str]:
    """
    Returns the current API key for Massive.

    Returns
    -------
    dict
        credentials

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    response = houston.get("/license-service/credentials/massive")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_massive_key(api_key: str) -> dict[str, str]:
    """
    Set Massive API key.

    Your credentials are encrypted at rest and never leave
    your deployment.

    Parameters
    ----------
    api_key : str, required
        Massive API key

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

    response = houston.put("/license-service/credentials/massive", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_massive_key(*args, **kwargs):
    if any(kwargs.values()):
        return json_to_cli(set_massive_key, *args, **kwargs)
    else:
        return json_to_cli(get_massive_key)

def get_nasdaq_key() -> dict[str, str]:
    """
    Returns the current API key for Nasdaq Data Link.

    Returns
    -------
    dict
        credentials

    Notes
    -----
    Usage Guide:

    * Broker and Data Connections: https://qrok.it/dl/qr/connect
    """
    response = houston.get("/license-service/credentials/nasdaq")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_nasdaq_key(api_key: str) -> dict[str, str]:
    """
    Set Nasdaq Data Link API key.

    Your credentials are encrypted at rest and never leave
    your deployment.

    Parameters
    ----------
    api_key : str, required
        Nasdaq Data Link API key

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

    response = houston.put("/license-service/credentials/nasdaq", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_nasdaq_key(*args, **kwargs):
    if any(kwargs.values()):
        return json_to_cli(set_nasdaq_key, *args, **kwargs)
    else:
        return json_to_cli(get_nasdaq_key)