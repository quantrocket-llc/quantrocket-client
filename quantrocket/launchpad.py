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

def list_gateway_statuses(exchanges=None, sec_type=None, status=None, gateways=None):
    """
    Lists statuses and permissions of IB Gateway services.

    Parameters
    ----------
    exchanges : list, optional
        limit to IB Gateway services with market data permission for these exchanges

    sec_type : str, optional
        limit to IB Gateway services with market data permission for these securitiy types (useful for disambiguating permissions for exchanges that trade multiple asset classes). Possible choices: STK, FUT

    status : str, optional
        limit to IB Gateway services in this status. Possible choices: running, stopped, error

    gateways : list, optional
        limit to these IB Gateway services

    Returns
    -------
    dict of gateways, statuses, and permissions
    """
    raise NotImplementedError("This service is not yet available")

def start_gateways(exchanges=None, sec_type=None, gateways=None):
    """
    Starts one or more IB Gateway services.

    Parameters
    ----------
    exchanges : list, optional
        limit to IB Gateway services with market data permission for these exchanges

    sec_type : str, optional
        limit to IB Gateway services with market data permission for these securitiy types (useful for disambiguating permissions for exchanges that trade multiple asset classes). Possible choices: STK, FUT

    gateways : list, optional
        limit to these IB Gateway services

    Returns
    -------
    dict of started gateways, statuses, and permissions
    """
    raise NotImplementedError("This service is not yet available")

def stop_gateways(exchanges=None, sec_type=None, gateways=None):
    """
    Stops one or more IB Gateway services.

    Parameters
    ----------
    exchanges : list, optional
        limit to IB Gateway services with market data permission for these exchanges

    sec_type : str, optional
        limit to IB Gateway services with market data permission for these securitiy types (useful for disambiguating permissions for exchanges that trade multiple asset classes). Possible choices: STK, FUT

    gateways : list, optional
        limit to these IB Gateway services

    Returns
    -------
    dict of stopped gateways, statuses, and permissions
    """
    raise NotImplementedError("This service is not yet available")

def load_config(filename):
    """
    Uploads a new config.

    Parameters
    ----------
    filename : str, required
        the config file to upload to the launchpad service

    Returns
    -------
    dict
        status message
    """
    with open(filename) as file:
        response = houston.put("/launchpad/config", data=file.read())
    response.raise_for_status()
    return response.json()

def get_config():
    """
    Returns the current config.

    Returns
    -------
    dict
        config
    """
    response = houston.get("/launchpad/config")
    response.raise_for_status()
    return response.text

def _load_or_show_config(filename=None):
    if filename:
        return load_config(filename)
    else:
        return get_config()
