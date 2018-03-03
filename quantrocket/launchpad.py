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

import os
import webbrowser
from quantrocket.houston import houston
from quantrocket.exceptions import UnavailableInsideJupyter
from quantrocket.cli.utils.output import json_to_cli

def list_gateway_statuses(exchanges=None, sec_type=None, research_vendors=None, status=None,
                          gateways=None):
    """
    Query statuses of IB Gateway services.

    Parameters
    ----------
    exchanges : list of str, optional
        limit to IB Gateway services with market data permission for these exchanges

    sec_type : str, optional
        limit to IB Gateway services with market data permission for this securitiy type (useful for disambiguating permissions for exchanges that trade multiple asset classes). Possible choices: STK, FUT, CASH, OPT

    research_vendors : list of str, optional
        limit to IB Gateway services with permission for these research vendors (choices: reuters, wsh)

    status : str, optional
        limit to IB Gateway services in this status. Possible choices: running, stopped, error

    gateways : list of str, optional
        limit to these IB Gateway services

    Returns
    -------
    dict of gateway:status (if status arg not provided), or list of gateways (if status arg provided)
    """
    params = {}
    if sec_type:
        params["sec_type"] = sec_type
    if exchanges:
        params["exchanges"] = exchanges
    if research_vendors:
        params["research_vendors"] = research_vendors
    if gateways:
        params["gateways"] = gateways
    if status:
        params["status"] = status

    response = houston.get("/launchpad/gateways", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_gateway_statuses(*args, **kwargs):
    return json_to_cli(list_gateway_statuses, *args, **kwargs)

def start_gateways(exchanges=None, sec_type=None, research_vendors=None, gateways=None, wait=False):
    """
    Start one or more IB Gateway services.

    Parameters
    ----------
    exchanges : list of str, optional
        limit to IB Gateway services with market data permission for these exchanges

    sec_type : str, optional
        limit to IB Gateway services with market data permission for this securitiy type (useful for disambiguating permissions for exchanges that trade multiple asset classes). Possible choices: STK, FUT, CASH, OPT

    research_vendors : list of str, optional
        limit to IB Gateway services with permission for these research vendors (choices: reuters, wsh)

    gateways : list of str, optional
        limit to these IB Gateway services

    wait: bool
        wait for the IB Gateway services to start before returning (default is to start the gateways asynchronously)

    Returns
    -------
    dict
        status message
    """
    params = {"wait": wait}
    if sec_type:
        params["sec_type"] = sec_type
    if exchanges:
        params["exchanges"] = exchanges
    if research_vendors:
        params["research_vendors"] = research_vendors
    if gateways:
        params["gateways"] = gateways

    response = houston.post("/launchpad/gateways", params=params, timeout=120)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_start_gateways(*args, **kwargs):
    return json_to_cli(start_gateways, *args, **kwargs)

def stop_gateways(exchanges=None, sec_type=None, research_vendors=None, gateways=None, wait=False):
    """
    Stop one or more IB Gateway services.

    Parameters
    ----------
    exchanges : list of str, optional
        limit to IB Gateway services with market data permission for these exchanges

    sec_type : str, optional
        limit to IB Gateway services with market data permission for this securitiy type (useful for disambiguating permissions for exchanges that trade multiple asset classes). Possible choices: STK, FUT, CASH, OPT

    research_vendors : list of str, optional
        limit to IB Gateway services with permission for these research vendors (choices: reuters, wsh)

    gateways : list of str, optional
        limit to these IB Gateway services

    wait: bool
        wait for the IB Gateway services to stop before returning (default is to stop the gateways asynchronously)

    Returns
    -------
    dict
        status message
    """
    params = {"wait": wait}
    if sec_type:
        params["sec_type"] = sec_type
    if exchanges:
        params["exchanges"] = exchanges
    if research_vendors:
        params["research_vendors"] = research_vendors
    if gateways:
        params["gateways"] = gateways

    response = houston.delete("/launchpad/gateways", params=params, timeout=45)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_stop_gateways(*args, **kwargs):
    return json_to_cli(stop_gateways, *args, **kwargs)

def load_launchpad_config(filename):
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
    houston.raise_for_status_with_json(response)
    return response.json()

def get_launchpad_config():
    """
    Returns the current config.

    Returns
    -------
    dict
        the config as a dict
    """
    response = houston.get("/launchpad/config")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def _cli_load_or_show_config(filename=None):
    if filename:
        return json_to_cli(load_launchpad_config, filename)
    else:
        return json_to_cli(get_launchpad_config)

def open_ibg_gui(gateways=None):
    """
    Access the IB Gateway GUI in a web browser.

    Note: IB Gateway must already be running.

    Parameters
    ----------
    gateways : list of str, optional
        limit to these IB Gateway services (default all IB Gateway services)

    Returns
    -------
    None
    """
    if os.environ.get("YOU_ARE_INSIDE_JUPYTER", False):
        raise UnavailableInsideJupyter("""Cannot open GUI inside Jupyter

Please use the Jupyter commands menu to open the IB Gateway GUI
(Commands > QuantRocket > IB Gateway GUI)
""")

    if not gateways:
        gateways = sorted(list_gateway_statuses())
    for gateway in gateways:
        url = "{0}/{1}/vnc".format(houston.base_url, gateway)
        webbrowser.open(url)

def _cli_open_ibg_gui(*args, **kwargs):
    return json_to_cli(open_ibg_gui, *args, **kwargs)