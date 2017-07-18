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

import sys
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.stream import to_bytes

def create_db(code, universes=None, start_date=None, end_date=None,
              vendor=None, bar_size=None, bar_type=None, outside_rth=False,
              primary_exchange=False, times=None, dividend_adjust=False,
              no_config=False, config_filepath_or_buffer=None):
    """
    Create a new history database.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    universes : list of str, required unless loadable = True
        include these universes

    start_date : str (YYYY-MM-DD), optional
        fetch history back to this start date (default is to fetch as far back as data
        is available)

    end_date : str (YYYY-MM-DD), optional
        fetch history up to this end date (default is to fetch up to the present)

    vendor : str, required unless loadable = True
        the vendor to fetch data from (defaults to 'ib' which is currently the only
        supported vendor)

    bar_size : str, required for vendor ib
        the bar size to fetch. Possible choices:
            "1 secs", "5 secs",	"10 secs", "15 secs", "30 secs",
            "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins",
            "1 hour", "2 hours", "3 hours", "4 hours", "8 hours",
            "1 day",
            "1 week",
            "1 month"

    bar_type : str, optional
        the bar type to fetch (if not specified, defaults to MIDPOINT for forex and
        TRADES for everything else)

    outside_rth : bool
        include data from outside regular trading hours (default is to limit to regular
        trading hours)

    primary_exchange : bool
        limit to data from the primary exchange (default False)

    times : list of str (HH:MM:SS), optional
        limit to these times

    dividend_adjust : bool
        adjust for dividends

    no_config : bool
        create a database with no config (data can be loaded manually instead of fetched
        from a vendor)

    config_filepath_or_buffer : str or file-like object, optional
        a YAML config file defining the historical data requirements (specify '-' to read file from stdin)

    Returns
    -------
    dict
        status message

    """
    params = {}
    if universes:
        params["universes"] = universes
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if vendor:
        params["vendor"] = vendor
    if bar_size:
        params["bar_size"] = bar_size
    if bar_type:
        params["bar_type"] = bar_type
    if outside_rth:
        params["outside_rth"] = outside_rth
    if primary_exchange:
        params["primary_exchange"] = primary_exchange
    if times:
        params["times"] = times
    if dividend_adjust:
        params["dividend_adjust"] = dividend_adjust
    if no_config:
        params["no_config"] = True

    if config_filepath_or_buffer == "-":
        response = houston.put("/history/databases/{0}".format(code), params=params,
                               data=to_bytes(sys.stdin))

    elif config_filepath_or_buffer and hasattr(config_filepath_or_buffer, "read"):
        response = houston.put("/history/databases/{0}".format(code), params=params,
                               data=to_bytes(config_filepath_or_buffer))

    elif config_filepath_or_buffer:
        with open(config_filepath_or_buffer, "rb") as f:
            response = houston.put("/history/databases/{0}".format(code), params=params, data=f)

    else:
        response = houston.put("/history/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_db(*args, **kwargs):
    return json_to_cli(create_db, *args, **kwargs)

def get_db_config(code):
    """
    Return the configuration for a history database.

    Parameters
    ----------
    code : str, required
        the database code

    Returns
    -------
    dict
        config

    """
    response = houston.get("/history/databases/{0}".format(code))
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_db_config(*args, **kwargs):
    return json_to_cli(get_db_config, *args, **kwargs)

def drop_db(code, confirm_by_typing_db_code_again=None):
    """
    Delete a history database.

    Parameters
    ----------
    code : str, required
        the database code

    confirm_by_typing_db_code_again : str, required
       enter the db code again to confirm you want to drop the database, its config,
       and all its data

    Returns
    -------
    dict
        status message

    """
    params = {"confirm_by_typing_db_code_again": confirm_by_typing_db_code_again}
    response = houston.delete("/history/databases/{0}".format(code), params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_drop_db(*args, **kwargs):
    return json_to_cli(drop_db, *args, **kwargs)
