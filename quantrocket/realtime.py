# Copyright 2019 QuantRocket - All Rights Reserved
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

def create_db(code, universes=None, conids=None, vendor=None,
              fields=None, primary_exchange=False):
    """
    Create a new real-time database.


    The market data requirements you specify when you create a new database are
    applied each time you collect data for that database.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    universes : list of str
        include these universes

    conids : list of int
        include these conids

    vendor : str, optional
        the vendor to collect data from (default 'ib'. Possible choices: ib)

    fields : list of str
        collect these fields (pass '?' or any invalid fieldname to see
        available fields, default fields are 'last' and 'volume')

    primary_exchange : bool
        limit to data from the primary exchange (default False)

    Returns
    -------
    dict
        status message

    Examples
    --------
    Create a database for collecting real-time trades and volume for US stocks:

    >>> create_db("usa-stk-trades", universes="usa-stk")

    Create a database for collecting trades and quotes for a universe of futures:

    >>> create_db("globex-fut-taq", universes="globex-fut",
                  fields=["last", "volume", "bid", "ask", "bid_size", "ask_size"])
    """
    params = {}
    if universes:
        params["universes"] = universes
    if conids:
        params["conids"] = conids
    if vendor:
        params["vendor"] = vendor
    if fields:
        params["fields"] = fields
    if primary_exchange:
        params["primary_exchange"] = primary_exchange

    response = houston.put("/realtime/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_db(*args, **kwargs):
    return json_to_cli(create_db, *args, **kwargs)

def get_db_config(code):
    """
    Return the configuration for a real-time database.

    Parameters
    ----------
    code : str, required
        the database code

    Returns
    -------
    dict
        config

    """
    response = houston.get("/realtime/databases/{0}".format(code))
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_db_config(*args, **kwargs):
    return json_to_cli(get_db_config, *args, **kwargs)

def drop_db(code, confirm_by_typing_db_code_again=None):
    """
    Delete a real-time database.

    Deleting a real-time database deletes its configuration and data and is
    irreversible.

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
    response = houston.delete("/realtime/databases/{0}".format(code), params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_drop_db(*args, **kwargs):
    return json_to_cli(drop_db, *args, **kwargs)

def collect_market_data(codes, conids=None, universes=None, fields=None, until=None,
                        snapshot=False, wait=False):
    """
    Collect real-time market data and save it to a real-time database.

    A single snapshot of market data or a continuous stream of market data can
    be collected, depending on the `snapshot` parameter.

    Streaming real-time data is collected until cancelled, or can be scheduled
    for cancellation using the `until` parameter.

    Parameters
    ----------
    codes : list of str, required
        the database code(s) to collect data for

    conids : list of int, optional
        collect market data for these conids, overriding db config (typically
        used to collect a subset of securities)

    universes : list of str, optional
        collect market data for these universes, overriding db config (typically
        used to collect a subset of securities)

    fields : list of str, optional
        limit to these fields, overriding db config

    until : str, optional
        schedule data collection to end at this time. Can be a datetime
        (YYYY-MM-DD HH:MM:SS), a time (HH:MM:SS), or a Pandas timedelta
        string (e.g. 2h or 30min). If not provided, market data is collected
        until cancelled.

    snapshot : bool
        collect a snapshot of market data (default is to collect a continuous
        stream of market data)

    wait : bool
        wait for market data snapshot to complete before returning (default is
        to return immediately). Requires 'snapshot=True'

    Returns
    -------
    dict
        status message

    Examples
    --------
    Collect market data for all securities in a database called 'japan-banks-trades':

    >>> collect_market_data("japan-banks-trades")

    Collect market data for a subset of securities in a database called 'usa-stk-trades'
    and automatically cancel the data collection in 30 minutes:

    >>> collect_market_data("usa-stk-trades", conids=[12345,23456,34567], until="30m")

    Collect a market data snapshot and wait until it completes:

    >>> collect_market_data("usa-stk-trades", snapshot=True, wait=True)
    """
    params = {}
    if codes:
        params["codes"] = codes
    if conids:
        params["conids"] = conids
    if universes:
        params["universes"] = universes
    if fields:
        params["fields"] = fields
    if until:
        params["until"] = until
    if snapshot:
        params["snapshot"] = snapshot
    if wait:
        params["wait"] = wait
    response = houston.post("/realtime/subscriptions", params=params, timeout=3600 if wait else 30)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_market_data(*args, **kwargs):
    return json_to_cli(collect_market_data, *args, **kwargs)

def get_active_subscriptions(detail=False):
    """
    Return the number of currently subscribed tickers by vendor and
    database.

    Parameters
    ----------

    detail : bool
        return lists of subscribed tickers (default is to return
        counts of subscribed tickers)

    Returns
    -------
    dict
        subscribed tickers by vendor and database

    """
    params = {}
    if detail:
        params["detail"] = detail

    response = houston.get("/realtime/subscriptions", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_active_subscriptions(*args, **kwargs):
    return json_to_cli(get_active_subscriptions, *args, **kwargs)

def cancel_market_data(codes=None, conids=None, universes=None, cancel_all=False):
    """
    Cancel market data collection.

    Parameters
    ----------
    codes : list of str, optional
        the database code(s) to cancel collection for

    conids : list of int, optional
        cancel market data for these conids, overriding db config

    universes : list of str, optional
        cancel market data for these universes, overriding db config

    cancel_all : bool
        cancel all market data collection

    Returns
    -------
    dict
        subscribed tickers by vendor and database, after cancellation

    Examples
    --------
    Cancel market data collection for a database called 'globex-fut-taq':

    >>> cancel_market_data("globex-fut-taq")

    Cancel all market data collection:

    >>> cancel_market_data(cancel_all=True)
    """
    params = {}
    if codes:
        params["codes"] = codes
    if conids:
        params["conids"] = conids
    if universes:
        params["universes"] = universes
    if cancel_all:
        params["cancel_all"] = cancel_all

    response = houston.delete("/realtime/subscriptions", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_cancel_market_data(*args, **kwargs):
    return json_to_cli(cancel_market_data, *args, **kwargs)
