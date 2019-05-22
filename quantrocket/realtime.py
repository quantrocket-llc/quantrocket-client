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

import sys
import requests
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.houston import houston
from quantrocket.exceptions import NoRealtimeData
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

def download_market_data_file(code, filepath_or_buffer=None, output="csv",
                              start_date=None, end_date=None,
                              universes=None, conids=None,
                              exclude_universes=None, exclude_conids=None,
                              fields=None):
    """
    Query market data from a real-time database and download to file.

    Parameters
    ----------
    code : str, required
        the code of the database to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD HH:MM:SS), optional
        limit to market data on or after this datetime. Can pass a date (YYYY-MM-DD),
        datetime with optional timezone (YYYY-MM-DD HH:MM:SS TZ), or time with
        optional timezone. A time without date will be interpreted as referring to
        today if the time is earlier than now, or yesterday if the time is later than
        now.

    end_date : str (YYYY-MM-DD HH:MM:SS), optional
        limit to market data on or before this datetime. Can pass a date (YYYY-MM-DD),
        datetime with optional timezone (YYYY-MM-DD HH:MM:SS TZ), or time with
        optional timezone.

    universes : list of str, optional
        limit to these universes (default is to return all securities in database)

    conids : list of int, optional
        limit to these conids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_conids : list of int, optional
        exclude these conids

    fields : list of str, optional
        only return these fields (pass '?' or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    Examples
    --------
    Download a CSV of futures market data since 08:00 AM Chicago time:

    >>> download_market_data_file("globex-fut-taq",
                                 start_date="08:00:00 America/Chicago",
                                 filepath_or_buffer="globex_taq.csv")
    >>> market_data = pd.read_csv("globex_taq.csv", parse_dates=["Date"])
    """
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if universes:
        params["universes"] = universes
    if conids:
        params["conids"] = conids
    if exclude_universes:
        params["exclude_universes"] = exclude_universes
    if exclude_conids:
        params["exclude_conids"] = exclude_conids
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/realtime/{0}.{1}".format(code, output), params=params,
                           timeout=60*30)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        if "no market data matches the query parameters" in repr(e).lower():
            raise NoRealtimeData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_market_data_file(*args, **kwargs):
    return json_to_cli(download_market_data_file, *args, **kwargs)
