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

import six
import os
import sys
import requests
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.stream import to_bytes
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.exceptions import NoHistoricalData
from quantrocket.utils.warn import deprecated_replaced_by

TMP_DIR = os.environ.get("QUANTROCKET_TMP_DIR", "/tmp")

def create_db(code, universes=None, conids=None, start_date=None, end_date=None,
              vendor=None, bar_size=None, bar_type=None, outside_rth=False,
              primary_exchange=False, times=None, between_times=None,
              shard=None, no_config=False, config_filepath_or_buffer=None):
    """
    Create a new history database.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    universes : list of str
        include these universes

    conids : list of int
        include these conids

    start_date : str (YYYY-MM-DD), optional
        collect history back to this start date (default is to collect as far back as data
        is available)

    end_date : str (YYYY-MM-DD), optional
        collect history up to this end date (default is to collect up to the present)

    vendor : str, optional
        the vendor to collect data from (default 'ib'. Possible choices: ib, sharadar)

    bar_size : str, required for vendor ib
        the bar size to collect. Possible choices:
        "1 secs", "5 secs",	"10 secs", "15 secs", "30 secs",
        "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins",
        "1 hour", "2 hours", "3 hours", "4 hours", "8 hours",
        "1 day",
        "1 week",
        "1 month"

    bar_type : str, optional
        the bar type to collect (if not specified, defaults to MIDPOINT for forex and
        TRADES for everything else). Possible choices:
        "TRADES",
        "ADJUSTED_LAST",
        "MIDPOINT",
        "BID",
        "ASK",
        "BID_ASK",
        "HISTORICAL_VOLATILITY",
        "OPTION_IMPLIED_VOLATILITY"

    outside_rth : bool
        include data from outside regular trading hours (default is to limit to regular
        trading hours)

    primary_exchange : bool
        limit to data from the primary exchange (default False)

    times : list of str (HH:MM:SS), optional
        limit to these times (refers to the bar's start time; mutually exclusive
        with `between_times`)

    between_times : list of str (HH:MM:SS), optional
        limit to times between these two times (refers to the bar's start time;
        mutually exclusive with `times`)

    shard : str, optional
        whether and how to shard the database, i.e. break it into smaller pieces.
        Required for intraday databases. Possible choices are `year` (separate
        database for each year), `time` (separate database for each bar time),
        `conid` (separate database for each security), `conid,time` (duplicate copies
        of database, one sharded by conid and the other by time), or `off` (no
        sharding). See http://qrok.it/h/shard for more help.

    no_config : bool
        create a database with no config (data can be loaded manually instead of collected
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
    if conids:
        params["conids"] = conids
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
    if between_times:
        params["between_times"] = between_times
    if shard:
        params["shard"] = shard
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

def list_databases():
    """
    List history databases.

    Returns
    -------
    list
        list of database codes

    """
    response = houston.get("/history/databases")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_databases(*args, **kwargs):
    return json_to_cli(list_databases, *args, **kwargs)

def collect_history(codes, priority=False, conids=None, universes=None,
                    start_date=None, end_date=None, availability_only=False,
                    delist_missing=False):
    """
    Collect historical market data from IB and save it to a history database. The request is
    queued and the data is collected asynchronously.

    Parameters
    ----------
    codes : list of str, required
        the database code(s) to collect data for

    priority : bool
        use the priority queue (default is to use the standard queue)

    conids : list of int, optional
        collect history for these conids, overriding config (typically
        used to collect a subset of securities)

    universes : list of str, optional
        collect history for these universes, overriding config (typically
        used to collect a subset of securities)

    start_date : str (YYYY-MM-DD), optional
        collect history back to this start date, overriding config

    end_date : str (YYYY-MM-DD), optional
        collect history up to this end date, overriding config

    availability_only : bool
        determine and store how far back data is available but
        don't yet collect the data

    delist_missing : bool
        auto-delist securities that are no longer available from IB

    Returns
    -------
    dict
        status message

    """
    params = {}
    if codes:
        params["codes"] = codes
    if priority:
        params["priority"] = priority
    if conids:
        params["conids"] = conids
    if universes:
        params["universes"] = universes
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if availability_only:
        params["availability_only"] = availability_only
    if delist_missing:
        params["delist_missing"] = delist_missing
    response = houston.post("/history/queue", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_history(*args, **kwargs):
    return json_to_cli(collect_history, *args, **kwargs)

def get_history_queue():
    """
    Get the current queue of historical data collections.

    Returns
    -------
    dict
        standard and priority queues

    """
    response = houston.get("/history/queue")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_history_queue(*args, **kwargs):
    return json_to_cli(get_history_queue, *args, **kwargs)

def cancel_collections(codes, queues=None):
    """
    Cancel running or pending historical data collections.

    Parameters
    ----------
    codes : list of str, required
        the database code(s) to cancel collections for

    queues : list of str, optional
        only cancel collections in these queues. Possible choices: standard, priority

    Returns
    -------
    dict
        standard and priority queues

    """
    params = {}
    if codes:
        params["codes"] = codes
    if queues:
        params["queues"] = queues
    response = houston.delete("/history/queue", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_cancel_collections(*args, **kwargs):
    return json_to_cli(cancel_collections, *args, **kwargs)

def wait_for_collections(codes, timeout=None):
    """
    Wait for historical data collection to finish.

    Parameters
    ----------
    codes : list of str, required
        the database code(s) to wait for

    timeout : str, optional
        time out if data collection hasn't finished after this much time (use Pandas
        timedelta string, e.g. 30sec or 5min or 2h)

    Returns
    -------
    dict
        status message

    """
    params = {}
    params["codes"] = codes
    if timeout:
        params["timeout"] = timeout
    response = houston.put("/history/queue", params=params, timeout=60*60*24*365)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_wait_for_collections(*args, **kwargs):
    return json_to_cli(wait_for_collections, *args, **kwargs)

def download_history_availability_file(code, filepath_or_buffer=None, output="csv"):
    """
    Query historical market data availability from a history database and download to file.

    This function is normally called after running:

        quantrocket history collect mydb --availability

    Parameters
    ----------
    code : str, required
        the code of the database to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    Returns
    -------
    None

    See Also
    --------
    get_history_availability : load historical availability into Series
    """
    output = output or "csv"

    if output not in ("csv", "json", "txt"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/history/availability/{0}.{1}".format(code, output))

    houston.raise_for_status_with_json(response)
    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_history_availability_file(*args, **kwargs):
    return json_to_cli(download_history_availability_file, *args, **kwargs)

def get_history_availability(code):
    """
    Query historical data availability from a history database, returning a
    Series of start dates (with conids as the index) representing how far back
    data can be collected from IB.

    This function is normally called after running a command such as:

        quantrocket history collect [DB] --availability

    Parameters
    ----------
    code : str, required
        the code of the database to query

    Returns
    -------
    Series
        Series of start dates by conid

    Examples
    --------
    Load start dates and display cumulative number of tickers available by
    start year:

    >>> start_dates = get_history_availability("mydb")
    >>> cum_ticker_counts = start_dates.groupby(start_dates.dt.year).count().cumsum()
    >>> print(cum_ticker_counts)
    StartDate
    1984    420
    1985    493
    1986    539
    1987    648
    ...
    """
    # Import pandas lazily since it can take a moment to import
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    f = six.StringIO()
    download_history_availability_file(code, f)
    start_dates = pd.read_csv(f, index_col="ConId", parse_dates=["StartDate"])
    return start_dates.StartDate

def download_history_file(code, filepath_or_buffer=None, output="csv",
                          start_date=None, end_date=None,
                          universes=None, conids=None,
                          exclude_universes=None, exclude_conids=None,
                          times=None, cont_fut=None, fields=None, tz_naive=False):
    """
    Query historical market data from a history database and download to file.

    Parameters
    ----------
    code : str, required
        the code of the database to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, txt, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to history on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to history on or before this date

    universes : list of str, optional
        limit to these universes (default is to return all securities in database)

    conids : list of int, optional
        limit to these conids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_conids : list of int, optional
        exclude these conids

    times: list of str (HH:MM:SS), optional
        limit to these times

    cont_fut : str
        stitch futures into continuous contracts using this method (default is not
        to stitch together). Possible choices: concat

    fields : list of str, optional
        only return these fields (pass ['?'] or any invalid fieldname to see
        available fields)

    tz_naive : bool
        return timestamps without UTC offsets: 2018-02-01T10:00:00 (default is to
        include UTC offsets: 2018-02-01T10:00:00-4000)

    Returns
    -------
    None

    Examples
    --------
    You can use StringIO to load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_history_file("my-db", f)
    >>> history = pd.read_csv(f, parse_dates=["Date"])

    See Also
    --------
    quantrocket.get_prices : load prices into a DataFrame
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
    if times:
        params["times"] = times
    if cont_fut:
        params["cont_fut"] = cont_fut
    if fields:
        params["fields"] = fields
    if tz_naive:
        params["tz_naive"] = tz_naive

    output = output or "csv"

    if output not in ("csv", "json", "txt"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/history/{0}.{1}".format(code, output), params=params,
                           timeout=60*30)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        if "no history matches the query parameters" in repr(e).lower():
            raise NoHistoricalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_history_file(*args, **kwargs):
    return json_to_cli(download_history_file, *args, **kwargs)

@deprecated_replaced_by("from quantrocket import get_prices")
def get_historical_prices(codes, start_date=None, end_date=None,
                          universes=None, conids=None,
                          exclude_universes=None, exclude_conids=None,
                          times=None, cont_fut=None, fields=None,
                          timezone=None, infer_timezone=None,
                          master_fields=None):
    """
    [DEPRECATED] This function is deprecated and will be removed in a future release.
    Please use `from quantrocket import get_prices` instead. `get_prices` is more flexible
    as it supports querying of both history databases and real-time aggregate databases.
    """
    from quantrocket import get_prices

    return get_prices(codes,
                      start_date=start_date, end_date=end_date,
                      universes=universes, conids=conids,
                      exclude_universes=exclude_universes,
                      exclude_conids=exclude_conids,
                      times=times, cont_fut=cont_fut,
                      fields=fields, timezone=timezone,
                      infer_timezone=infer_timezone,
                      master_fields=master_fields)

def load_history_from_file(code, infilepath_or_buffer):
    """
    Load market data from a CSV file into a history database.

    Parameters
    ----------
    code : str, required
        the database code to load into

    infilepath_or_buffer : str or file-like object
        CSV file containing market data (specify '-' to read file from stdin)

    Returns
    -------
    dict
        status message

    """
    url = "/history/{0}.csv".format(code)

    # Loading large amounts of data can take awhile
    timeout = 60*60

    if infilepath_or_buffer == "-":
        response = houston.patch(url, data=to_bytes(sys.stdin), timeout=timeout)

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.patch(url, data=to_bytes(infilepath_or_buffer), timeout=timeout)

    else:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.patch(url, data=f, timeout=timeout)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_load_history_from_file(*args, **kwargs):
    return json_to_cli(load_history_from_file, *args, **kwargs)

@deprecated_replaced_by(collect_history)
def fetch_history(*args, **kwargs):
    """
    Collect historical market data from IB and save it to a history database.

    [DEPRECATED] `fetch_history` is deprecated and will be removed
    in a future release, please use `collect_history` instead.
    """
    return collect_history(*args, **kwargs)

@deprecated_replaced_by("collect", old_name="fetch")
def _cli_fetch_history(*args, **kwargs):
    return json_to_cli(collect_history, *args, **kwargs)

@deprecated_replaced_by(cancel_collections)
def cancel_history_requests(*args, **kwargs):
    """
    Cancel running or pending historical data collections.

    [DEPRECATED] `cancel_history_requests` is deprecated and will be removed
    in a future release, please use `cancel_collections` instead.
    """
    return cancel_collections(*args, **kwargs)
