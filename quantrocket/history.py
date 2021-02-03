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
from quantrocket.cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs
from quantrocket.exceptions import NoHistoricalData

TMP_DIR = os.environ.get("QUANTROCKET_TMP_DIR", "/tmp")

def create_edi_db(code, exchanges):
    """
    Create a new database for collecting historical data from EDI.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    exchanges : list of str, required
        one or more exchange codes (MICs) which should be collected

    Returns
    -------
    dict
        status message

    """
    params = {
        "vendor": "edi",
        "exchanges": exchanges
    }
    response = houston.put("/history/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_edi_db(*args, **kwargs):
    return json_to_cli(create_edi_db, *args, **kwargs)

def create_ibkr_db(code, universes=None, sids=None, start_date=None, end_date=None,
                   bar_size=None, bar_type=None, outside_rth=False,
                   primary_exchange=False, times=None, between_times=None,
                   shard=None):
    """
    Create a new database for collecting historical data from Interactive Brokers.

    The historical data requirements you specify when you create a new database (bar
    size, universes, etc.) are applied each time you collect data for that database.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    universes : list of str
        include these universes

    sids : list of str
        include these sids

    start_date : str (YYYY-MM-DD), optional
        collect history back to this start date (default is to collect as far back as data
        is available)

    end_date : str (YYYY-MM-DD), optional
        collect history up to this end date (default is to collect up to the present)

    bar_size : str, required
        the bar size to collect. Possible choices:
        "1 secs", "5 secs",	"10 secs", "15 secs", "30 secs",
        "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins",
        "1 hour", "2 hours", "3 hours", "4 hours", "8 hours",
        "1 day",
        "1 week",
        "1 month"

    bar_type : str, optional
        the bar type to collect (if not specified, defaults to MIDPOINT for FX and
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
        database for each year), `month` (separate database for each year+month),
        `day` (separate database for each day), `time` (separate database for each
        bar time), `sid` (separate database for each security), `sid,time`
        (duplicate copies of database, one sharded by sid and the other by time),
        or `off` (no sharding). See http://qrok.it/h/shard for more help.

    Returns
    -------
    dict
        status message

    """
    params = {}
    if universes:
        params["universes"] = universes
    if sids:
        params["sids"] = sids
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
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

    params["vendor"] = "ibkr"

    response = houston.put("/history/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_ibkr_db(*args, **kwargs):
    return json_to_cli(create_ibkr_db, *args, **kwargs)

def create_sharadar_db(code, sec_type, country="US"):
    """
    Create a new database for collecting historical data from Sharadar.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    sec_type : str, required
        the security type to collect. Possible choices: STK, ETF

    country : str, required
        country to collect data for. Possible choices: US, FREE

    Returns
    -------
    dict
        status message

    """
    params = {"vendor": "sharadar"}
    if sec_type:
        params["sec_type"] = sec_type
    if country:
        params["country"] = country

    response = houston.put("/history/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_sharadar_db(*args, **kwargs):
    return json_to_cli(create_sharadar_db, *args, **kwargs)

def create_usstock_db(code, bar_size=None, free=False, universe=None):
    """
    Create a new database for collecting historical US stock data from QuantRocket.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    bar_size : str, optional
        the bar size to collect. Possible choices: 1 day

    free : bool
        limit to free sample data. Default is to collect the full dataset.

    universe : str, optional
        [DEPRECATED] whether to collect free sample data or the full dataset. This
        parameter is deprecated and will be removed in a future release. Please use
        free=True to request free sample data or free=False (or omit the free parameter)
        to request the full dataset.

    Returns
    -------
    dict
        status message

    Examples
    --------
    Create a database for end-of-day US stock prices:

    create_usstock_db('usstock-1d')
    """
    params = {
        "vendor": "usstock",
    }
    if bar_size:
        params["bar_size"] = bar_size
    if free:
        params["free"] = free
    if universe:
        import warnings
        # DeprecationWarning is ignored by default but we want the user
        # to see it
        warnings.simplefilter("always", DeprecationWarning)
        warnings.warn(
            "the `universe` parameter is deprecated and will be removed in a "
            "future release, please use `free=True` to request free sample data "
            "or free=False (or omit the free parameter) to request the full dataset",
            DeprecationWarning)
        params["universe"] = universe

    response = houston.put("/history/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_usstock_db(*args, **kwargs):
    return json_to_cli(create_usstock_db, *args, **kwargs)

def create_custom_db(code, bar_size=None, columns=None):
    """
    Create a new database into which custom data can be loaded.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    bar_size : str, required
        the bar size that will be loaded. This isn't enforced but facilitates efficient
        querying and provides a hint to other parts of the API. Use a Pandas timedelta
        string, for example, '1 day' or '1 min' or '1 sec'.

    columns : dict of column name:type, required
        the columns to create, specified as a Python dictionary mapping column names to
        column types. For example, {"Close":"float", "Volume":"int"}. Valid column
        types are "int", "float", "text", "date", and "datetime". Column names must
        start with a letter and include only letters, numbers, and underscores.
        Sid and Date columns are automatically created and need not be specified.
        For boolean columns, choose type 'int' and store 1 or 0.

    Returns
    -------
    dict
        status message

    Examples
    --------
    Create a custom database for loading fundamental data:

    >>> create_custom_db(
            "custom-fundamentals",
            bar_size="1 day",
            columns={
                "Revenue":"int",
                "EPS":"float",
                "Currency":"str",
                "TotalAssets":"int"})

    Create a custom database for loading intraday OHCLV data:

    >>> create_custom_db(
            "custom-stk-1sec",
            bar_size="1 sec",
            columns={
                "Open":"float",
                "High":"float",
                "Low":"float",
                "Close":"float",
                "Volume":"int"})
    """
    params = {}
    if bar_size:
        params["bar_size"] = bar_size
    if columns:
        params["columns"] = dict_to_dict_strs(columns)

    params["vendor"] = "custom"

    response = houston.put("/history/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_custom_db(*args, **kwargs):
    columns = kwargs.get("columns", None)
    if columns:
        kwargs["columns"] = dict_strs_to_dict(*columns)
    return json_to_cli(create_custom_db, *args, **kwargs)

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

    Deleting a history database deletes its configuration and data and is
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

def collect_history(codes, sids=None, universes=None, start_date=None, end_date=None,
                    priority=False):
    """
    Collect historical market data from a vendor and save it to a history database.

    The vendor and collection parameters are determined by the stored database
    configuration as defined at the time the database was created. For certain
    vendors, collection parameters can be overridden at the time of data collection.

    Parameters
    ----------
    codes : list of str, required
        the database code(s) to collect data for

    sids : list of str, optional
        collect history for these sids, overriding config (typically
        used to collect a subset of securities). Only supported for IBKR
        databases.

    universes : list of str, optional
        collect history for these universes, overriding config (typically
        used to collect a subset of securities). Only supported for IBKR
        databases.

    start_date : str (YYYY-MM-DD), optional
        collect history back to this start date, overriding config. Only
        supported for IBKR databases.

    end_date : str (YYYY-MM-DD), optional
        collect history up to this end date, overriding config. Only supported
        for IBKR databases.

    priority : bool
        use the priority queue (default is to use the standard queue). Only
        applicable to IBKR databases.

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
    if sids:
        params["sids"] = sids
    if universes:
        params["universes"] = universes
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
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
        queue by vendor

    """
    response = houston.get("/history/queue")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_history_queue(*args, **kwargs):
    return json_to_cli(get_history_queue, *args, **kwargs)

def cancel_collections(codes):
    """
    Cancel running or pending historical data collections.

    Parameters
    ----------
    codes : list of str, required
        the database code(s) to cancel collections for

    Returns
    -------
    dict
        queue by vendor

    """
    params = {}
    if codes:
        params["codes"] = codes
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

def download_history_file(code, filepath_or_buffer=None, output="csv",
                          start_date=None, end_date=None,
                          universes=None, sids=None,
                          exclude_universes=None, exclude_sids=None,
                          times=None, cont_fut=None, fields=None):
    """
    Query historical market data from a history database and download to file.

    Parameters
    ----------
    code : str, required
        the code of the database to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to history on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to history on or before this date

    universes : list of str, optional
        limit to these universes (default is to return all securities in database)

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    times: list of str (HH:MM:SS), optional
        limit to these times

    cont_fut : str
        stitch futures into continuous contracts using this method (default is not
        to stitch together). Possible choices: concat

    fields : list of str, optional
        only return these fields (pass ['?'] or any invalid fieldname to see
        available fields)

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
    if sids:
        params["sids"] = sids
    if exclude_universes:
        params["exclude_universes"] = exclude_universes
    if exclude_sids:
        params["exclude_sids"] = exclude_sids
    if times:
        params["times"] = times
    if cont_fut:
        params["cont_fut"] = cont_fut
    if fields:
        params["fields"] = fields

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
