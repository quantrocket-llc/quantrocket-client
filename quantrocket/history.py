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

import io
import os
import sys
import time
from quantrocket.master import download_master_file
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.stream import to_bytes
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.exceptions import ParameterError

TMP_DIR = os.environ.get("QUANTROCKET_TMP_DIR", "/tmp")

def create_db(code, universes=None, start_date=None, end_date=None,
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

    start_date : str (YYYY-MM-DD), optional
        fetch history back to this start date (default is to fetch as far back as data
        is available)

    end_date : str (YYYY-MM-DD), optional
        fetch history up to this end date (default is to fetch up to the present)

    vendor : str, optional
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
        whether and how to shard the database, i.e. break it into smaller pieces. Possible
        choices are `time` (separate database for each bar time), `off` (no sharding), or
        `auto` (decide automatically based on bar size and universe size). Default `auto`.

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

def fetch_history(codes, priority=False, conids=None, universes=None,
                  start_date=None, end_date=None, availability_only=False,
                  delist_missing=False):
    """
    Fetch historical market data from IB and save it to a history database. The request is
    queued and the data is fetched asynchronously.

    Parameters
    ----------
    codes : list of str, required
        the database code(s) to fetch data for

    priority : bool
        use the priority queue (default is to use the standard queue)

    conids : list of int, optional
        fetch history for these conids, overriding config (typically
        used to fetch a subset of securities)

    universes : list of str, optional
        fetch history for these universes, overriding config (typically
        used to fetch a subset of securities)

    start_date : str (YYYY-MM-DD), optional
        fetch history back to this start date, overriding config

    end_date : str (YYYY-MM-DD), optional
        fetch history up to this end date, overriding config

    availability_only : bool
        determine and store how far back data is available but
        don't yet fetch the data

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

def _cli_fetch_history(*args, **kwargs):
    return json_to_cli(fetch_history, *args, **kwargs)

def get_history_queue():
    """
    Get the current queue of historical data requests.

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

def cancel_history_requests(codes, queues=None):
    """
    Cancel running or pending historical data requests.

    Parameters
    ----------
    codes : list of str, required
        the database code(s) to cancel requests for

    queues : list of str, optional
        only cancel requests in these queues. Possible choices: standard, priority

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

def _cli_cancel_history_requests(*args, **kwargs):
    return json_to_cli(cancel_history_requests, *args, **kwargs)

def download_history_availability_file(code, filepath_or_buffer=None, output="csv"):
    """
    Query historical market data availability from a history database and download to file.

    This function is normally called after running:

        quantrocket history fetch mydb --availability

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
    data can be fetched from IB.

    This function is normally called after running a command such as:

        quantrocket history fetch [DB] --availability

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

    f = io.StringIO()
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
    get_historical_prices : load historical prices into DataFrame
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

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_history_file(*args, **kwargs):
    return json_to_cli(download_history_file, *args, **kwargs)

def _infer_timezone(prices):
    """
    Infers the timezone from the component securities if possible.
    """
    if "Timezone" not in prices.index.get_level_values("Field"):
        raise ParameterError(
            "cannot infer timezone because Timezone field is missing, "
            "please specify timezone or include Timezone in master_fields")

    timezones = prices.loc["Timezone"].stack().unique()

    if len(timezones) > 1:
        raise ParameterError(
            "cannot infer timezone because multiple timezones are present "
            "in data, please specify timezone explicitly (timezones: {0})".format(
                ", ".join(timezones)))

    return timezones[0]

def get_historical_prices(codes, start_date=None, end_date=None,
                          universes=None, conids=None,
                          exclude_universes=None, exclude_conids=None,
                          times=None, cont_fut=None, fields=None,
                          master_fields=None, timezone=None,
                          infer_timezone=None):
    """
    Query one or more history databases and load prices into a DataFrame.

    For bar sizes smaller than 1-day, the resulting DataFrame will have a MultiIndex
    with levels (Field, Date, Time). For bar sizes of 1-day or larger, the MultiIndex
    will have levels (Field, Date).

    Parameters
    ----------
    codes : str or list of str, required
        the code(s) of one or more databases to query. If multiple databases
        are specified, they must have the same bar size.

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

    master_fields : list of str, optional
        append these fields from the securities master database (pass ['?'] or any
        invalid fieldname to see available fields)

    timezone : str, optional
        convert timestamps to this timezone, for example America/New_York (see
        `pytz.all_timezones` for choices); ignored for non-intraday bar sizes

    infer_timezone : bool
        infer the timezone from the securities master Timezone field; defaults to
        True if using intraday bars and no `timezone` specified; ignored for
        non-intraday bars, or if `timezone` is passed

    Returns
    -------
    DataFrame
        a MultiIndex

    Examples
    --------
    Load intraday prices:

    >>> prices = get_historical_prices('stk-sample-5min', fields=["Close", "Volume"], timezone="America/New_York")
    >>> prices.head()
                                ConId   	265598	38708077
    Field	Date	        Time
    Close	2017-07-26      09:30:00	153.62	2715.0
                                09:35:00	153.46	2730.0
                                09:40:00	153.21	2725.0
                                09:45:00	153.28	2725.0
                                09:50:00	153.18	2725.0

    Isolate the closes:

    >>> closes = prices.loc["Close"]
    >>> closes.head()
                ConId	        265598  38708077
    Date        Time
    2017-07-26	09:30:00	153.62	2715.0
                09:35:00	153.46	2730.0
                09:40:00	153.21	2725.0
                09:45:00	153.28	2725.0
                09:50:00	153.18	2725.0

    Isolate the 15:45:00 prices:

    >>> session_closes = closes.xs("15:45:00", level="Time")
    >>> session_closes.head()
        ConId	265598	38708077
    Date
    2017-07-26	153.29	2700.00
    2017-07-27 	150.10	2660.00
    2017-07-28	149.43	2650.02
    2017-07-31 	148.99	2650.34
    2017-08-01 	149.72	2675.50
    """
    # Import pandas lazily since it can take a moment to import
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    try:
        import pytz
    except ImportError:
        raise ImportError("pytz must be installed to use this function")

    if timezone and timezone not in pytz.all_timezones:
        raise ParameterError(
            "invalid timezone: {0} (see `pytz.all_timezones` for choices)".format(
                timezone))

    dbs = codes
    if not isinstance(dbs, (list, tuple)):
        dbs = [dbs]

    if master_fields:
        if isinstance(master_fields, tuple):
            master_fields = list(master_fields)
        elif not isinstance(master_fields, list):
            master_fields = [master_fields]

    db_universes = set()
    db_bar_sizes = set()
    for db in dbs:
        db_config = get_db_config(db)
        _db_universes = db_config.get("universes", None)
        if _db_universes:
            db_universes.update(set(_db_universes))
        bar_size = db_config.get("bar_size")
        db_bar_sizes.add(bar_size)

    db_universes = list(db_universes)
    db_bar_sizes = list(db_bar_sizes)

    if len(db_bar_sizes) > 1:
        raise ParameterError(
            "all databases must contain same bar size but {0} have different "
            "bar sizes: {1}".format(", ".join(dbs), ", ".join(db_bar_sizes))
        )

    all_prices = []

    for db in dbs:

        kwargs = dict(
            start_date=start_date,
            end_date=end_date,
            universes=universes,
            conids=conids,
            exclude_universes=exclude_universes,
            exclude_conids=exclude_conids,
            times=times,
            cont_fut=cont_fut,
            fields=fields,
            tz_naive=False
        )

        tmp_filepath = "{0}/history.{1}.{2}.{3}.csv".format(
            TMP_DIR, db, os.getpid(), time.time()
        )
        download_history_file(db, tmp_filepath, **kwargs)

        prices = pd.read_csv(tmp_filepath)
        all_prices.append(prices)

        os.remove(tmp_filepath)

    prices = pd.concat(all_prices)

    prices = prices.pivot(index="ConId", columns="Date").T
    prices.index.set_names(["Field", "Date"], inplace=True)


    is_intraday = db_bar_sizes[0] not in ("1 day", "1 week", "1 month")

    if is_intraday and not timezone and infer_timezone is not False:
        infer_timezone = True
        if not master_fields:
            master_fields = []
        if "Timezone" not in master_fields:
            master_fields.append("Timezone")

    # Next, get the master file
    if master_fields:
        universes = universes
        conids = conids
        if not conids and not universes:
            universes = db_universes
            if not universes:
                conids = list(prices.columns)

        f = io.StringIO()
        download_master_file(
            f,
            conids=conids,
            universes=universes,
            exclude_conids=exclude_conids,
            exclude_universes=exclude_universes,
            fields=master_fields,
            delisted=True
        )
        securities = pd.read_csv(f, index_col="ConId")

        if "Delisted" in securities.columns:
            securities.loc[:, "Delisted"] = securities.Delisted.astype(bool)

        if "Etf" in securities.columns:
            securities.loc[:, "Etf"] = securities.Etf.astype(bool)

        # Append securities, indexed to the min date, to allow easy ffill on demand
        securities = pd.DataFrame(securities.T, columns=prices.columns)
        securities.index.name = "Field"
        idx = pd.MultiIndex.from_product(
            (securities.index, [prices.index.get_level_values("Date").min()]),
            names=["Field", "Date"])

        securities = securities.reindex(index=idx, level="Field")
        prices = pd.concat((prices, securities))

    if is_intraday:
        dates = pd.to_datetime(prices.index.get_level_values("Date"), utc=True)

        if not timezone and infer_timezone:
            timezone = _infer_timezone(prices)

        if timezone:
            dates = dates.tz_convert(timezone)
    else:
        dates = pd.to_datetime(prices.index.get_level_values("Date"))

    prices.index = pd.MultiIndex.from_arrays((
        prices.index.get_level_values("Field"),
        dates
        ), names=("Field", "Date"))

    # Split date and time
    dts = prices.index.get_level_values("Date")
    dates = pd.to_datetime(dts.date).tz_localize(None) # drop tz-aware in Date index
    prices.index = pd.MultiIndex.from_arrays(
        (prices.index.get_level_values("Field"),
         dates,
         dts.strftime("%H:%M:%S")),
        names=["Field", "Date", "Time"]
    )

    # Align dates if there are any duplicate. Explanation: Suppose there are
    # two timezones represented in the data. After parsing these dates into a
    # common timezone, they will align properly, but we pivoted before
    # parsing the dates (for performance reasons), so they may not be
    # aligned. Thus we need to dedupe the index.
    prices = prices.groupby(prices.index).first()
    prices.index = pd.MultiIndex.from_tuples(prices.index)
    prices.index.set_names(["Field", "Date", "Time"], inplace=True)

    # Drop time if not intraday
    if not is_intraday:
        prices.index = prices.index.droplevel("Time")
        return prices

    # If intraday, fill missing times so that each date has the same set of
    # times, allowing easier comparisons. Example implications:
    # - if history is retrieved intraday, this ensures that today will have NaN
    #   entries for future times
    # - early close dates will have a full set of times, with NaNs after the
    #   early close
    unique_fields = prices.index.get_level_values("Field").unique()
    unique_dates = prices.index.get_level_values("Date").unique()
    unique_times = prices.index.get_level_values("Time").unique()
    interpolated_index = None
    for field in unique_fields:
        if master_fields and field in master_fields:
            min_date = prices.loc[field].index.min()
            field_idx = pd.MultiIndex.from_tuples([(field,min_date[0], min_date[1])])
        else:
            field_idx = pd.MultiIndex.from_product([[field], unique_dates, unique_times])
        if interpolated_index is None:
            interpolated_index = field_idx
        else:
            interpolated_index = interpolated_index.append(field_idx)

    prices = prices.reindex(interpolated_index)
    prices.index.set_names(["Field", "Date", "Time"], inplace=True)

    return prices

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