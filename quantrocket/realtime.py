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
import urllib.parse
import subprocess
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.houston import houston
from quantrocket.exceptions import NoRealtimeData, ParameterError
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs

def create_ibkr_tick_db(code, universes=None, sids=None, fields=None,
                        primary_exchange=False):
    """
    Create a new database for collecting real-time tick data from Interactive
    Brokers.

    The market data requirements you specify when you create a new database are
    applied each time you collect data for that database.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    universes : list of str
        include these universes

    sids : list of str
        include these sids

    fields : list of str
        collect these fields (pass '?' or any invalid fieldname to see
        available fields, default fields are 'LastPrice' and 'Volume')

    primary_exchange : bool
        limit to data from the primary exchange (default False)

    Returns
    -------
    dict
        status message

    Examples
    --------
    Create a database for collecting real-time trades and volume for US stocks:

    >>> create_ibkr_tick_db("usa-stk-trades", universes="usa-stk",
                            fields=["LastPrice", "Volume"])

    Create a database for collecting trades and quotes for a universe of futures:

    >>> create_ibkr_tick_db("globex-fut-taq", universes="globex-fut",
                            fields=["LastPrice", "Volume", "BidPrice", "AskPrice", "BidSize", "AskSize"])
    """
    params = {}
    if universes:
        params["universes"] = universes
    if sids:
        params["sids"] = sids
    if fields:
        params["fields"] = fields
    if primary_exchange:
        params["primary_exchange"] = primary_exchange

    params["vendor"] = "ibkr"

    response = houston.put("/realtime/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_ibkr_tick_db(*args, **kwargs):
    return json_to_cli(create_ibkr_tick_db, *args, **kwargs)

def create_polygon_tick_db(code, universes=None, sids=None, fields=None):
    """
    Create a new database for collecting real-time tick data from Polygon.

    The market data requirements you specify when you create a new database are
    applied each time you collect data for that database.

    Parameters
    ----------
    code : str, required
        the code to assign to the database (lowercase alphanumerics and hyphens only)

    universes : list of str
        include these universes

    sids : list of str
        include these sids

    fields : list of str
        collect these fields (pass '?' or any invalid fieldname to see
        available fields, default fields are 'LastPrice' and 'LastSize')

    Returns
    -------
    dict
        status message

    Examples
    --------
    Create a database for collecting real-time trade prices and sizes for US stocks:

    >>> create_polygon_tick_db("usa-stk-trades", universes="usa-stk", fields=["LastPrice", "LastSize"])

    """
    params = {}
    if universes:
        params["universes"] = universes
    if sids:
        params["sids"] = sids
    if fields:
        params["fields"] = fields

    params["vendor"] = "polygon"

    response = houston.put("/realtime/databases/{0}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_polygon_tick_db(*args, **kwargs):
    return json_to_cli(create_polygon_tick_db, *args, **kwargs)

def create_agg_db(code, tick_db_code, bar_size, fields=None):
    """
    Create an aggregate database from a tick database.

    Aggregate databases provide rolled-up views of the underlying tick data,
    aggregated to a desired frequency (such as 1-minute bars).

    Parameters
    ----------
    code : str, required
        the code to assign to the aggregate database (lowercase alphanumerics and hyphens only)

    tick_db_code : str, required
        the code of the tick database to aggregate

    bar_size : str, required
        the time frequency to aggregate to (use a Pandas timedelta string, for example
        10s or 1m or 2h or 1d)

    fields : dict of list of str, optional
        include these fields in aggregate database, aggregated in these ways. Provide a dict
        mapping tick db fields to lists of aggregate functions to apply to the field. Available
        aggregate functions are "Close", "Open", "High", "Low", "Mean", "Sum", and "Count".
        See examples section. If not specified, defaults to including the "Close" for each tick
        db field.

    Returns
    -------
    dict
        status message

    Examples
    --------
    Create an aggregate database of 1 minute bars consisting of OHLC trades and volume,
    from a tick database of US stocks, resulting in fields called LastPriceOpen, LastPriceHigh,
    LastPriceLow, LastPriceClose, and VolumeClose:

    >>> create_agg_db("usa-stk-trades-1min", tick_db_code="usa-stk-trades",
                      bar_size="1m",
                      fields={"LastPrice":["Open","High","Low","Close"],
                              "Volume": ["Close"]})

    Create an aggregate database of 1 second bars containing the closing bid and ask and
    the mean bid size and ask size, from a tick database of futures trades and
    quotes, resulting in fields called BidPriceClose, AskPriceClose, BidSizeMean, and AskSizeMean:

    >>> create_agg_db("globex-fut-taq-1sec", tick_db_code="globex-fut-taq",
                      bar_size="1s",
                      fields={"BidPrice":["Close"],
                              "AskPrice": ["Close"],
                              "BidSize": ["Mean"],
                              "AskSize": ["Mean"]
                              })
    """
    params = {}
    params["bar_size"] = bar_size
    if fields:
        if not isinstance(fields, dict):
            raise ParameterError("fields must be a dict")

        # convert lists to comma-separated strings
        _fields = {}
        for k, v in fields.items():
            if isinstance(v, (list, tuple)):
                v = ",".join(v)
            _fields[k] = v
        params["fields"] = dict_to_dict_strs(_fields)

    response = houston.put("/realtime/databases/{0}/aggregates/{1}".format(tick_db_code, code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_agg_db(*args, **kwargs):
    fields = kwargs.get("fields", None)
    if fields:
        kwargs["fields"] = dict_strs_to_dict(*fields)
    return json_to_cli(create_agg_db, *args, **kwargs)

def get_db_config(code):
    """
    Return the configuration for a tick database or aggregate database.

    Parameters
    ----------
    code : str, required
        the tick database code or aggregate database code

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

def drop_db(code, confirm_by_typing_db_code_again=None, cascade=False):
    """
    Delete a tick database or aggregate database.

    Deleting a tick database deletes its configuration and data and any
    associated aggregate databases. Deleting an aggregate database does not
    delete the tick database from which it is derived.

    Deleting databases is irreversible.

    Parameters
    ----------
    code : str, required
        the tick database code or aggregate database code

    confirm_by_typing_db_code_again : str, required
       enter the db code again to confirm you want to drop the database, its config,
       and all its data

    cascade : bool
       also delete associated aggregated databases, if any. Only applicable when
       deleting a tick database.

    Returns
    -------
    dict
        status message

    See Also
    --------
    drop_ticks : Delete ticks from a tick database.
    """
    params = {"confirm_by_typing_db_code_again": confirm_by_typing_db_code_again}
    if cascade:
        params["cascade"] = cascade
    response = houston.delete("/realtime/databases/{0}".format(code), params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_drop_db(*args, **kwargs):
    return json_to_cli(drop_db, *args, **kwargs)

def drop_ticks(code, older_than=None):
    """
    Delete ticks from a tick database. Does not delete any aggregate
    database records.

    Deleting ticks is a way to free up disk space by deleting ticks older
    than a certain threshold while maintaining the ability to continue
    collecting new ticks as well as use any aggregate databases derived from
    the ticks.

    Note: ticks are stored in the database in chunks, and this function only
    deletes chunks in which *all* of the ticks are older than you specify. If
    some of the ticks are older but some are newer, the chunk is not deleted.
    This means you may still see older data returned in queries.

    Parameters
    ----------
    code : str, required
        the tick database code

    older_than : str, required
       delete ticks older than this (use a Pandas timedelta string, for example
        7d)

    Returns
    -------
    dict
        status message

    See Also
    --------
    drop_db : Delete a tick database or aggregate database.

    Examples
    --------
    Delete ticks older than 7 days in a database called 'usa-tech-stk-tick' (no
    aggregate records are deleted):

    >>> drop_ticks("usa-tech-stk-tick", older_than="7d")
    """
    params = {"older_than": older_than}
    response = houston.delete("/realtime/ticks/{0}".format(code), params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_drop_ticks(*args, **kwargs):
    return json_to_cli(drop_ticks, *args, **kwargs)

def list_databases():
    """
    List tick databases and associated aggregate databases.

    Returns
    -------
    dict
        dict of {tick_db: [agg_dbs]}

    """
    response = houston.get("/realtime/databases")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_databases(*args, **kwargs):
    return json_to_cli(list_databases, *args, **kwargs)

def collect_market_data(codes, sids=None, universes=None, fields=None, until=None,
                        snapshot=False, wait=False):
    """
    Collect real-time market data and save it to a tick database.

    A single snapshot of market data or a continuous stream of market data can
    be collected, depending on the `snapshot` parameter. (Snapshots are not
    supported for all vendors.)

    Streaming real-time data is collected until cancelled, or can be scheduled
    for cancellation using the `until` parameter.

    Parameters
    ----------
    codes : list of str, required
        the tick database code(s) to collect data for

    sids : list of str, optional
        collect market data for these sids, overriding db config (typically
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
    Collect market data for all securities in a tick database called 'japan-banks-trades':

    >>> collect_market_data("japan-banks-trades")

    Collect market data for a subset of securities in a tick database called 'usa-stk-trades'
    and automatically cancel the data collection in 30 minutes:

    >>> collect_market_data("usa-stk-trades",
                            sids=["FIBBG12345", "FIBBG23456", "FIBBG34567"],
                            until="30m")

    Collect a market data snapshot and wait until it completes:

    >>> collect_market_data("usa-stk-trades", snapshot=True, wait=True)
    """
    params = {}
    if codes:
        params["codes"] = codes
    if sids:
        params["sids"] = sids
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
    response = houston.post("/realtime/collections", params=params, timeout=3600 if wait else 30)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_market_data(*args, **kwargs):
    return json_to_cli(collect_market_data, *args, **kwargs)

def get_active_collections(detail=False):
    """
    Return the number of tickers currently being collected, by vendor and
    database.

    Parameters
    ----------

    detail : bool
        return lists of tickers (default is to return counts of tickers)

    Returns
    -------
    dict
        subscribed tickers by vendor and database

    """
    params = {}
    if detail:
        params["detail"] = detail

    response = houston.get("/realtime/collections", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_active_collections(*args, **kwargs):
    return json_to_cli(get_active_collections, *args, **kwargs)

def cancel_market_data(codes=None, sids=None, universes=None, cancel_all=False):
    """
    Cancel market data collection.

    Parameters
    ----------
    codes : list of str, optional
        the tick database code(s) to cancel collection for

    sids : list of str, optional
        cancel market data for these sids, overriding db config

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
    Cancel market data collection for a tick database called 'globex-fut-taq':

    >>> cancel_market_data("globex-fut-taq")

    Cancel all market data collection:

    >>> cancel_market_data(cancel_all=True)
    """
    params = {}
    if codes:
        params["codes"] = codes
    if sids:
        params["sids"] = sids
    if universes:
        params["universes"] = universes
    if cancel_all:
        params["cancel_all"] = cancel_all

    response = houston.delete("/realtime/collections", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_cancel_market_data(*args, **kwargs):
    return json_to_cli(cancel_market_data, *args, **kwargs)

def download_market_data_file(code, filepath_or_buffer=None, output="csv",
                              start_date=None, end_date=None,
                              universes=None, sids=None,
                              exclude_universes=None, exclude_sids=None,
                              fields=None):
    """
    Query market data from a tick database or aggregate database and download to file.

    Parameters
    ----------
    code : str, required
        the code of the tick database or aggregate database to query

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

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

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

def _cli_stream_market_data(sids, exclude_sids, fields):

    url = houston.base_url + "/realtime/stream"

    params = {}

    if sids:
        params["sids"] = sids
    if exclude_sids:
        params["exclude_sids"] = exclude_sids
    if fields:
        params["fields"] = fields

    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)

    try:
        with subprocess.Popen(["wscat", "-c", url],
                              stdout=subprocess.PIPE,
                              bufsize=1,
                              universal_newlines=True) as p:
            for line in p.stdout:
                print(line, end='')
    except FileNotFoundError as e:
        if "wscat" in repr(e):
            return json_to_cli(lambda: {
                "status": "error",
                "msg": "wscat must be installed to stream data "
                "(install with `npm install -g wscat`)"})
        raise
    except KeyboardInterrupt:
        return None, 0
