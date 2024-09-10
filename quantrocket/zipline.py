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
Functions for backtesting and trading with Zipline, and managing Zipline
bundles.

Functions
---------
create_usstock_bundle
    Create a Zipline bundle for US stocks.

create_sharadar_bundle
    Create a Zipline bundle of daily data for Sharadar stocks and/or ETFs.

create_bundle_from_db
    Create a Zipline bundle from a history database or real-time aggregate
    database.

ingest_bundle
    Ingest data into a previously defined bundle.

list_bundles
    List available data bundles and whether data has been
    ingested into them.

get_bundle_config
    Return the configuration of a bundle.

drop_bundle
    Delete a bundle.

get_default_bundle
    Return the current default bundle, if any.

set_default_bundle
    Set the default bundle to use for backtesting and trading.

list_sids
    List the sids in a bundle.

download_bundle_file
    Query minute or daily data from a Zipline bundle and download to a CSV file.

backtest
    Backtest a Zipline strategy and write the test results to a CSV file.

scan_parameters
    Run a parameter scan for a Zipline strategy.

trade
    Trade a Zipline strategy.

list_active_strategies
    List actively trading Zipline strategies.

cancel_strategies
    Cancel actively trading strategies.

Classes
-------
ZiplineBacktestResult
    Convenience class for parsing a CSV result file from a Zipline backtest
    into a variety of useful DataFrames, which can be passed to pyfolio or
    inspected by the user.

Notes
-----
Usage Guide:

* Zipline: https://qrok.it/dl/qr/zipline
"""
import sys
import six
import requests
from typing import TYPE_CHECKING, Union, Literal, Any
if TYPE_CHECKING:
    import pandas as pd
from quantrocket.utils._typing import FilepathOrBuffer
from quantrocket.houston import houston
from quantrocket.exceptions import NoHistoricalData
from quantrocket._cli.utils.output import json_to_cli
from quantrocket._cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket._cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs
from quantrocket.utils._warn import deprecated_replaced_by

__all__ = [
    "create_usstock_bundle",
    "create_sharadar_bundle",
    "create_bundle_from_db",
    "ingest_bundle",
    "list_bundles",
    "get_bundle_config",
    "drop_bundle",
    "get_default_bundle",
    "set_default_bundle",
    "list_sids",
    "download_bundle_file",
    "backtest",
    "scan_parameters",
    "trade",
    "list_active_strategies",
    "cancel_strategies",
    "ZiplineBacktestResult",
]

def create_usstock_bundle(
    code: str,
    sids: Union[list[str], str] = None,
    universes: Union[list[str], str] = None,
    free: bool = False,
    learn: bool = False,
    data_frequency: Literal["daily", "minute", "d", "m"] = None
    ) -> dict[str, str]:
    """
    Create a Zipline bundle for US stocks.

    This function defines the bundle parameters but does not ingest the actual
    data. To ingest the data, see `ingest_bundle`.

    Parameters
    ----------
    code : str, required
        the code to assign to the bundle (lowercase alphanumerics and hyphens only)

    sids : list of str, optional
        limit to these sids (only supported for minute data bundles)

    universes : list of str, optional
        limit to these universes (only supported for minute data bundles)

    free : bool
        limit to free sample data (complete minute data history for a small number
        of stocks)

    learn : bool
        create the learning data bundle (daily history for all stocks from 2007-2011)

    data_frequency : str, optional
         whether to collect minute data (which also includes daily data) or only
         daily data. Default is minute data. Possible choices: daily, minute (or
         aliases d, m)

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * US Stock bundle: https://qrok.it/dl/qr/zipline-usstock

    Examples
    --------
    Create a minute data bundle for all US stocks:

    >>> create_usstock_bundle("usstock-1min")

    Create a bundle for daily data only:

    >>> create_usstock_bundle("usstock-1d", data_frequency="daily")

    Create a minute data bundle based on a universe:

    >>> create_usstock_bundle("usstock-tech-1min", universes="us-tech")

    Create a minute data bundle of free sample data (full minute history for a small number of stocks):

    >>> create_usstock_bundle("usstock-free-1min", free=True)

    Create the learning bundle (daily history for all stocks from 2007-2011):

    >>> create_usstock_bundle("usstock-learn-1d", learn=True)
    """
    params = {}
    params["ingest_type"] = "usstock"
    if sids:
        params["sids"] = sids
    if universes:
        params["universes"] = universes
    if free:
        params["free"] = free
    if learn:
        params["learn"] = learn
    if data_frequency:
        params["data_frequency"] = data_frequency

    response = houston.put("/zipline/bundles/{}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_usstock_bundle(*args, **kwargs):
    return json_to_cli(create_usstock_bundle, *args, **kwargs)

def create_sharadar_bundle(
    code: str,
    sec_types: Union[list[
        Literal["STK", "ETF"]],
        str] = None,
    free: bool = False
    ) -> dict[str, str]:
    """
    Create a Zipline bundle of daily data for Sharadar stocks and/or ETFs.

    This function defines the bundle parameters but does not ingest the actual
    data. To ingest the data, see `ingest_bundle`.

    Parameters
    ----------
    code : str, required
        the code to assign to the bundle (lowercase alphanumerics and hyphens only)

    sec_types : list of str, optional
        limit to these security types. Possible choices: STK, ETF. Default is to
        include both stocks and ETFs.

    free : bool
        limit to free sample data

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Sharadar bundle: https://qrok.it/dl/qr/zipline-sharadar

    Examples
    --------
    Create a bundle for all Sharadar stocks and ETFs:

    >>> create_sharadar_bundle("sharadar-1d")

    Create a bundle for ETFs only:

    >>> create_sharadar_bundle("sharadar-etf-1d", sec_types="ETF")

    Create a bundle of free sample data:

    >>> create_sharadar_bundle("sharadar-free-1d", free=True)
    """
    params = {}
    params["ingest_type"] = "sharadar"
    if sec_types:
        params["sec_types"] = sec_types
    if free:
        params["free"] = free

    response = houston.put("/zipline/bundles/{}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_sharadar_bundle(*args, **kwargs):
    return json_to_cli(create_sharadar_bundle, *args, **kwargs)

def create_bundle_from_db(
    code: str,
    from_db: Union[str, list[str]],
    calendar: str,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    fields: dict[str, str] = None
    ) -> dict[str, str]:
    """
    Create a Zipline bundle from a history database or real-time aggregate
    database.

    You can ingest 1-minute or 1-day databases.

    This function defines the bundle parameters but does not ingest the actual
    data. To ingest the data, see `ingest_bundle`.

    Parameters
    ----------
    code : str, required
        the code to assign to the bundle (lowercase alphanumerics and hyphens only)

    from_db : str or list of str, required
        the code(s) of one or more history databases or real-time aggregate databases
        to ingest. If multiple databases are specified, they must have the same bar
        size and the same fields. If a security is present in multiple databases, the
        first database's values will be used.

    calendar : str, required
        the name of the calendar to use with this bundle (provide '?' or
        any invalid calendar name to see available choices)

    start_date : str (YYYY-MM-DD), required
        limit to historical data on or after this date. This parameter is required
        and also determines the default start date for backtests and queries.

    end_date : str (YYYY-MM-DD), optional
        limit to historical data on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    fields : dict, optional
        mapping of Zipline fields (open, high, low, close, volume) to
        db fields. Defaults to mapping Zipline 'open' to db 'Open', etc.

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * History db bundle: https://qrok.it/dl/qr/zipline-fromdb

    Examples
    --------
    Create a bundle from a history database called "es-fut-1min" and name
    it like the history database:

    >>> create_bundle_from_db("es-fut-1min", from_db="es-fut-1min", calendar="us_futures")

    Create a bundle named "usa-stk-1min-2017" for ingesting a single year of
    US 1-minute stock data from a history database called "usa-stk-1min":

    >>> create_bundle_from_db("usa-stk-1min-2017", from_db="usa-stk-1min",
    >>>                      calendar="XNYS",
    >>>                      start_date="2017-01-01", end_date="2017-12-31")

    Create a bundle from a real-time aggregate database and specify how to map
    Zipline fields to the database fields:

    >>> create_bundle_from_db("free-stk-1min", from_db="free-stk-tick-1min",
    >>>                       calendar="XNYS", start_date="2020-06-01",
    >>>                       fields={
    >>>                           "close": "LastPriceClose",
    >>>                           "open": "LastPriceOpen",
    >>>                           "high": "LastPriceHigh",
    >>>                           "low": "LastPriceLow",
    >>>                           "volume": "VolumeClose"})
    """
    params = {}
    params["ingest_type"] = "from_db"
    params["from_db"] = from_db
    params["calendar"] = calendar
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
        params["fields"] = dict_to_dict_strs(fields)

    response = houston.put("/zipline/bundles/{}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_bundle_from_db(*args, **kwargs):
    fields = kwargs.get("fields", None)
    if fields:
        kwargs["fields"] = dict_strs_to_dict(*fields)
    return json_to_cli(create_bundle_from_db, *args, **kwargs)

def ingest_bundle(
    code: str,
    sids: Union[list[str], str] = None,
    universes: Union[list[str], str] = None
    ) -> dict[str, str]:
    """
    Ingest data into a previously defined bundle.

    Parameters
    ----------
    code : str, required
        the bundle code

    sids : list of str, optional
        limit to these sids, overriding stored config

    universes : list of str, optional
        limit to these universes, overriding stored config

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Data bundles: https://qrok.it/dl/qr/zipline-bundles

    Examples
    --------
    Ingest data into a bundle called usstock-1min:

    >>> ingest_bundle("usstock-1min")
    """
    params = {}
    if sids:
        params["sids"] = sids
    if universes:
        params["universes"] = universes

    response = houston.post("/zipline/ingestions/{}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_ingest_bundle(*args, **kwargs):
    return json_to_cli(ingest_bundle, *args, **kwargs)

def list_bundles() -> dict[str, bool]:
    """
    List available data bundles and whether data has been
    ingested into them.

    Returns
    -------
    dict
        data bundles and whether they have data (True indicates
        data, False indicates config only)

    Notes
    -----
    Usage Guide:

    * Data bundles: https://qrok.it/dl/qr/zipline-bundles
    """
    response = houston.get("/zipline/bundles")

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_bundles(*args, **kwargs):
    return json_to_cli(list_bundles, *args, **kwargs)

def get_bundle_config(code: str) -> dict[str, str]:
    """
    Return the configuration of a bundle.

    Parameters
    ----------
    code : str, required
        the bundle code

    Returns
    -------
    dict
        config

    Notes
    -----
    Usage Guide:

    * Data bundles: https://qrok.it/dl/qr/zipline-bundles
    """
    response = houston.get(f"/zipline/bundles/config/{code}")

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_bundle_config(*args, **kwargs):
    return json_to_cli(get_bundle_config, *args, **kwargs)

def drop_bundle(
    code: str,
    confirm_by_typing_bundle_code_again: str = None
    ) -> dict[str, str]:
    """
    Delete a bundle.

    Parameters
    ----------
    code : str, required
        the bundle code

    confirm_by_typing_bundle_code_again : str, required
       enter the bundle code again to confirm you want to drop the bundle, its config,
       and all its data

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Data bundles: https://qrok.it/dl/qr/zipline-bundles

    Examples
    --------
    Delete a bundle called 'es-fut-1min':

    >>> drop_bundle("es-fut-1min")
    """
    params = {}
    if confirm_by_typing_bundle_code_again:
        params["confirm_by_typing_bundle_code_again"] = confirm_by_typing_bundle_code_again

    response = houston.delete("/zipline/bundles/{}".format(code), params=params, timeout=6*60*60)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_drop_bundle(*args, **kwargs):
    return json_to_cli(drop_bundle, *args, **kwargs)

def get_default_bundle() -> dict[str, str]:
    """
    Return the current default bundle, if any.

    Returns
    -------
    dict
        default bundle

    Notes
    -----
    Usage Guide:

    * Data bundles: https://qrok.it/dl/qr/zipline-bundles
    """
    response = houston.get("/zipline/config")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_default_bundle(bundle: str) -> dict[str, str]:
    """
    Set the default bundle to use for backtesting and trading.

    Setting a default bundle is a convenience and is optional. It
    can be overridden by manually specifying a bundle when backtesting
    or trading.

    Parameters
    ----------
    bundle : str, required
        the bundle code

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Data bundles: https://qrok.it/dl/qr/zipline-bundles
    """
    data = {
        "default_bundle": bundle
    }
    response = houston.put("/zipline/config", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_default_bundle(bundle=None, *args, **kwargs):
    if bundle:
        return json_to_cli(set_default_bundle, bundle, *args, **kwargs)
    else:
        return json_to_cli(get_default_bundle, *args, **kwargs)

def list_sids(code: str) -> list[str]:
    """
    List the sids in a bundle.

    Parameters
    ----------
    code : str, required
        the bundle code

    Returns
    -------
    list of str
        sids

    Examples
    --------
    List the sids in a bundle called 'usstock-1min':

    >>> list_sids("usstock-1min")
    """
    response = houston.get("/zipline/bundles/{0}/sids".format(code))

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_sids(*args, **kwargs):
    return json_to_cli(list_sids, *args, **kwargs)

def download_bundle_file(
    code: str,
    filepath_or_buffer: FilepathOrBuffer = None,
    start_date: str = None,
    end_date: str = None,
    data_frequency: Literal["daily", "minute", "d", "m"] = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    times: Union[list[str], str] = None,
    fields: Union[list[str], str] = None
    ) -> None:
    """
    Query minute or daily data from a Zipline bundle and download to a CSV file.

    Parameters
    ----------
    code : str, required
        the bundle code

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    start_date : str (YYYY-MM-DD), optional
        limit to history on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to history on or before this date

    data_frequency : str, optional
        whether to query minute or daily data. If omitted, defaults to minute data
        for minute bundles and to daily data for daily bundles. This parameter
        only needs to be set to request daily data from a minute bundle. Possible
        choices: daily, minute (or aliases d, m).

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    times: list of str (HH:MM:SS), optional
        limit to these times

    fields : list of str, optional
        only return these fields (pass ['?'] or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    See Also
    --------
    quantrocket.get_prices : load prices into a DataFrame

    Notes
    -----
    Usage Guide:

    * Query bundle: https://qrok.it/dl/qr/zipline-query-bundle
    * get_prices: https://qrok.it/dl/qr/prices

    Examples
    --------
    Load minute prices into pandas:

    >>> download_bundle_file("usstock-1min", sids=["FIBBG12345"])
    >>> prices = pd.read_csv(f, parse_dates=["Date"], index_col=["Field","Date"])

    Isolate fields with .loc:

    >>> closes = prices.loc["Close"]
    """
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if data_frequency:
        params["data_frequency"] = data_frequency
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
    if fields:
        params["fields"] = fields

    response = houston.get("/zipline/bundles/data/{0}.csv".format(code), params=params,
                           timeout=60*30)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        # This endpoint returns "no minute|daily data match the query parameters"
        no_data_messages = (
            "data match the query parameters",
            "no free securities match",
            "no data has been ingested",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoHistoricalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_bundle_file(*args, **kwargs):
    return json_to_cli(download_bundle_file, *args, **kwargs)

@deprecated_replaced_by(download_bundle_file)
def download_minute_file(*args, **kwargs):
    kwargs["data_frequency"] = "minute"
    return download_bundle_file(*args, **kwargs)

def backtest(
    strategy: str,
    data_frequency: Literal["daily", "minute", "d", "m"] = None,
    capital_base: float = None,
    bundle: str = None,
    start_date: str = None,
    end_date: str = None,
    progress: str = None,
    params: dict[str, Any] = None,
    filepath_or_buffer: FilepathOrBuffer = None
    ) -> None:
    """
    Backtest a Zipline strategy and write the test results to a CSV file.

    The CSV result file contains several DataFrames stacked into one: the Zipline performance
    results, plus the extracted returns, transactions, positions, and benchmark returns from those
    results.

    Parameters
    ----------
    strategy : str, required
        the strategy to run (strategy filename without extension)

    data_frequency : str, optional
        the data frequency of the simulation. Possible choices: daily, minute
        (or aliases d, m). Defaults to minute for minute bundles and daily
        for daily bundles. Only needs to set to request daily data from a
        minute bundle.

    capital_base : float, optional
        the starting capital for the simulation (default is 1e6 (1 million))

    bundle : str, optional
        the data bundle to use for the simulation. Can be omitted if a BUNDLE variable
        is defined in the algorithm file. If omitted and a BUNDLE variable is not
        defined, the default bundle (if set) is used.

    start_date : str (YYYY-MM-DD), optional
        the start date of the simulation (defaults to the bundle start date)

    end_date : str (YYYY-MM-DD), optional
        the end date of the simulation (defaults to today)

    progress : str, optional
        log backtest progress at this interval (use a pandas offset alias,
        for example "D" for daily, "W" for weeky, "M" for monthly,
        "A" for annually)

    params : dict of PARAM:VALUE, optional
        one or more strategy parameters (defined as module-level attributes
        in the algo file) to modify on the fly before backtesting (pass as
        {param:value}).

    filepath_or_buffer : str or file-like object, optional
        the location to write the output file (omit to write to stdout)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Zipline backtesting: https://qrok.it/dl/qr/zipline-backtest

    Examples
    --------
    Run a backtest defined in momentum-pipeline.py and save to CSV,
    logging backtest progress at weekly intervals.

    >>> backtest("momentum-pipeline", bundle="my-bundle",
                 start_date="2015-02-04", end_date="2015-12-31",
                 progress="W",
                 filepath_or_buffer="momentum_pipeline_results.csv")

    Get a pyfolio tear sheet from the results:

    >>> import pyfolio as pf
    >>> pf.from_zipline_csv("momentum_pipeline_results.csv")
    """
    _params = {}
    if data_frequency:
        _params["data_frequency"] = data_frequency
    if capital_base:
        _params["capital_base"] = capital_base
    if bundle:
        _params["bundle"] = bundle
    if start_date:
        _params["start_date"] = start_date
    if end_date:
        _params["end_date"] = end_date
    if params:
        _params["params"] = dict_to_dict_strs(params)
    if progress:
        _params["progress"] = progress

    response = houston.post("/zipline/backtests/{0}".format(strategy), params=_params, timeout=60*60*96)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_backtest(*args, **kwargs):
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(backtest, *args, **kwargs)

def scan_parameters(
    strategy: str,
    data_frequency: Literal["daily", "minute", "d", "m"] = None,
    capital_base: float = None,
    bundle: str = None,
    start_date: str = None,
    end_date: str = None,
    param1: str = None,
    vals1: list[Any] = None,
    param2: str = None,
    vals2: list[Any] = None,
    progress: str = None,
    params: dict[str, Any] = None,
    num_workers: int = None,
    filepath_or_buffer: FilepathOrBuffer = None
    ) -> None:
    """
    Run a parameter scan for a Zipline strategy.

    Returns a CSV of scan results which can be plotted with
    `moonchart.ParamscanTearsheet`.

    Parameters
    ----------
    strategy : str, required
        the strategy to run (strategy filename without extension)

    data_frequency : str, optional
        the data frequency of the simulation. Possible choices: daily, minute
        (or aliases d, m). Default is minute.

    capital_base : float, optional
        the starting capital for the simulation (default is 1e6 (1 million))

    bundle : str, optional
        the data bundle to use for the simulation. If omitted, the default bundle (if set)
        is used.

    start_date : str (YYYY-MM-DD), optional
        the start date of the simulation (defaults to the bundle start date)

    end_date : str (YYYY-MM-DD), optional
        the end date of the simulation (defaults to today)

    param1 : str, required
        the name of the parameter to test (a module-level attribute
        in the algo file)

    vals1 : list of int/float/str/tuple, required
        parameter values to test (values can be ints, floats, strings, False,
        True, None, 'default' (to test current param value), or lists of
        ints/floats/strings)

    param2 : str, optional
        name of a second parameter to test (for 2-D parameter scans)

    vals2 : list of int/float/str/tuple, optional
        values to test for parameter 2 (values can be ints, floats, strings,
        False, True, None, 'default' (to test current param value), or lists
        of ints/floats/strings)

    params : dict of PARAM:VALUE, optional
        one or more strategy parameters (defined as module-level attributes
        in the algo file) to modify on the fly before running the parameter
        scan (pass as {param:value}).

    num_workers : int, optional
        the number of parallel workers to run. Running in parallel can speed
        up the parameter scan if your system has adequate resources. Default
        is 1, meaning no parallel processing.

    progress : str, optional
        log backtest progress at this interval (use a pandas offset alias,
        for example "D" for daily, "W" for weeky, "M" for monthly,
        "A" for annually). This parameter controls logging in the underlying
        backtests; a summary of scan results will be logged regardless of this
        parameter. Using this parameter when num_workers is greater than 1 will
        result in messy and interleaved log output and is not recommended.

    filepath_or_buffer : str, optional
        the location to write the output file (omit to write to stdout)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Zipline parameter scans: https://qrok.it/dl/qr/zipline-paramscan

    Examples
    --------
    Run a parameter scan for a moving average strategy called dma, then
    view a tear sheet of the results:

    >>> from moonchart import ParamscanTearsheet
    >>> scan_parameters("dma",
                        bundle="usstock-1min",
                        data_frequency="daily",
                        start_date="2015-01-03",
                        end_date="2022-06-30",
                        param1="MAVG_WINDOW",
                        vals1=[20, 50, 100],
                        filepath_or_buffer="dma_MAVG_WINDOW.csv")
    >>> ParamscanTearsheet.from_csv("dma_MAVG_WINDOW.csv")

    Run a 2-D parameter scan testing combinations of values for a long and
    short moving average, using 3 parallel worker processes:

    >>> scan_parameters("dma",
                        bundle="usstock-1min",
                        data_frequency="daily",
                        start_date="2015-01-03",
                        end_date="2022-06-30",
                        param1="LONG_MAVG_WINDOW",
                        vals1=[100, 200],
                        param2="SHORT_MAVG_WINDOW",
                        vals2=[20, 50],
                        num_workers=3,
                        filepath_or_buffer="dma_LONG_MAVG_WINDOW_and_SHORT_MAVG_WINDOW.csv")
    >>> ParamscanTearsheet.from_csv("dma_LONG_MAVG_WINDOW_and_SHORT_MAVG_WINDOW.csv")
    """
    _params = {}
    if data_frequency:
        _params["data_frequency"] = data_frequency
    if capital_base:
        _params["capital_base"] = capital_base
    if bundle:
        _params["bundle"] = bundle
    if start_date:
        _params["start_date"] = start_date
    if end_date:
        _params["end_date"] = end_date
    if progress:
        _params["progress"] = progress
    if param1:
        _params["param1"] = param1
    if vals1:
        _params["vals1"] = [str(v) for v in vals1]
    if param2:
        _params["param2"] = param2
    if vals2:
        _params["vals2"] = [str(v) for v in vals2]
    if params:
        _params["params"] = dict_to_dict_strs(params)
    if num_workers:
        _params["num_workers"] = num_workers

    response = houston.post("/zipline/paramscans/{0}".format(strategy), params=_params, timeout=60*60*96)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_scan_parameters(*args, **kwargs):
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(scan_parameters, *args, **kwargs)

def create_tearsheet(
    infilepath_or_buffer: FilepathOrBuffer,
    outfilepath_or_buffer: FilepathOrBuffer = None
    ) -> None:
    """
    Create a pyfolio PDF tear sheet from a Zipline backtest result.

    Parameters
    ----------
    infilepath_or_buffer : str, required
        the CSV file from a Zipline backtest (specify '-' to read file from stdin)

    outfilepath_or_buffer : str or file-like, optional
        the location to write the pyfolio tear sheet (write to stdout if omitted)

    Returns
    -------
    None
    """
    url = "/zipline/tearsheets"
    # Pyfolio can take a long time
    timeout = 60*60*5

    if infilepath_or_buffer == "-":
        infilepath_or_buffer = sys.stdin.buffer if six.PY3 else sys.stdin
        response = houston.post(url, data=infilepath_or_buffer, timeout=timeout)

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.post(url, data=infilepath_or_buffer, timeout=timeout)

    else:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.post(url, data=f, timeout=timeout)

    houston.raise_for_status_with_json(response)

    outfilepath_or_buffer = outfilepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(outfilepath_or_buffer, response)

def _cli_create_tearsheet(*args, **kwargs):
    return json_to_cli(create_tearsheet, *args, **kwargs)

def trade(
    strategy: str,
    bundle: str = None,
    account: str = None,
    data_frequency: Literal["daily", "minute", "d", "m"] = None,
    dry_run: bool = False
    ) -> dict[str, str]:
    """
    Trade a Zipline strategy.

    Parameters
    ----------
    strategy : str, required
        the strategy to run (strategy filename without extension)

    bundle : str, optional
        the data bundle to use. If omitted, the default bundle (if set)
        is used.

    account : str, optional
        the account to run the strategy in. Only required
        if the strategy is allocated to more than one
        account in quantrocket.zipline.allocations.yml.

    data_frequency : str, optional
        the data frequency to use. Possible choices: daily, minute
        (or aliases d, m). Default is minute.

    dry_run : bool
        write orders to file instead of sending them to the blotter.
        Orders will be written to
        /codeload/zipline/{strategy}.{account}.orders.{date}.csv.
        Default is False, meaning orders will be sent to the blotter
        and not written to file.

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Zipline live trading: https://qrok.it/dl/qr/zipline-trade

    Examples
    --------
    Trade a strategy defined in momentum-pipeline.py:

    >>> trade("momentum-pipeline", bundle="my-bundle")
    """
    params = {}
    if bundle:
        params["bundle"] = bundle
    if account:
        params["account"] = account
    if data_frequency:
        params["data_frequency"] = data_frequency
    if dry_run:
        params["dry_run"] = dry_run

    response = houston.post("/zipline/trade/{0}".format(strategy), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_trade(*args, **kwargs):
    return json_to_cli(trade, *args, **kwargs)

def list_active_strategies() -> dict[str, list[str]]:
    """
    List actively trading Zipline strategies.

    Returns
    -------
    dict
        dict of account: strategies

    Notes
    -----
    Usage Guide:

    * Zipline live trading: https://qrok.it/dl/qr/zipline-trade
    """
    response = houston.get("/zipline/trade")

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_active_strategies(*args, **kwargs):
    return json_to_cli(list_active_strategies, *args, **kwargs)

def cancel_strategies(
    strategies: Union[list[str], str] = None,
    accounts: Union[list[str], str] = None,
    cancel_all: bool = False
    ) -> dict[str, list[str]]:
    """
    Cancel actively trading strategies.

    Parameters
    ----------
    strategies : list of str, optional
        limit to these strategies

    accounts : list of str, optional
        limit to these accounts

    cancel_all : bool
        cancel all actively trading strategies

    Returns
    -------
    dict
        dict of actively trading strategies after canceling

    Notes
    -----
    Usage Guide:

    * Zipline live trading: https://qrok.it/dl/qr/zipline-trade

    Examples
    --------
    Cancel a single strategy:

    >>> cancel_strategies(strategies="momentum-pipeline")

    Cancel all strategies:

    >>> cancel_strategies(cancel_all=True)
    """
    params = {}
    if strategies:
        params["strategies"] = strategies
    if accounts:
        params["accounts"] = accounts
    if cancel_all:
        params["cancel_all"] = cancel_all

    response = houston.delete("/zipline/trade", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_cancel_strategies(*args, **kwargs):
    return json_to_cli(cancel_strategies, *args, **kwargs)

class ZiplineBacktestResult(object):
    """
    Convenience class for parsing a CSV result file from a Zipline backtest
    into a variety of useful DataFrames, which can be passed to pyfolio or
    inspected by the user.

    Notes
    -----
    Usage Guide:

    * Zipline backtesting: https://qrok.it/dl/qr/zipline-backtest

    Examples
    --------
    Run a Zipline backtest and parse the CSV results:

    >>> f = io.StringIO()
    >>> backtest("momentum_pipeline.py",
                 bundle="etf-sampler-1d",
                 start="2015-02-04",
                 end="2015-12-31",
                 filepath_or_buffer=f)
    >>> zipline_result = ZiplineBacktestResult.from_csv(f)

    The ZiplineBacktestResult object contains returns, positions, transactions,
    benchmark_returns, and the performance DataFrame.

    >>> print(zipline_result.returns.head())
    >>> print(zipline_result.positions.head())
    >>> print(zipline_result.transactions.head())
    >>> print(zipline_result.benchmark_returns.head())
    >>> print(zipline_result.perf.head())

    The outputs are ready to be passed to pyfolio:

    >>> pf.create_full_tear_sheet(
            zipline_result.returns,
            positions=zipline_result.positions,
            transactions=zipline_result.transactions,
            benchmark_rets=zipline_result.benchmark_returns)
    """

    def __init__(self):
        self.returns: 'pd.Series[float]' = None
        self.positions: 'pd.DataFrame' = None
        self.transactions: 'pd.DataFrame' = None
        self.benchmark_returns: 'pd.Series[float]' = None
        self.perf: 'pd.DataFrame' = None

    @classmethod
    def from_csv(cls, filepath_or_buffer: FilepathOrBuffer) -> 'ZiplineBacktestResult':
        """
        Parse a CSV result file from a Zipline backtest into a variety of useful
        DataFrames, which can be passed to pyfolio or inspected by the user.

        Notes
        -----
        Usage Guide:

        * Zipline backtesting: https://qrok.it/dl/qr/zipline-backtest

        Examples
        --------
        Run a Zipline backtest and parse the CSV results:

        >>> f = io.StringIO()
        >>> backtest("momentum_pipeline.py",
                    bundle="etf-sampler-1d",
                    start="2015-02-04",
                    end="2015-12-31",
                    filepath_or_buffer=f)
        >>> zipline_result = ZiplineBacktestResult.from_csv(f)

        The ZiplineBacktestResult object contains returns, positions, transactions,
        benchmark_returns, and the performance DataFrame.

        >>> print(zipline_result.returns.head())
        >>> print(zipline_result.positions.head())
        >>> print(zipline_result.transactions.head())
        >>> print(zipline_result.benchmark_returns.head())
        >>> print(zipline_result.perf.head())

        The outputs are ready to be passed to pyfolio:

        >>> pf.create_full_tear_sheet(
                zipline_result.returns,
                positions=zipline_result.positions,
                transactions=zipline_result.transactions,
                benchmark_rets=zipline_result.benchmark_returns)
        """
        # Import pandas lazily since it can take a moment to import
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas must be installed to use ZiplineBacktestResult")

        zipline_result = cls()

        results = pd.read_csv(
            filepath_or_buffer,
            parse_dates=["date"],
            index_col=["dataframe", "index", "date", "column"],
            low_memory=False)["value"]

        # Extract returns
        returns = results.loc["returns"].unstack()
        is_tz_naive = returns.index.droplevel(0).tz is None
        returns.index = returns.index.droplevel(0)
        if is_tz_naive:
            returns.index = returns.index.tz_localize("UTC")
        else:
            returns.index = returns.index.tz_convert("UTC")
        zipline_result.returns = returns["returns"].astype(float)

        # Extract positions
        if "positions" in results.index.get_level_values("dataframe"):
            positions = results.loc["positions"].unstack()
            positions.index = positions.index.droplevel(0)
            if is_tz_naive:
                positions.index = positions.index.tz_localize("UTC")
            else:
                positions.index = positions.index.tz_convert("UTC")
            zipline_result.positions = positions.astype(float)

        # Extract transactions
        transactions = results.loc["transactions"].unstack()
        transactions.index = transactions.index.droplevel(0)
        if is_tz_naive:
            transactions.index = transactions.index.tz_localize("UTC")
        else:
            transactions.index = transactions.index.tz_convert("UTC")
        zipline_result.transactions = transactions.apply(pd.to_numeric, errors='ignore')

        # Extract benchmark returns
        if "benchmark" in results.index.get_level_values("dataframe"):
            benchmark_returns = results.loc["benchmark"].unstack()
            benchmark_returns.index = benchmark_returns.index.droplevel(0)
            if is_tz_naive:
                benchmark_returns.index = benchmark_returns.index.tz_localize("UTC")
            else:
                benchmark_returns.index = benchmark_returns.index.tz_convert("UTC")
            zipline_result.benchmark_returns = benchmark_returns["benchmark"].astype(float)

        # Extract performance dataframe
        perf = results.loc["perf"].unstack()
        perf.index = perf.index.droplevel(0)
        if is_tz_naive:
            perf.index = perf.index.tz_localize("UTC")
        else:
            perf.index = perf.index.tz_convert("UTC")
        zipline_result.perf = perf.apply(pd.to_numeric, errors='ignore')

        return zipline_result

ZiplineBacktestResult.from_csv.__func__.__doc__ = ZiplineBacktestResult.__doc__
