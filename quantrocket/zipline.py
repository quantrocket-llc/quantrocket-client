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
import six
import requests
from quantrocket.houston import houston
from quantrocket.exceptions import NoHistoricalData
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.stream import to_bytes
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs
from quantrocket.utils.warn import deprecated_replaced_by

def create_usstock_bundle(code, sids=None, universes=None, free=False, data_frequency=None):
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
        limit to free sample data

    data_frequency : str, optional
         whether to collect minute data (which also includes daily data) or only
         daily data. Default is minute data. Possible choices: daily, minute (or
         aliases d, m)

    Returns
    -------
    dict
        status message

    Examples
    --------
    Create a minute data bundle for all US stocks:

    >>> create_usstock_bundle("usstock-1min")

    Create a bundle for daily data only:

    >>> create_usstock_bundle("usstock-1d", data_frequency="daily")

    Create a minute data bundle based on a universe:

    >>> create_usstock_bundle("usstock-tech-1min", universes="us-tech")

    Create a minute data bundle of free sample data:

    >>> create_usstock_bundle("usstock-free-1min", free=True)
    """
    params = {}
    params["ingest_type"] = "usstock"
    if sids:
        params["sids"] = sids
    if universes:
        params["universes"] = universes
    if free:
        params["free"] = free
    if data_frequency:
        params["data_frequency"] = data_frequency

    response = houston.put("/zipline/bundles/{}".format(code), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_usstock_bundle(*args, **kwargs):
    return json_to_cli(create_usstock_bundle, *args, **kwargs)

def create_bundle_from_db(code, from_db, calendar,
                         start_date=None, end_date=None,
                         universes=None, sids=None,
                        exclude_universes=None, exclude_sids=None,
                        fields=None):
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

    from_db : str, required
        the code of a history database or real-time aggregate database to ingest

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

def ingest_bundle(code, sids=None, universes=None):
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

def list_bundles():
    """
    List available data bundles and whether data has been
    ingested into them.

    Returns
    -------
    dict
        data bundles and whether they have data (True indicates
        data, False indicates config only)
    """
    response = houston.get("/zipline/bundles")

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_bundles(*args, **kwargs):
    return json_to_cli(list_bundles, *args, **kwargs)

def get_bundle_config(code):
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
    """
    response = houston.get(f"/zipline/bundles/config/{code}")

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_bundle_config(*args, **kwargs):
    return json_to_cli(get_bundle_config, *args, **kwargs)

def drop_bundle(code, confirm_by_typing_bundle_code_again=None):
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

def get_default_bundle():
    """
    Return the current default bundle, if any.

    Returns
    -------
    dict
        default bundle
    """
    response = houston.get("/zipline/config")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_default_bundle(bundle):
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

def download_bundle_file(code, filepath_or_buffer=None,
                         start_date=None, end_date=None,
                         data_frequency=None,
                         universes=None, sids=None,
                         exclude_universes=None, exclude_sids=None,
                         times=None, fields=None):
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

    Examples
    --------
    Load minute prices into pandas:

    >>> download_bundle_file("usstock-1min", sids=["FIBBG12345"])
    >>> prices = pd.read_csv(f, parse_dates=["Date"], index_col=["Field","Date"])

    Isolate fields with .loc:

    >>> closes = prices.loc["Close"]

    See Also
    --------
    quantrocket.get_prices : load prices into a DataFrame
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
        if "data match the query parameters" in repr(e).lower():
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

def backtest(strategy, data_frequency=None, capital_base=None, bundle=None,
             start_date=None, end_date=None, progress=None, filepath_or_buffer=None):
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

    progress : str, optional
        log backtest progress at this interval (use a pandas offset alias,
        for example "D" for daily, "W" for weeky, "M" for monthly,
        "A" for annually)

    filepath_or_buffer : str, optional
        the location to write the output file (omit to write to stdout)

    Returns
    -------
    None

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
    params = {}
    if data_frequency:
        params["data_frequency"] = data_frequency
    if capital_base:
        params["capital_base"] = capital_base
    if bundle:
        params["bundle"] = bundle
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if progress:
        params["progress"] = progress

    response = houston.post("/zipline/backtests/{0}".format(strategy), params=params, timeout=60*60*96)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_backtest(*args, **kwargs):
    return json_to_cli(backtest, *args, **kwargs)

def create_tearsheet(infilepath_or_buffer, outfilepath_or_buffer=None):
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

def trade(strategy, bundle=None, account=None, data_frequency=None):
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

    Returns
    -------
    None

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

    response = houston.post("/zipline/trade/{0}".format(strategy), params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_trade(*args, **kwargs):
    return json_to_cli(trade, *args, **kwargs)

def list_active_strategies():
    """
    List actively trading Zipline strategies.

    Returns
    -------
    dict
        dict of account: strategies
    """
    response = houston.get("/zipline/trade")

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_active_strategies(*args, **kwargs):
    return json_to_cli(list_active_strategies, *args, **kwargs)

def cancel_strategies(strategies=None, accounts=None, cancel_all=False):
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

    Examples
    --------
    Run a Zipline backtest and parse the CSV results:

    >>> f = io.StringIO()
    >>> run_algorithm("momentum_pipeline.py",
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
        self.returns = None
        self.positions = None
        self.transactions = None
        self.benchmark_returns = None
        self.perf = None

    @classmethod
    def from_csv(cls, filepath_or_buffer):

        # Import pandas lazily since it can take a moment to import
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas must be installed to use ZiplineBacktestResult")

        zipline_result = cls()

        results = pd.read_csv(
            filepath_or_buffer,
            parse_dates=["date"],
            index_col=["dataframe", "index", "date", "column"])["value"]

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
