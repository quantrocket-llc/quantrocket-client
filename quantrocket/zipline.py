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
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.stream import to_bytes
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer

def run_algorithm(algofile, data_frequency=None, capital_base=None,
                          bundle=None, bundle_timestamp=None, start=None, end=None,
                          filepath_or_buffer=None, calendar=None):
    """
    Run a Zipline backtest and write the test results to a CSV file.

    The CSV result file contains several DataFrames stacked into one: the Zipline performance
    results, plus the extracted returns, transactions, positions, and benchmark returns from those
    results.

    Parameters
    ----------
    algofile : str, required
        the file that contains the algorithm to run

    data_frequency : str, optional
        the data frequency of the simulation. Possible choices: daily, minute (default is daily)

    capital_base : float, optional
        the starting capital for the simulation (default is 10000000.0)

    bundle : str, optional
        the data bundle to use for the simulation (default is quantopian-quandl)

    bundle_timestamp : str, optional
        the date to lookup data on or before (default is <current-time>)

    start : str (YYYY-MM-DD), required
        the start date of the simulation

    end : str (YYYY-MM-DD), required
        the end date of the simulation

    filepath_or_buffer : str, optional
        the location to write the output file (omit to write to stdout)

    calendar : str, optional
        the calendar you want to use e.g. LSE. NYSE is the default.

    Returns
    -------
    None

    Examples
    --------
    Run a backtest and load the results into pandas.

    >>> from quantrocket.zipline import run_algorithm
    >>> import pandas as pd
    >>> import io
    >>> f = io.StringIO()
    >>> run_algorithm("momentum_pipeline.py", bundle="my-bundle", start="2015-02-04", end="2015-12-31", filepath_or_buffer=f)
    >>> results = pd.read_csv(f, index_col=["dataframe", "index", "column"])["value"]

    To use the results with pyfolio, extract and massage the returns, positions,
    transactions, and benchmark returns:

    >>> # Extract returns
    >>> returns = results.loc["returns"].unstack()
    >>> returns.index = returns.index.droplevel(0).tz_localize("UTC")
    >>> returns = returns["returns"].astype(float)
    >>> # Extract positions
    >>> positions = results.loc["positions"].unstack()
    >>> positions.index = positions.index.droplevel(0).tz_localize("UTC")
    >>> positions = positions.astype(float)
    >>> # Extract transactions
    >>> transactions = results.loc["transactions"].unstack()
    >>> transactions.index = transactions.index.droplevel(0).tz_localize("UTC")
    >>> transactions = transactions.apply(pd.to_numeric, errors='ignore')
    >>> # Extract benchmark
    >>> benchmark_returns = results.loc["benchmark"].unstack()
    >>> benchmark_returns.index = benchmark_returns.index.droplevel(0).tz_localize("UTC")
    >>> benchmark_returns = benchmark_returns["benchmark"].astype(float)

    Ready for pyfolio:

    >>> pf.create_full_tear_sheet(returns, positions=positions, transactions=transactions, benchmark_rets=benchmark_returns)
    """
    params = {}
    if data_frequency:
        params["data_frequency"] = data_frequency
    if capital_base:
        params["capital_base"] = capital_base
    if bundle:
        params["bundle"] = bundle
    if bundle_timestamp:
        params["bundle_timestamp"] = bundle_timestamp
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if calendar:
        params["calendar"] = calendar

    response = houston.post("/zipline/backtests/{0}".format(algofile), params=params, timeout=60*60*3)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_run_algorithm(*args, **kwargs):
    return json_to_cli(run_algorithm, *args, **kwargs)

def create_tearsheet(infilepath_or_buffer, outfilepath_or_buffer=None, simple=None,
                     live_start_date=None, slippage=None, hide_positions=None,
                     bayesian=None, round_trips=None, bootstrap=None):
    """
    Create a pyfolio PDF tear sheet from a Zipline backtest result.

    Parameters
    ----------
    infilepath_or_buffer : str, required
        the CSV file from a Zipline backtest (specify '-' to read file from stdin)

    outfilepath_or_buffer : str or file-like, optional
        the location to write the pyfolio tear sheet (write to stdout if omitted)

    simple : bool
        create a simple tear sheet (default is to create a full tear sheet)

    live_start_date : str (YYYY-MM-DD), optional
        date when the strategy began live trading

    slippage : int or float, optional
        basis points of slippage to apply to returns before generating tear sheet
        stats and plots

    hide_positions : bool
        don't output any symbol names

    bayesian : bool
        include a Bayesian tear sheet

    round_trips : bool
        include a round-trips tear sheet

    bootstrap : bool
        perform bootstrap analysis for the performance metrics (takes a few minutes
        longer)

    Returns
    -------
    None
    """
    params = {}
    if simple:
        params["simple"] = simple
    if live_start_date:
        params["live_start_date"] = live_start_date
    if slippage:
        params["slippage"] = slippage
    if hide_positions:
        params["hide_positions"] = hide_positions
    if bayesian:
        params["bayesian"] = bayesian
    if round_trips:
        params["round_trips"] = round_trips
    if bootstrap:
        params["bootstrap"] = bootstrap

    url = "/zipline/tearsheets"
    # Pyfolio can take a long time, particularly for Bayesian analysis
    timeout = 60*60*5

    if infilepath_or_buffer == "-":
        infilepath_or_buffer = sys.stdin.buffer if six.PY3 else sys.stdin
        response = houston.post(url, data=infilepath_or_buffer, params=params, timeout=timeout)

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.post(url, data=infilepath_or_buffer, params=params, timeout=timeout)

    else:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.post(url, data=f, params=params, timeout=timeout)

    houston.raise_for_status_with_json(response)

    outfilepath_or_buffer = outfilepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(outfilepath_or_buffer, response)

def _cli_create_tearsheet(*args, **kwargs):
    return json_to_cli(create_tearsheet, *args, **kwargs)

def ingest_bundle(history_db=None, calendar=None, bundle=None, assets_versions=None):
    """
    Ingest a data bundle into Zipline for later backtesting.

    You can ingest 1-minute or 1-day history databases from QuantRocket, or you
    can ingest data using Zipline's built-in capabilities.

    Parameters
    ----------
    history_db : str, optional
        the code of a history db to ingest

    calendar : str, optional
        the name of the calendar to use with this history db bundle (default is
        NYSE; provide an invalid calendar name to see available choices)

    bundle : str, optional
        the data bundle to ingest (default is quantopian-quandl); don't provide
        if specifying history_db

    assets_versions : list of int, optional
        versions of the assets db to which to downgrade

    Returns
    -------
    dict
        status message
    """
    params = {}
    if history_db:
        params["history_db"] = history_db
    if calendar:
        params["calendar"] = calendar
    if bundle:
        params["bundle"] = bundle
    if assets_versions:
        params["assets_versions"] = assets_versions

    response = houston.post("/zipline/bundles", params=params, timeout=60*30)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_ingest_bundle(*args, **kwargs):
    return json_to_cli(ingest_bundle, *args, **kwargs)

def list_bundles():
    """
    List all of the available data bundles.

    Returns
    -------
    dict
        data bundles and timestamps
    """
    response = houston.get("/zipline/bundles")

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_bundles(*args, **kwargs):
    return json_to_cli(list_bundles, *args, **kwargs)

def clean_bundles(bundle=None, before=None, after=None, keep_last=None):
    """
    Clean up data downloaded with the ingest command.

    Parameters
    ----------
    bundle : str, optional
        the data bundle to clean (default is quantopian-quandl)

    before : str, optional
        clear all data before this TIMESTAMP. This may not be passed with keep_last

    after : str, optional
        clear all data after this TIMESTAMP. This may not be passed with keep_last

    keep_last : int, optional
        clear all but the last N downloads. This may not be passed with before or after.

    Returns
    -------
    list
        bundles removed
    """
    params = {}
    if bundle:
        params["bundle"] = bundle
    if before:
        params["before"] = before
    if after:
        params["after"] = after
    if keep_last:
        params["keep_last"] = keep_last

    response = houston.delete("/zipline/bundles", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_clean_bundles(*args, **kwargs):
    return json_to_cli(clean_bundles, *args, **kwargs)

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
        returns.index = returns.index.droplevel(0).tz_localize("UTC")
        zipline_result.returns = returns["returns"].astype(float)

        # Extract positions
        positions = results.loc["positions"].unstack()
        positions.index = positions.index.droplevel(0).tz_localize("UTC")
        zipline_result.positions = positions.astype(float)

        # Extract transactions
        transactions = results.loc["transactions"].unstack()
        transactions.index = transactions.index.droplevel(0).tz_localize("UTC")
        zipline_result.transactions = transactions.apply(pd.to_numeric, errors='ignore')

        # Extract benchmark returns
        benchmark_returns = results.loc["benchmark"].unstack()
        benchmark_returns.index = benchmark_returns.index.droplevel(0).tz_localize("UTC")
        zipline_result.benchmark_returns = benchmark_returns["benchmark"].astype(float)

        # Extract performance dataframe
        perf = results.loc["perf"].unstack()
        perf.index = perf.index.droplevel(0).tz_localize("UTC")
        zipline_result.perf = perf.apply(pd.to_numeric, errors='ignore')

        return zipline_result
