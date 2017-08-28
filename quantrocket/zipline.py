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
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer

def run_zipline_algorithm(algofile, data_frequency=None, capital_base=None,
                          bundle=None, bundle_timestamp=None, start=None, end=None,
                          filepath_or_buffer=None, raw=False):
    """
    Run a Zipline backtest and return a Pyfolio tearsheet or results pickle.

    Parameters
    ----------
    algofile : str, required
        the file that contains the algorithm to run

    data_frequency : str, optional
        the data frequency of the simulation. Possible choices: daily, minute (default is daily)

    capital_base : float, optional
        the starting capital for the simulation (default is 10000000.0)

    bundle : str, optional
        the vendor to fetch data from (defaults to 'ib' which is currently the only
        supported vendor)

    bundle_timestamp : str, optional
        the date to lookup data on or before (default is <current-time>)

    start : str (YYYY-MM-DD), required
        the start date of the simulation

    end : str (YYYY-MM-DD), required
        the end date of the simulation

    filepath_or_buffer : str, optional
        the location to write the Pyfolio tearsheet (or pickle file if raw=True).
        If this is '-' the perf will be written to stdout (default is -)

    raw : bool
        return pickle file of results instead of Pyfolio tearsheet

    Returns
    -------
    None
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
    if raw:
        params["raw"] = raw

    response = houston.post("/zipline/backtests/{0}".format(algofile), params=params, timeout=60*60*3)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_run_zipline_algorithm(*args, **kwargs):
    return json_to_cli(run_zipline_algorithm, *args, **kwargs)

def ingest_bundle(history_db=None, bundle=None, assets_versions=None):
    """
    Ingest a data bundle into Zipline for later backtesting.

    Parameters
    ----------
    history_db : str, optional
        the code of a history db to ingest

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
    list
        data bundles
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
    dict
        status message
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
