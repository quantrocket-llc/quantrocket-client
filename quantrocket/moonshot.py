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
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs

def backtest(strategies, start_date=None, end_date=None, allocations=None,
                 nlv=None, params=None, details=None, csv=None, filepath_or_buffer=None):
    """
    Backtest one or more strategies.

    By default returns a PDF tear sheet of performance charts but can also return a CSV of
    backtest results.

    Parameters
    ----------
    strategies : list of str, required
        one or more strategy codes

    start_date : str (YYYY-MM-DD), optional
        the backtest start date (default is to use all available history)

    end_date : str (YYYY-MM-DD), optional
        the backtest end date (default is to use all available history)

    allocations : dict of CODE:FLOAT, optional
        the allocation for each strategy, passed as {code:allocation} (default
        allocation is 1.0 / number of strategies)

    nlv : dict of CURRENCY:NLV, optional
        the NLV (net liquidation value, i.e. account balance) to assume for
        the backtest, expressed in each currency represented in the backtest (pass
        as {currency:nlv})

    params : dict of PARAM:VALUE, optional
        one or more strategy params to set on the fly before backtesting
        (pass as {param:value})

    details : bool
        return detailed results for all securities instead of aggregating to
        strategy level (only supported for single-strategy backtests)

    csv : bool
        return a CSV of performance data (default is to return a PDF
        performance tear sheet)

    filepath_or_buffer : str, optional
        the location to write the results file (omit to write to stdout)

    Returns
    -------
    None
    """
    _params = {}
    if strategies:
        _params["strategies"] = strategies
    if start_date:
        _params["start_date"] = start_date
    if end_date:
        _params["end_date"] = end_date
    if allocations:
        _params["allocations"] = dict_to_dict_strs(allocations)
    if nlv:
        _params["nlv"] = dict_to_dict_strs(nlv)
    if details:
        _params["details"] = details
    if csv:
        _params["csv"] = csv
    if params:
        _params["params"] = dict_to_dict_strs(params)

    response = houston.post("/moonshot/backtests", params=_params, timeout=60*60*24)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_backtest(*args, **kwargs):
    allocations = kwargs.get("allocations", None)
    if allocations:
        kwargs["allocations"] = dict_strs_to_dict(*allocations)
    nlv = kwargs.get("nlv", None)
    if nlv:
        kwargs["nlv"] = dict_strs_to_dict(*nlv)
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(backtest, *args, **kwargs)

def scan_parameters(strategies, start_date=None, end_date=None,
                    param1=None, vals1=None, param2=None, vals2=None,
                    allocations=None, nlv=None, params=None, csv=None,
                    filepath_or_buffer=None):
    """
    Run a parameter scan for one or more strategies.

    By default returns a PDF tear sheet of results but can also return a CSV.

    Parameters
    ----------
    strategies : list of str, required
        one or more strategy codes

    start_date : str (YYYY-MM-DD), optional
        the backtest start date (default is to use all available history)

    end_date : str (YYYY-MM-DD), optional
        the backtest end date (default is to use all available history)

    param1 : str, required
        the name of the parameter to test (a class attribute on the strategy)

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

    allocations : dict of CODE:FLOAT, optional
        the allocation for each strategy, passed as {code:allocation} (default
        allocation is 1.0 / number of strategies)

    nlv : dict of CURRENCY:NLV, optional
        the NLV (net liquidation value, i.e. account balance) to assume for
        the backtest, expressed in each currency represented in the backtest (pass
        as {currency:nlv})

    params : dict of PARAM:VALUE, optional
        one or more strategy params to set on the fly before backtesting
        (pass as {param:value})

    csv : bool
        return a CSV of performance data (default is to return a PDF
        tear sheet)

    filepath_or_buffer : str, optional
        the location to write the results file (omit to write to stdout)

    Returns
    -------
    None
    """
    _params = {}
    if strategies:
        _params["strategies"] = strategies
    if start_date:
        _params["start_date"] = start_date
    if end_date:
        _params["end_date"] = end_date
    if param1:
        _params["param1"] = param1
    if vals1:
        _params["vals1"] = [str(v) for v in vals1]
    if param2:
        _params["param2"] = param2
    if vals2:
        _params["vals2"] = [str(v) for v in vals2]
    if allocations:
        _params["allocations"] = dict_to_dict_strs(allocations)
    if nlv:
        _params["nlv"] = dict_to_dict_strs(nlv)
    if csv:
        _params["csv"] = csv
    if params:
        _params["params"] = dict_to_dict_strs(params)

    response = houston.post("/moonshot/paramscans", params=_params, timeout=60*60*24)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_scan_parameters(*args, **kwargs):
    allocations = kwargs.get("allocations", None)
    if allocations:
        kwargs["allocations"] = dict_strs_to_dict(*allocations)
    nlv = kwargs.get("nlv", None)
    if nlv:
        kwargs["nlv"] = dict_strs_to_dict(*nlv)
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(scan_parameters, *args, **kwargs)

def generate_orders(strategies, accounts=None, json=False, filepath_or_buffer=None):
    """
    Run one or more strategies and generate orders.

    Allocations are read from configuration (quantrocket.moonshot.allocations.yml).

    Parameters
    ----------
    strategies : list of str, required
        one or more strategy codes

    accounts : list of str, optional
        limit to these accounts

    json : bool
        format orders as JSON (default is CSV)

    filepath_or_buffer : str, optional
        the location to write the orders file (omit to write to stdout)

    Returns
    -------
    None
    """
    params = {}
    if strategies:
        params["strategies"] = strategies
    if accounts:
        params["accounts"] = accounts

    output = "json" if json else "csv"

    response = houston.get("/moonshot/orders.{0}".format(output), params=params, timeout=60*5)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_generate_orders(*args, **kwargs):
    return json_to_cli(generate_orders, *args, **kwargs)
