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

def backtest(strategies, start_date=None, end_date=None, allocations=None,
                 nlv=None, params=None, details=None, raw=None, filepath_or_buffer=None):
    """
    Backtest one or more strategies and return a CSV of backtest results.

    Parameters
    ----------
    strategies : list of str, required
        one or more strategy codes

    start_date : str (YYYY-MM-DD), optional
        the backtest start date (default is to use all available history)

    end_date : str (YYYY-MM-DD), optional
        the backtest end date (default is to use all available history)

    allocations : list of float, optional
        a list of allocations corresponding to the list of strategies
        (must be the same length as strategies if provided)

    nlv : list of str (CURRENCY:NLV), optional
        the NLV (net liquidation value, i.e. account balance) to assume for
        the backtest, expressed in each currency represented in the backtest (pass
        as 'currency:nlv')

    params : list of str (PARAM:VALUE), optional
        one or more strategy params to set on the fly before backtesting
        (pass as 'param:value')

    details : bool
        return detailed results for all securities instead of aggregating to
        strategy level (only supported for single-strategy backtests)

    raw : bool
        return a CSV of raw performance data (default is to return a PDF
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
        _params["allocations"] = allocations
    if nlv:
        _params["nlv"] = nlv
    if details:
        _params["details"] = details
    if raw:
        _params["raw"] = raw
    if params:
        _params["params"] = params

    response = houston.post("/moonshot/backtests", params=_params, timeout=60*60*24)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_backtest(*args, **kwargs):
    return json_to_cli(backtest, *args, **kwargs)
