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

def fetch_reuters_statements(universes=None, conids=None):
    """
    Fetch Reuters financial statements from IB and save to database.

    This data provides cash flow, balance sheet, and income metrics.

    Parameters
    ----------
    universes : list of str, optional
        limit to these universes (must provide universes, conids, or both)

    conids : list of int, optional
        limit to these conids (must provide universes, conids, or both)

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
    response = houston.post("/fundamental/reuters/statements", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_fetch_reuters_statements(*args, **kwargs):
    return json_to_cli(fetch_reuters_statements, *args, **kwargs)

def fetch_reuters_estimates(universes=None, conids=None):
    """
    Fetch Reuters estimates and actuals from IB and save to database.

    This data provides analyst estimates and actuals for a variety of indicators.

    Parameters
    ----------
    universes : list of str, optional
        limit to these universes (must provide universes, conids, or both)

    conids : list of int, optional
        limit to these conids (must provide universes, conids, or both)

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
    response = houston.post("/fundamental/reuters/estimates", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_fetch_reuters_estimates(*args, **kwargs):
    return json_to_cli(fetch_reuters_estimates, *args, **kwargs)

def list_coa_codes(statement_types=None):
    """
    Query Chart of Account (COA) codes from the Reuters financial statements
    database.

    Note: you must fetch Reuters financial statements into the database before
    you can query COA codes.

    Parameters
    ----------
    statement_types : list of str, optional
        limit to these statement types. Possible choices: INC, BAL, CAS

    Returns
    -------
    dict
        COA codes and descriptions
    """
    params = {}
    if statement_types:
        params["statement_types"] = statement_types
    response = houston.get("/fundamental/reuters/statements/coa", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_coa_codes(*args, **kwargs):
    return json_to_cli(list_coa_codes, *args, **kwargs)

def download_reuters_statements(codes, filepath_or_buffer=None, output="csv",
                                start_date=None, end_date=None,
                                universes=None, conids=None,
                                exclude_universes=None, exclude_conids=None,
                                period_types=None, fields=None):
    """
    Query financial statements from the Reuters statements database and
    download to file.

    Parameters
    ----------
    codes : list of str, required
        the Chart of Account (COA) code(s) to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, txt, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to statements on or after this source date

    end_date : str (YYYY-MM-DD), optional
        limit to statements on or before this source date

    universes : list of str, optional
        limit to these universes

    conids : list of int, optional
        limit to these conids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_conids : list of int, optional
        exclude these conids

    period_types : list of str, optional
        limit to these period types. Possible choices: Interim, Annual

    fields : list of str, optional
        only return these fields

    Returns
    -------
    None

    Examples
    --------
    Query total revenue (COA code RTLR) for a universe of Australian stocks. You can use
    StringIO to load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_reuters_statements(["RTLR"], f, universes=["asx-stk"])
    >>> statements = pd.read_csv(f, parse_dates=["StatementDate", "SourceDate"])

    Query net income (COA code NINC) for two securities (identified by conid), limiting to
    annual reports:

    >>> download_reuters_statements(["NINC"], f, conids=[123456, 234567], period_types=["Annual"])
    """
    params = {}
    if codes:
        params["codes"] = codes
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
    if period_types:
        params["period_types"] = period_types
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json", "txt"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/reuters/statements.{0}".format(output), params=params,
                           timeout=60*5)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_reuters_statements(*args, **kwargs):
    return json_to_cli(download_reuters_statements, *args, **kwargs)