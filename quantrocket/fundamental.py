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
import sys
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.exceptions import ParameterError

def fetch_reuters_financials(universes=None, conids=None):
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
    response = houston.post("/fundamental/reuters/financials", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_fetch_reuters_financials(*args, **kwargs):
    return json_to_cli(fetch_reuters_financials, *args, **kwargs)

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

def list_reuters_codes(codes=None, report_types=None, statement_types=None):
    """
    List available Chart of Account (COA) codes from the Reuters financials database
    and/or indicator codes from the Reuters estimates/actuals database

    Note: you must fetch Reuters financials into the database before you can
    list COA codes.


    Parameters
    ----------
    codes : list of str, optional
        limit to these Chart of Account (COA) or indicator codes

    report_types : list of str, optional
        limit to these report types. Possible choices: financials, estimates

    statement_types : list of str, optional
        limit to these statement types. Only applies to financials, not estimates. Possible choices: INC, BAL, CAS

    Returns
    -------
    dict
        codes and descriptions
    """
    params = {}
    if codes:
        params["codes"] = codes
    if report_types:
        params["report_types"] = report_types
    if statement_types:
        params["statement_types"] = statement_types
    response = houston.get("/fundamental/reuters/codes", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_reuters_codes(*args, **kwargs):
    return json_to_cli(list_reuters_codes, *args, **kwargs)

def download_reuters_financials(codes, filepath_or_buffer=None, output="csv",
                                start_date=None, end_date=None,
                                universes=None, conids=None,
                                exclude_universes=None, exclude_conids=None,
                                interim=False, exclude_restatements=False, fields=None):
    """
    Query financial statements from the Reuters financials database and
    download to file.

    You can query one or more COA codes. Use the `list_reuters_codes` function to see
    available codes.

    Annual or interim reports are available. Annual is the default and provides
    deeper history.

    Parameters
    ----------
    codes : list of str, required
        the Chart of Account (COA) code(s) to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, txt, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to statements on or after this fiscal period end date

    end_date : str (YYYY-MM-DD), optional
        limit to statements on or before this fiscal period end date

    universes : list of str, optional
        limit to these universes

    conids : list of int, optional
        limit to these conids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_conids : list of int, optional
        exclude these conids

    interim : bool, optional
        return interim reports (default is to return annual reports,
        which provide deeper history)

    exclude_restatements : bool, optional
        exclude restatements (default is to include them)

    fields : list of str, optional
        only return these fields (pass ['?'] or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    Examples
    --------
    Query total revenue (COA code RTLR) for a universe of Australian stocks. You can use
    StringIO to load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_reuters_financials(["RTLR"], f, universes=["asx-stk"],
                                    start_date="2014-01-01"
                                    end_date="2017-01-01")
    >>> financials = pd.read_csv(f, parse_dates=["StatementDate", "SourceDate", "FiscalPeriodEndDate"])

    Query net income (COA code NINC) from interim reports for two securities
    (identified by conid) and exclude restatements:

    >>> download_reuters_financials(["NINC"], f, conids=[123456, 234567],
                                    interim=True, exclude_restatements=True)

    Query common and preferred shares outstanding (COA codes QTCO and QTPO) and return a
    minimal set of fields (several required fields will always be returned):

    >>> download_reuters_financials(["QTCO", "QTPO"], f, universes=["nyse-stk"],
                                    fields=["Amount"])
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
    if interim:
        params["interim"] = interim
    if exclude_restatements:
        params["exclude_restatements"] = exclude_restatements
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json", "txt"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/reuters/financials.{0}".format(output), params=params,
                           timeout=60*15)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_reuters_financials(*args, **kwargs):
    return json_to_cli(download_reuters_financials, *args, **kwargs)

def get_reuters_financials_reindexed_like(reindex_like, coa_codes, fields=["Amount"],
                           interim=False, exclude_restatements=False, max_lag=None):
    """
    Return a multiindex (CoaCode, Field, Date) DataFrame of point-in-time
    Reuters financial statements for one or more Chart of Account (COA)
    codes, reindexed to match the index (dates) and columns (conids) of
    `reindex_like`. Financial values are forward-filled in order to provide
    the latest reading at any given date. Financials are indexed to the
    SourceDate field, i.e. the date on which the financial statement was
    released. SourceDate is shifted forward 1 day to avoid lookahead bias.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and conids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    coa_codes : list of str, required
        the Chart of Account (COA) code(s) to query. Use the `list_reuters_codes`
        function to see available codes.

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        simply including the Amount field.

    interim : bool
        query interim/quarterly reports (default is to query annual reports,
        which provide deeper history)

    exclude_restatements : bool, optional
        exclude restatements (default is to include them)

    max_lag : str, optional
        maximum amount of time a data point can be used after the
        associated fiscal period has ended. Setting a limit can prevent
        using data that is reported long after the fiscal period ended, or
        can limit how far data is forward filled in the absence of subsequent
        data. Specify as a Pandas offset alias, e.g. '500D'. By default, no
        maximum limit is applied.

    Returns
    -------
    DataFrame
        a multiindex (CoaCode, Field, Date) DataFrame of financials,
        shaped like the input DataFrame

    Examples
    --------
    Let's calculate book value per share, defined as:

        (Total Assets - Total Liabilities) / Number of shares outstanding

    The COA codes for these metrics are 'ATOT' (Total Assets), 'LTLL' (Total
    Liabilities), and 'QTCO' (Total Common Shares Outstanding).


    >>> closes = prices.loc["Close"]
    >>> financials = get_reuters_financials_reindexed_like(closes, coa_codes=["ATOT", "LTLL", "QTCO"])
    >>> tot_assets = financials.loc["ATOT"].loc["Amount"]
    >>> tot_liabilities = financials.loc["LTLL"].loc["Amount"]
    >>> shares_out = financials.loc["QTCO"].loc["Amount"]
    >>> book_values_per_share = (tot_assets - tot_liabilities)/shares_out

    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    index_levels = reindex_like.index.names
    if "Time" in index_levels:
        raise ParameterError(
            "reindex_like should not have 'Time' in index, please take a cross-section first, "
            "for example: `prices.loc['Close'].xs('15:45:00', level='Time')`")

    if index_levels != ["Date"]:
        raise ParameterError(
            "reindex_like must have index called 'Date', but has {0}".format(
                ",".join(index_levels)))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    conids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    # Since financial reports are sparse, start well before the reindex_like
    # min date
    start_date -= pd.Timedelta(days=365+180)
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    f = six.StringIO()
    download_reuters_financials(
        coa_codes, f, conids=conids, start_date=start_date, end_date=end_date,
        fields=fields, interim=interim, exclude_restatements=exclude_restatements)
    financials = pd.read_csv(
        f, parse_dates=["SourceDate","FiscalPeriodEndDate"])

    # Rename SourceDate to match price history index name
    financials = financials.rename(columns={"SourceDate": "Date"})

    # Drop any fields we don't need
    needed_fields = set(fields)
    needed_fields.update(set(("ConId", "Date", "CoaCode")))
    if max_lag:
        needed_fields.add("FiscalPeriodEndDate")
    unneeded_fields = set(financials.columns) - needed_fields
    if unneeded_fields:
        financials = financials.drop(unneeded_fields, axis=1)

    # Create a unioned index of input DataFrame and statement SourceDates
    union_date_idx = reindex_like.index.union(financials.Date.drop_duplicates().values).sort_values()

    all_financials = {}
    for code in coa_codes:
        financials_for_code = financials.loc[financials.CoaCode == code]
        if "CoaCode" not in fields:
            financials_for_code = financials_for_code.drop("CoaCode", axis=1)
        # There might be duplicate SourceDates if a company announced
        # reports for several fiscal periods at once. In this case we keep
        # only the last value (i.e. latest fiscal period)
        financials_for_code = financials_for_code.drop_duplicates(subset=["ConId", "Date"], keep="last")
        financials_for_code = financials_for_code.pivot(index="ConId",columns="Date").T
        multiidx = pd.MultiIndex.from_product(
            (financials_for_code.index.get_level_values(0).unique(), union_date_idx),
            names=["Field", "Date"])
        financials_for_code = financials_for_code.reindex(index=multiidx, columns=reindex_like.columns)

        # financial values are sparse so ffill (one field at a time)
        all_fields_for_code = {}
        for field in financials_for_code.index.get_level_values("Field").unique():
            field_for_code = financials_for_code.loc[field].fillna(method="ffill")

            # Shift to avoid lookahead bias
            field_for_code = field_for_code.shift()

            all_fields_for_code[field] = field_for_code

        # Filter stale values if asked to
        if max_lag:
            fiscal_period_end_dates = all_fields_for_code["FiscalPeriodEndDate"]
            # subtract the max_lag from the index date to get the
            # earliest possible fiscal period end date for that row
            earliest_allowed_fiscal_period_end_dates = fiscal_period_end_dates.apply(
                lambda x: fiscal_period_end_dates.index - pd.Timedelta(max_lag))
            within_max_timedelta = fiscal_period_end_dates.apply(pd.to_datetime) >= earliest_allowed_fiscal_period_end_dates

            for field, field_for_code in all_fields_for_code.items():
                field_for_code = field_for_code.where(within_max_timedelta)
                all_fields_for_code[field] = field_for_code

            # Clean up if FiscalPeriodEndDate was just kept around for this purpose
            if "FiscalPeriodEndDate" not in fields:
                del all_fields_for_code["FiscalPeriodEndDate"]

        financials_for_code = pd.concat(all_fields_for_code, names=["Field", "Date"])

        # In cases the statements included dates not in the input
        # DataFrame, drop those now that we've ffilled
        extra_dates = union_date_idx.difference(reindex_like.index)
        if not extra_dates.empty:
            financials_for_code.drop(extra_dates, axis=0, level="Date", inplace=True)

        all_financials[code] = financials_for_code

    financials = pd.concat(all_financials, names=["CoaCode", "Field", "Date"])

    return financials

def download_reuters_estimates(codes, filepath_or_buffer=None, output="csv",
                               start_date=None, end_date=None,
                               universes=None, conids=None,
                               exclude_universes=None, exclude_conids=None,
                               period_types=None, fields=None):
    """
    Query estimates and actuals from the Reuters estimates database and
    download to file.

    You can query one or more indicator codes. Use the `list_reuters_codes`
    function to see available codes.

    Parameters
    ----------
    codes : list of str, required
        the indicator code(s) to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, txt, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to estimates and actuals on or after this fiscal period end date

    end_date : str (YYYY-MM-DD), optional
        limit to estimates and actuals on or before this fiscal period end date

    universes : list of str, optional
        limit to these universes

    conids : list of int, optional
        limit to these conids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_conids : list of int, optional
        exclude these conids

    period_types : list of str, optional
        limit to these fiscal period types. Possible choices: A, Q, S, where
        A=Annual, Q=Quarterly, S=Semi-Annual

    fields : list of str, optional
        only return these fields (pass ['?'] or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    Examples
    --------
    Query EPS estimates and actuals for a universe of Australian stocks. You can use
    StringIO to load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_reuters_estimates(["EPS"], f, universes=["asx-stk"],
                                    start_date="2014-01-01"
                                    end_date="2017-01-01")
    >>> estimates = pd.read_csv(f, parse_dates=["FiscalPeriodEndDate", "AnnounceDate"])
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

    response = houston.get("/fundamental/reuters/estimates.{0}".format(output), params=params,
                           timeout=60*15)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_reuters_estimates(*args, **kwargs):
    return json_to_cli(download_reuters_estimates, *args, **kwargs)

def get_reuters_estimates_reindexed_like(reindex_like, codes, fields=["Actual"],
                                         period_types=["Q"], max_lag=None):
    """
    Return a multiindex (Indicator, Field, Date) DataFrame of point-in-time
    Reuters estimates and actuals for one or more indicator codes, reindexed
    to match the index (dates) and columns (conids) of `reindex_like`.
    Estimates and actuals are forward-filled in order to provide the latest
    reading at any given date, indexed to the UpdatedDate field.
    UpdatedDate is shifted forward 1 day to avoid lookahead bias.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and conids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    codes : list of str, required
        the indicator code(s) to query. Use the `list_reuters_codes`
        function to see available codes.

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        simply including the Actual field.

    period_types : list of str, optional
        limit to these fiscal period types. Possible choices: A, Q, S, where
        A=Annual, Q=Quarterly, S=Semi-Annual. Default is Q/Quarterly.

    max_lag : str, optional
        maximum amount of time a data point can be used after the
        associated fiscal period has ended. Setting a limit can prevent
        using data that is reported long after the fiscal period ended, or
        can limit how far data is forward filled in the absence of subsequent
        data. Specify as a Pandas offset alias, e.g. '500D'. By default, no
        maximum limit is applied.

    Returns
    -------
    DataFrame
        a multiindex (Indicator, Field, Date) DataFrame of estimates and actuals,
        shaped like the input DataFrame

    Examples
    --------
    Query book value per share (code BVPS):

    >>> closes = prices.loc["Close"]
    >>> estimates = get_reuters_estimates_reindexed_like(closes, codes=["BVPS"])
    >>> book_values_per_share = estimates.loc["BVPS"].loc["Actual"]
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    index_levels = reindex_like.index.names
    if "Time" in index_levels:
        raise ParameterError(
            "reindex_like should not have 'Time' in index, please take a cross-section first, "
            "for example: `prices.loc['Close'].xs('15:45:00', level='Time')`")

    if index_levels != ["Date"]:
        raise ParameterError(
            "reindex_like must have index called 'Date', but has {0}".format(
                ",".join(index_levels)))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    conids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    # Since financial reports are sparse, start well before the reindex_like
    # min date
    start_date -= pd.Timedelta(days=365+180)
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    f = six.StringIO()
    if "UpdatedDate" not in fields:
        fields.append("UpdatedDate")
    download_reuters_estimates(
        codes, f, conids=conids, start_date=start_date, end_date=end_date,
        fields=fields, period_types=period_types)
    parse_dates = ["UpdatedDate","FiscalPeriodEndDate"]
    if "AnnounceDate" in fields:
        parse_dates.append("AnnounceDate")
    estimates = pd.read_csv(
        f, parse_dates=parse_dates)

    # Drop records with no actuals
    estimates = estimates.loc[estimates.UpdatedDate.notnull()]

    # Rename UpdatedDate to match price history index name
    estimates = estimates.rename(columns={"UpdatedDate": "Date"})

    # Drop any fields we don't need
    needed_fields = set(fields)
    needed_fields.update(set(("ConId", "Date", "Indicator")))
    if max_lag:
        needed_fields.add("FiscalPeriodEndDate")
    unneeded_fields = set(estimates.columns) - needed_fields
    if unneeded_fields:
        estimates = estimates.drop(unneeded_fields, axis=1)

    # Create a unioned index of input DataFrame and UpdatedDate
    union_date_idx = reindex_like.index.union(estimates.Date.drop_duplicates().values).sort_values()

    all_estimates = {}
    for code in codes:
        estimates_for_code = estimates.loc[estimates.Indicator == code]
        if "Indicator" not in fields:
            estimates_for_code = estimates_for_code.drop("Indicator", axis=1)
        # There might be duplicate UpdatedDates if a company announced
        # reports for several fiscal periods at once. In this case we keep
        # only the last value (i.e. latest fiscal period)
        estimates_for_code = estimates_for_code.drop_duplicates(subset=["ConId","Date"], keep="last")
        estimates_for_code = estimates_for_code.pivot(index="ConId",columns="Date").T
        multiidx = pd.MultiIndex.from_product(
            (estimates_for_code.index.get_level_values(0).unique(), union_date_idx),
            names=["Field", "Date"])
        estimates_for_code = estimates_for_code.reindex(index=multiidx, columns=reindex_like.columns)

        # estimates are sparse so ffill (one field at a time)
        all_fields_for_code = {}
        for field in estimates_for_code.index.get_level_values("Field").unique():
            field_for_code = estimates_for_code.loc[field].fillna(method="ffill")

            # Shift to avoid lookahead bias
            field_for_code = field_for_code.shift()

            all_fields_for_code[field] = field_for_code

        # Filter stale values if asked to
        if max_lag:
            fiscal_period_end_dates = all_fields_for_code["FiscalPeriodEndDate"]
            # subtract the max_lag from the index date to get the
            # earliest possible fiscal period end date for that row
            earliest_allowed_fiscal_period_end_dates = fiscal_period_end_dates.apply(
                lambda x: fiscal_period_end_dates.index - pd.Timedelta(max_lag))
            within_max_timedelta = fiscal_period_end_dates.apply(pd.to_datetime) >= earliest_allowed_fiscal_period_end_dates

            for field, field_for_code in all_fields_for_code.items():
                field_for_code = field_for_code.where(within_max_timedelta)
                all_fields_for_code[field] = field_for_code

            # Clean up if FiscalPeriodEndDate was just kept around for this purpose
            if "FiscalPeriodEndDate" not in fields:
                del all_fields_for_code["FiscalPeriodEndDate"]

        estimates_for_code = pd.concat(all_fields_for_code, names=["Field", "Date"])

        # In cases the statements included dates not in the input
        # DataFrame, drop those now that we've ffilled
        extra_dates = union_date_idx.difference(reindex_like.index)
        if not extra_dates.empty:
            estimates_for_code.drop(extra_dates, axis=0, level="Date", inplace=True)

        all_estimates[code] = estimates_for_code

    estimates = pd.concat(all_estimates, names=["Indicator", "Field", "Date"])

    return estimates
