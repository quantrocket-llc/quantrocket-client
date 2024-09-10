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
Functions for collecting and querying fundamental data.

Functions
---------
collect_alpaca_etb
    Collect Alpaca easy-to-borrow data and save to database.

download_alpaca_etb
    Query Alpaca easy-to-borrow data from the local database and download to file.

get_alpaca_etb_reindexed_like
    Return a DataFrame of Alpaca easy-to-borrow status, reindexed to match the index
    (dates) and columns (sids) of the input DataFrame.

collect_ibkr_shortable_shares
    Collect Interactive Brokers shortable shares data and save to database.

collect_ibkr_borrow_fees
    Collect Interactive Brokers borrow fees data and save to database.

collect_ibkr_margin_requirements
    Collect Interactive Brokers margin requirements data and save to database.

download_ibkr_shortable_shares
    Query intraday or daily Interactive Brokers shortable shares data from the
    local database and download to file.

download_ibkr_borrow_fees
    Query Interactive Brokers borrow fees from the local database and download to file.

download_ibkr_margin_requirements
    Query Interactive Brokers margin requirements from the local database and
    download to file.

get_ibkr_shortable_shares_reindexed_like
    Return a DataFrame of Interactive Brokers shortable shares, reindexed to
    match the index (dates) and columns (sids) of the input DataFrame.

get_ibkr_borrow_fees_reindexed_like
    Return a DataFrame of Interactive Brokers borrow fees, reindexed to match
    the index (dates) and columns (sids) of the input DataFrame.

get_ibkr_margin_requirements_reindexed_like
    Return a multiindex (Field, Date) DataFrame of Interactive Brokers margin
    requirements, reindexed to match the index (dates) and columns (sids) of
    the input DataFrame.

collect_sharadar_fundamentals
    Collect fundamental data from Sharadar and save to database.

collect_sharadar_insiders
    Collect insider holdings data from Sharadar and save to database.

collect_sharadar_institutions
    Collect institutional investor data from Sharadar and save to database.

collect_sharadar_sec8
    Collect SEC Form 8-K events from Sharadar and save to database.

collect_sharadar_sp500
    Collect historical S&P 500 index constituents from Sharadar and save to
    database.

download_sharadar_fundamentals
    Query Sharadar fundamentals from the local database and download
    to file.

download_sharadar_insiders
    Query Sharadar insider holdings data from the local database and download
    to file.

download_sharadar_institutions
    Query Sharadar institutional investor data from the local database and
    download to file.

download_sharadar_sec8
    Query Sharadar SEC Form 8-K events data from the local database and
    download to file.

download_sharadar_sp500
    Query Sharadar S&P 500 index changes (additions and removals) from the
    local database and download to file.

get_sharadar_fundamentals_reindexed_like
    Return a multiindex (Field, Date) DataFrame of point-in-time
    Sharadar fundamentals, reindexed to match the index (dates)
    and columns (sids) of the input DataFrame.

get_sharadar_institutions_reindexed_like
    Return a multiindex (Field, Date) DataFrame of Sharadar institutional
    investor data, reindexed to match the index (dates) and columns (sids) of
    the input DataFrame.

get_sharadar_sec8_reindexed_like
    Return a Boolean DataFrame indicating whether securities filed SEC Form
    8-K for specified event codes on given dates, reindexed to match the index
    (dates) and columns (sids) of the input DataFrame.

get_sharadar_sp500_reindexed_like
    Return a Boolean DataFrame indicating whether securities were in the S&P
    500 on the given dates, reindexed to match the index (dates) and columns
    (sids) of the input DataFrame.

collect_brain_bsi
    Collect Brain Sentiment Indicator (BSI) data and save to database.

collect_brain_blmcf
    Collect Brain Language Metrics on Company Filings (BLMCF) data and save to
    database.

collect_brain_blmect
    Collect Brain Language Metrics on Earnings Call Transcripts (BLMECT) data
    and save to database.

download_brain_bsi
    Query Brain Sentiment Indicator (BSI) data from the local database and download
    to file.

download_brain_blmcf
    Query Brain Language Metrics on Company Filings (BLMCF) data from the local
    database and download to file.

download_brain_blmect
    Query Brain Language Metrics on Earnings Call Transcripts (BLMECT) data from
    the local database and download to file.

get_brain_bsi_reindexed_like
    Return a multiindex (Field, Date) DataFrame of Brain Sentiment Indicator (BSI) data,
    reindexed to match the index (dates) and columns (sids) of the input DataFrame.

get_brain_blmcf_reindexed_like
    Return a multiindex (Field, Date) DataFrame of Brain Language Metrics on Company
    Filings (BLMCF) data, reindexed to match the index (dates) and columns (sids) of
    the input DataFrame.

get_brain_blmect_reindexed_like
    Return a multiindex (Field, Date) DataFrame of Brain Language Metrics on Earnings
    Call Transcripts (BLMECT) data, reindexed to match the index (dates) and columns
    (sids) of the input DataFrame.

Notes
-----
Usage Guide:

* Fundamental Data: https://qrok.it/dl/qr/fundamental
"""
import six
import sys
import os
import datetime
import requests
from typing import TYPE_CHECKING, Union, Literal
if TYPE_CHECKING:
    import pandas as pd
from quantrocket.utils._typing import FilepathOrBuffer
from quantrocket.houston import houston
from quantrocket.master import download_master_file
from quantrocket._cli.utils.output import json_to_cli
from quantrocket._cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.exceptions import ParameterError, MissingData, NoFundamentalData

__all__ = [
    "collect_alpaca_etb",
    "download_alpaca_etb",
    "get_alpaca_etb_reindexed_like",
    "collect_ibkr_shortable_shares",
    "collect_ibkr_borrow_fees",
    "collect_ibkr_margin_requirements",
    "download_ibkr_shortable_shares",
    "download_ibkr_borrow_fees",
    "download_ibkr_margin_requirements",
    "get_ibkr_shortable_shares_reindexed_like",
    "get_ibkr_borrow_fees_reindexed_like",
    "get_ibkr_margin_requirements_reindexed_like",
    "collect_sharadar_fundamentals",
    "collect_sharadar_insiders",
    "collect_sharadar_institutions",
    "collect_sharadar_sec8",
    "collect_sharadar_sp500",
    "download_sharadar_fundamentals",
    "download_sharadar_insiders",
    "download_sharadar_institutions",
    "download_sharadar_sec8",
    "download_sharadar_sp500",
    "get_sharadar_fundamentals_reindexed_like",
    "get_sharadar_institutions_reindexed_like",
    "get_sharadar_sec8_reindexed_like",
    "get_sharadar_sp500_reindexed_like",
    "collect_brain_bsi",
    "collect_brain_blmcf",
    "collect_brain_blmect",
    "download_brain_bsi",
    "download_brain_blmcf",
    "download_brain_blmect",
    "get_brain_bsi_reindexed_like",
    "get_brain_blmcf_reindexed_like",
    "get_brain_blmect_reindexed_like",
]

def collect_alpaca_etb() -> dict[str, str]:
    """
    Collect Alpaca easy-to-borrow data and save to database.

    Data is updated daily. Historical data is available from March 2019.

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Alpaca ETB: https://qrok.it/dl/qr/alpaca-etb
    """
    response = houston.post("/fundamental/alpaca/stockloan/etb")

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_alpaca_etb(*args, **kwargs):
    return json_to_cli(collect_alpaca_etb, *args, **kwargs)

def download_alpaca_etb(
    filepath_or_buffer: FilepathOrBuffer,
    output: Literal["csv", "json"] = "csv",
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None
    ) -> None:
    """
    Query Alpaca easy-to-borrow data from the local database and download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Alpaca ETB: https://qrok.it/dl/qr/alpaca-etb

    Examples
    --------
    Query easy-to-borrow data for a universe of US stocks.

    >>> f = io.StringIO()
    >>> download_alpaca_etb("usa_etb.csv", universes=["usa-stk"])
    >>> etb = pd.read_csv("usa_etb.csv", parse_dates=["Date"])
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

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/alpaca/stockloan/etb.{0}".format(output), params=params,
                           timeout=60*5)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "no easy-to-borrow data match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_alpaca_etb(*args, **kwargs):
    return json_to_cli(download_alpaca_etb, *args, **kwargs)

def get_alpaca_etb_reindexed_like(
    reindex_like: 'pd.DataFrame'
    ) -> 'pd.DataFrame':
    """
    Return a DataFrame of Alpaca easy-to-borrow status, reindexed to match the index
    (dates) and columns (sids) of `reindex_like`.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    Returns
    -------
    DataFrame
        a Boolean DataFrame indicating easy-to-borrow status, shaped like
        the input DataFrame

    Notes
    -----
    Usage Guide:

    * Alpaca ETB: https://qrok.it/dl/qr/alpaca-etb

    Examples
    --------
    Get easy-to-borrow status for a DataFrame of stocks:

    >>> closes = prices.loc["Close"]
    >>> are_etb = get_alpaca_etb_reindexed_like(closes)
    """
    etb = _get_stockloan_data_reindexed_like(
        download_alpaca_etb,
        reindex_like=reindex_like, is_intraday=False).loc["EasyToBorrow"]

    return etb.fillna(0).astype(bool)

def collect_ibkr_shortable_shares(
    countries: Union[list[str], str] = None
    ) -> dict[str, str]:
    """
    Collect Interactive Brokers shortable shares data and save to database.

    Data is organized by country and updated every 15 minutes. Historical
    data is available from April 2018. Detailed intraday data as well as
    aggregated daily data will be saved to the database.

    Parameters
    ----------
    countries : list of str, optional
        limit to these countries (pass '?' or any invalid country to see
        available countries)

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short
    """
    params = {}
    if countries:
        params["countries"] = countries
    response = houston.post("/fundamental/ibkr/stockloan/shares", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_ibkr_shortable_shares(*args, **kwargs):
    return json_to_cli(collect_ibkr_shortable_shares, *args, **kwargs)

def collect_ibkr_borrow_fees(
    countries: Union[list[str], str] = None
    ) -> dict[str, str]:
    """
    Collect Interactive Brokers borrow fees data and save to database.

    Data is organized by country. Historical data is available from April
    2018.

    Parameters
    ----------
    countries : list of str, optional
        limit to these countries (pass '?' or any invalid country to see
        available countries)

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short
    """
    params = {}
    if countries:
        params["countries"] = countries
    response = houston.post("/fundamental/ibkr/stockloan/fees", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_ibkr_borrow_fees(*args, **kwargs):
    return json_to_cli(collect_ibkr_borrow_fees, *args, **kwargs)

def collect_ibkr_margin_requirements(
    country: str
    ) -> dict[str, str]:
    """
    Collect Interactive Brokers margin requirements data and save to database.

    The `country` parameter refers to the country of the IBKR subsidiary
    where your account is located. (Margin requirements vary by IBKR
    subsidiary.) Note that this differs from the IBKR shortable shares
    or borrow fees APIs, where the `countries` parameter refers to the
    country of the security rather than the country of the account.

    Historical data is available from April 2018.

    Parameters
    ----------
    country : str, required
        the country of the IBKR subsidiary where your account is located (pass
        '?' or any invalid country to see available countries)

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * IBKR Margin Requirements: https://qrok.it/dl/qr/ibkr-margin
    """
    params = {}
    params["countries"] = country
    response = houston.post("/fundamental/ibkr/stockloan/margin", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_ibkr_margin_requirements(*args, **kwargs):
    return json_to_cli(collect_ibkr_margin_requirements, *args, **kwargs)

def download_ibkr_shortable_shares(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    aggregate: bool = False
    ) -> None:
    """
    Query intraday or daily Interactive Brokers shortable shares data from the
    local database and download to file.

    Intraday data timestamps are UTC.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    aggregate : bool
        if True, return aggregated daily data containing the min, max, mean,
        and last shortable share quantities per security per day. If False or
        omitted, return intraday data.

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short

    Examples
    --------
    Query shortable shares for a universe of Australian stocks:

    >>> f = io.StringIO()
    >>> download_ibkr_shortable_shares("asx_shortables.csv", universes=["asx-stk"])
    >>> shortables = pd.read_csv("asx_shortables.csv", parse_dates=["Date"])

    Query aggregated daily data instead:

    >>> f = io.StringIO()
    >>> download_ibkr_shortable_shares("asx_shortables.csv", universes=["asx-stk"], aggregate=True)
    >>> shortables = pd.read_csv("asx_shortables.csv", parse_dates=["Date"])
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
    if aggregate:
        params["aggregate"] = aggregate

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/ibkr/stockloan/shares.{0}".format(output), params=params,
                           timeout=60*5)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "no shortable shares data match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_ibkr_shortable_shares(*args, **kwargs):
    return json_to_cli(download_ibkr_shortable_shares, *args, **kwargs)

def download_ibkr_borrow_fees(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None
    ) -> None:
    """
    Query Interactive Brokers borrow fees from the local database and download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short

    Examples
    --------
    Query borrow fees for a universe of Australian stocks.

    >>> f = io.StringIO()
    >>> download_ibkr_borrow_fees("asx_borrow_fees.csv", universes=["asx-stk"])
    >>> borrow_fees = pd.read_csv("asx_borrow_fees.csv", parse_dates=["Date"])
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

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/ibkr/stockloan/fees.{0}".format(output), params=params,
                           timeout=60*5)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "no borrow fees data match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_ibkr_borrow_fees(*args, **kwargs):
    return json_to_cli(download_ibkr_borrow_fees, *args, **kwargs)

def download_ibkr_margin_requirements(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None
    ) -> None:
    """
    Query Interactive Brokers margin requirements from the local database and
    download to file.

    Only stocks with special margin requirements are included in the dataset.
    Default margin requirements apply to stocks that are omitted from the
    dataset. 0 in the dataset is a placeholder value that also indicates that
    default margin requirements apply.

    Margin requirements are expressed in percentages, as whole numbers, for
    example 50 means 50% margin requirement, which is equivalent to 0.5.

    Data timestamps are UTC.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * IBKR Margin Requirements: https://qrok.it/dl/qr/ibkr-margin

    Examples
    --------
    Query margin requirements for a universe of US stocks:

    >>> f = io.StringIO()
    >>> download_ibkr_margin_requirements("usa_margin_requirements.csv", universes=["usa-stk"])
    >>> margin = pd.read_csv("usa_margin_requirements.csv", parse_dates=["Date"])
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

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/ibkr/stockloan/margin.{0}".format(output), params=params,
                           timeout=60*5)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "no margin requirements data match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_ibkr_margin_requirements(*args, **kwargs):
    return json_to_cli(download_ibkr_margin_requirements, *args, **kwargs)

def _get_stockloan_data_reindexed_like(stockloan_func, reindex_like,
                                       time=None, is_intraday=True,
                                       aggregate=False, fields=None, shift=0):
    """
    Common base function for get_ibkr_shortable_shares_reindexed_like and
    get_ibkr_borrow_fees_reindexed_like and get_alpaca_etb_reindexed_like.

    Parameters
    ----------

    stockloan_func : func
        the download function for the stockloan data

    reindex_like : DataFrame
        the input DataFrame to conform the stockloan data to

    time : str
        the time of day for which to return stockloan data. Only
        applicable to intraday stockloan data

    is_intraday : bool
        whether the stockloan data is intraday or daily

    aggregate : bool
        whether to query aggregate data

    fields : list of str
        limit to these fields

    shift : int, optional
        shift values this many periods. By default, values are not shifted.
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    # Stockloan data is sparse but batched in monthly files, so start >1-month
    # before the reindex_like min date
    start_date -= pd.Timedelta(days=45)
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    f = six.StringIO()
    query_kwargs = dict(sids=sids, start_date=start_date, end_date=end_date)
    if aggregate:
        query_kwargs["aggregate"] = True
    stockloan_func(
        f, **query_kwargs)
    stockloan_data = pd.read_csv(f)
    stockloan_data["Date"] = pd.to_datetime(stockloan_data.Date, utc=is_intraday)

    if is_intraday:
        # Determine timezone, from:
        # - time param if provided
        # - else reindex_like.index.tz if set
        # - else component securities if all have same timezone
        timezone = None

        if time and " " in time:
            time, timezone = time.split(" ", 1)

            if reindex_like.index.tz and reindex_like.index.tz.zone != timezone:
                raise ParameterError((
                    "cannot use timezone {0} because reindex_like timezone is {1}, "
                    "these must match".format(timezone, reindex_like.index.tz.zone)))

        if not timezone:
            if reindex_like.index.tz:
                timezone = reindex_like.index.tz.zone
            else:
                # try to infer from component securities
                f = six.StringIO()
                download_master_file(f, sids=list(stockloan_data.Sid.unique()),
                                     fields=["Timezone"])
                security_timezones = pd.read_csv(f, index_col="Sid")
                security_timezones = list(security_timezones.Timezone.unique())
                if len(security_timezones) > 1:
                    raise ParameterError(
                        "no timezone specified and cannot infer because multiple timezones are "
                        "present in data, please specify timezone (timezones in data: {0})".format(
                        ", ".join(security_timezones)))
                timezone = security_timezones[0]

        # Create an index of `reindex_like` dates at `time`
        if time:
            try:
                time = pd.Timestamp(time).time()
            except ValueError as e:
                raise ParameterError("could not parse time '{0}': {1}".format(
                    time, str(e)))
            index_at_time = pd.Index(reindex_like.index.to_series().apply(
                lambda x: datetime.datetime.combine(x, time)))
        else:
            index_at_time = reindex_like.index

        if not index_at_time.tz:
            index_at_time = index_at_time.tz_localize(timezone)

        index_at_time = index_at_time.tz_convert("UTC")

    # For daily stockloan data, just use the input DataFrame index
    else:
        index_at_time = reindex_like.index
        if index_at_time.tz:
            index_at_time = index_at_time.tz_localize(None)

    stockloan_data = stockloan_data.pivot(index="Sid",columns="Date").T

    all_fields = {}
    for fieldname in stockloan_data.index.get_level_values(0).unique():

        if fields and fieldname not in fields:
            continue

        field = stockloan_data.loc[fieldname]

        # Create a unioned index of requested times and stockloan data timestamps
        unioned_idx = index_at_time.union(field.index)

        # performance boost: don't need to reindex and ffill earlier than the
        # stockloan data start date
        unioned_idx = unioned_idx[
            unioned_idx >= stockloan_data.index.get_level_values("Date").min()]

        field = field.reindex(
            index=unioned_idx, columns=reindex_like.columns)

        field = field.ffill()

        if shift and not is_intraday:
            # shift, now that we've forward-filled; do this before reindexing
            # so the first day isn't nan
            field = field.shift(shift)

        # Keep only the requested times, now that we've ffilled
        field = field.reindex(index=index_at_time)

        if shift and is_intraday:
            # shift, now that we've forward-filled and reindexed from
            # intraday to daily (leading values will be nan since we're
            # shifting after reindexing - this could be improved by
            # calculating index_at_time for the whole unioned index,
            # reindexing to that, then shifting, then reindexing to the
            # final index)
            field = field.shift(shift)

        # Replace index_at_time with the original reindex_like index (this needs
        # to be done because index_at_time is tz-aware and reindex_like may not
        # be, and because index_at_time uses the requested `time` if provided.
        # Since index_at_time was derived from reindex_like.index and has the
        # same shape, it is safe to replace it.)
        field.index = reindex_like.index

        all_fields[fieldname] = field

    stockloan_data = pd.concat(all_fields, names=["Field", "Date"])

    return stockloan_data

def get_ibkr_shortable_shares_reindexed_like(
    reindex_like: 'pd.DataFrame',
    aggregate: bool = False,
    time: str = None,
    fields: Union[list[str], str] = None,
    shift: int = 0
    ) -> 'pd.DataFrame':
    """
    Return a DataFrame of Interactive Brokers shortable shares, reindexed to
    match the index (dates) and columns (sids) of `reindex_like`.

    If `aggregate=False` (the default), query intraday shortable shares data
    and return a DataFrame of the quantity of shortable shares as of
    the time of day specified by `time`.

    If `aggregate=True`, query shortable shares data aggregated by security and
    date and return a multiindex DataFrame with levels (Field, Date).

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    aggregate : bool
        if True, query data aggregated by security and date and return a multiindex
        (Field, Date) DataFrame. If False (the default), query intraday data and
        return a DataFrame of shortable share quantities (with a single-level Date
        index) as of the time of day specified by `time`.

    time : str (HH:MM:SS[ TZ]), optional
        return shortable shares as of this time of day. Only applicable if
        `aggregate=False`. If omitted, shortable shares will be returned as
        of the times of day in `reindex_like`'s DatetimeIndex. (Note that for
        a DatetimeIndex containing dates only, the time is 00:00:00, meaning
        shortable shares will be returned as of midnight at the start of the
        day.) A time and timezone can be passed as a space-separated string
        (e.g. "09:30:00 America/New_York"). If timezone is omitted, the timezone
        of `reindex_like`'s DatetimeIndex will be used; if `reindex_like`'s
        timezone is not set, the timezone will be inferred from the component
        securities, if all securities share the same timezone.

    fields : list of str, optional
        limit to these fields. Only applicable if `aggregate=True`. If omitted,
        all aggregate fields are included. Available fields are MinQuantity,
        MaxQuantity, MeanQuantity, and LastQuantity.

    shift : int, optional
        shift shortable shares this many periods. For example, `shift=1` will
        return the previous day's shortable shares. By default, values are not
        shifted, meaning the values reflect the current day's shortable shares.

    Returns
    -------
    DataFrame
        a DataFrame of shortable shares, shaped like the input DataFrame

    Notes
    -----
    Usage Guide:

    * IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short

    Examples
    --------
    Get shortable shares as of midnight for a DataFrame of US stocks:

    >>> closes = prices.loc["Close"]
    >>> shortables = get_ibkr_shortable_shares_reindexed_like(closes)

    Get shortable shares as of 9:20 AM for a DataFrame of US stocks (timezone
    inferred from component stocks):

    >>> closes = prices.loc["Close"]
    >>> shortables = get_ibkr_shortable_shares_reindexed_like(closes, time="09:20:00")

    Get shortable shares as of 9:20 AM New York time for a multi-timezone DataFrame
    of stocks:

    >>> closes = prices.loc["Close"]
    >>> shortables = get_ibkr_shortable_shares_reindexed_like(closes, time="09:20:00 America/New_York")

    Get aggregate shortable shares data for a DataFrame of US stocks:

    >>> closes = prices.loc["Close"]
    >>> shortables = get_ibkr_shortable_shares_reindexed_like(closes, aggregate=True)
    >>> min_quantities = shortables.loc["MinQuantity"]
    >>> max_quantities = shortables.loc["MaxQuantity"]
    >>> mean_quantities = shortables.loc["MeanQuantity"]
    >>> last_quantities = shortables.loc["LastQuantity"]
    """
    if aggregate and time:
        raise ParameterError("the time argument is only supported if aggregate=False")

    if fields and not aggregate:
        raise ParameterError("the fields parameter is only supported if aggregate=True")

    if fields and not isinstance(fields, (list, tuple)):
        fields = [fields]

    if fields:
        known_fields = ["MinQuantity", "MaxQuantity", "MeanQuantity", "LastQuantity"]
        unknown_fields = set(fields) - set(known_fields)
        if unknown_fields:
            raise ParameterError(
                f"invalid fields: {', '.join(unknown_fields)} "
                f"(valid fields are {', '.join(known_fields)}")
    shortable_shares = _get_stockloan_data_reindexed_like(
        download_ibkr_shortable_shares,
        reindex_like=reindex_like,
        time=time,
        aggregate=aggregate,
        is_intraday=not aggregate,
        fields=fields,
        shift=shift)

    # fillna(0) where date > 2018-04-15, the data start date (NaNs after that
    # date indicate no shortable shares, NaNs before that date indicate don't
    # know)
    if aggregate:
        fieldnames = shortable_shares.index.get_level_values("Field").unique()
    else:
        fieldnames = ["Quantity"]

    all_fields = {}
    data_start_date = os.environ.get("STOCKLOAN_DATA_START_DATE", "2018-04-15")
    for fieldname in fieldnames:
        field = shortable_shares.loc[fieldname]
        after_start_date_selector = field.index > data_start_date
        field.loc[after_start_date_selector, :] = field.loc[
        after_start_date_selector].fillna(0)
        all_fields[fieldname] = field

    if not aggregate:
        return all_fields["Quantity"]

    import pandas as pd
    shortable_shares = pd.concat(all_fields, names=["Field", "Date"])
    return shortable_shares

def get_ibkr_borrow_fees_reindexed_like(
    reindex_like: 'pd.DataFrame',
    shift: int = 0
    ) -> 'pd.DataFrame':
    """
    Return a DataFrame of Interactive Brokers borrow fees, reindexed to match
    the index (dates) and columns (sids) of `reindex_like`.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    shift : int, optional
        shift borrow fees this many periods. For example, `shift=1` will
        return the previous day's borrow fees. By default, values are not
        shifted, meaning the values reflect the current day's borrow fees.

    Returns
    -------
    DataFrame
        a DataFrame of borrow fees, shaped like the input DataFrame

    Notes
    -----
    Usage Guide:

    * IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short

    Examples
    --------
    Get borrow fees as of midnight for a DataFrame of US stocks:

    >>> closes = prices.loc["Close"]
    >>> borrow_fees = get_ibkr_borrow_fees_reindexed_like(closes)
    """
    return _get_stockloan_data_reindexed_like(
        download_ibkr_borrow_fees,
        reindex_like=reindex_like, is_intraday=False, shift=shift).loc["FeeRate"]

def get_ibkr_margin_requirements_reindexed_like(
    reindex_like: 'pd.DataFrame',
    time: str = None,
    shift: int = 0
    ) -> 'pd.DataFrame':
    """
    Return a multiindex (Field, Date) DataFrame of Interactive Brokers margin
    requirements, reindexed to match the index (dates) and columns (sids) of
    `reindex_like`.

    Returned fields are LongInitialMargin, LongMaintenanceMargin,
    ShortInitialMargin, and ShortMaintenanceMargin. Margin requirements
    are expressed in percentages, as whole numbers, for example 50 means
    50% margin requirement, which is equivalent to 0.5.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    time : str (HH:MM:SS[ TZ]), optional
        return margin requirements as of this time of day. If omitted, margin
        requirements will be returned as of the times of day in `reindex_like`'s
        DatetimeIndex. (Note that for a DatetimeIndex containing dates only,
        the time is 00:00:00, meaning margin requirements will be returned as
        of midnight at the start of the day.) A time and timezone can be passed
        as a space-separated string (e.g. "09:30:00 America/New_York"). If
        timezone is omitted, the timezone of `reindex_like`'s DatetimeIndex
        will be used; if `reindex_like`'s timezone is not set, the timezone
        will be inferred from the component securities, if all securities
        share the same timezone.

    shift : int, optional
        shift margin requirements this many periods. For example, `shift=1` will
        return the previous day's margin requirements. By default, values are not
        shifted, meaning the values reflect the current day's margin requirements.

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of margin requirements, shaped
        like the input DataFrame

    Notes
    -----
    Usage Guide:

    * IBKR Margin Requirements: https://qrok.it/dl/qr/ibkr-margin

    Examples
    --------
    Get margin requirements as of midnight for a DataFrame of US stocks and
    calculate the greater of ShortInitialMargin and ShortMaintenanceMargin:

    >>> closes = prices.loc["Close"]
    >>> margin_requirements = get_ibkr_margin_requirements_reindexed_like(closes)
    >>> short_initial_margins = margin_requirements.loc["ShortInitialMargin"]
    >>> short_maintenance_margins = margin_requirements.loc["ShortMaintenanceMargin"]
    >>> short_margins = short_initial_margins.where(
        short_initial_margins > short_maintenance_margins,
        short_maintenance_margins)

    Get margin requirements as of 4:30 PM for a DataFrame of US stocks
    (timezone inferred from component stocks):

    >>> closes = prices.loc["Close"]
    >>> margin_requirements = get_ibkr_margin_requirements_reindexed_like(
        closes, time="16:30:00")

    Get margin requirements as of 4:30 PM New York time for a multi-timezone
    DataFrame of stocks:

    >>> closes = prices.loc["Close"]
    >>> margin_requirements = get_ibkr_margin_requirements_reindexed_like(
        closes, time="16:30:00 America/New_York")
    """
    import pandas as pd
    try:
        margin_requirements = _get_stockloan_data_reindexed_like(
            download_ibkr_margin_requirements,
            reindex_like=reindex_like, time=time, is_intraday=True,
            shift=shift)
    except NoFundamentalData:
        import numpy as np
        multiidx = pd.MultiIndex.from_product(
            (("LongInitialMargin",
            "LongMaintenanceMargin",
            "ShortInitialMargin",
            "ShortMaintenanceMargin"), reindex_like.index),
            names=["Field", "Date"])
        margin_requirements = pd.DataFrame(
            np.nan,
            index=multiidx,
            columns=reindex_like.columns)

    # fillna(0) where date > 2018-04-15, the data start date (NaNs after that
    # date indicate no shortable shares, NaNs before that date indicate don't
    # know)
    data_start_date = os.environ.get("STOCKLOAN_DATA_START_DATE", "2018-04-15")
    all_fields = {}
    for fieldname in margin_requirements.index.get_level_values("Field").unique():
        field = margin_requirements.loc[fieldname]
        after_start_date_selector = field.index > data_start_date
        field.loc[after_start_date_selector, :] = field.loc[
            after_start_date_selector].fillna(0)
        all_fields[fieldname] = field

    margin_requirements = pd.concat(all_fields, names=["Field", "Date"])

    return margin_requirements

def download_reuters_financials(codes, filepath_or_buffer=None, output="csv",
                                start_date=None, end_date=None,
                                universes=None, sids=None,
                                exclude_universes=None, exclude_sids=None,
                                interim=False, exclude_restatements=False, fields=None):
    """
    Query financial statements from the Reuters financials database and
    download to file.

    DEPRECATED. This data is no longer available from Interactive Brokers. Only
    data that was previously saved to the local database can be queried.

    Parameters
    ----------
    codes : list of str, required
        the Chart of Account (COA) code(s) to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json or csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to statements on or after this fiscal period end date

    end_date : str (YYYY-MM-DD), optional
        limit to statements on or before this fiscal period end date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

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
    (identified by sid) and exclude restatements:

    >>> download_reuters_financials(["NINC"], f, sids=["FIBBG123456", "FIBBG234567"],
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
    if sids:
        params["sids"] = sids
    if exclude_universes:
        params["exclude_universes"] = exclude_universes
    if exclude_sids:
        params["exclude_sids"] = exclude_sids
    if interim:
        params["interim"] = interim
    if exclude_restatements:
        params["exclude_restatements"] = exclude_restatements
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/reuters/financials.{0}".format(output), params=params,
                           timeout=60*15)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        if "no financial statements match the query parameters" in repr(e).lower():
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_reuters_financials(*args, **kwargs):
    return json_to_cli(download_reuters_financials, *args, **kwargs)

def get_reuters_financials_reindexed_like(reindex_like, coa_codes, fields=["Amount"],
                           interim=False, exclude_restatements=False, max_lag=None):
    """
    Return a multiindex (CoaCode, Field, Date) DataFrame of point-in-time
    Reuters financial statements for one or more Chart of Account (COA)
    codes, reindexed to match the index (dates) and columns (sids) of
    `reindex_like`. Financial values are forward-filled in order to provide
    the latest reading at any given date. Financials are indexed to the
    SourceDate field, i.e. the date on which the financial statement was
    released. SourceDate is shifted forward 1 day to avoid lookahead bias.

    DEPRECATED. This data is no longer available from Interactive Brokers. Only
    data that was previously saved to the local database can be queried.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    # Since financial reports are sparse, start well before the reindex_like
    # min date
    start_date -= pd.Timedelta(days=365+180)
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    if not isinstance(fields, (list,tuple)):
        fields = [fields]

    if not isinstance(coa_codes, (list, tuple)):
        coa_codes = [coa_codes]

    f = six.StringIO()
    download_reuters_financials(
        coa_codes, f, sids=sids, start_date=start_date, end_date=end_date,
        fields=fields, interim=interim, exclude_restatements=exclude_restatements)
    financials = pd.read_csv(
        f, parse_dates=["SourceDate","FiscalPeriodEndDate"])

    # Rename SourceDate to match price history index name
    financials = financials.rename(columns={"SourceDate": "Date"})

    # Drop any fields we don't need
    needed_fields = set(fields)
    needed_fields.update(set(("Sid", "Date", "CoaCode")))
    if max_lag:
        needed_fields.add("FiscalPeriodEndDate")
    unneeded_fields = set(financials.columns) - needed_fields
    if unneeded_fields:
        financials = financials.drop(unneeded_fields, axis=1)

    # if reindex_like.index is tz-aware, make financials tz-aware so they can
    # be joined (tz-aware or tz-naive are both fine, as SourceDate represents
    # dates which are assumed to be in the local timezone of the reported
    # company)
    if reindex_like.index.tz:
        financials["Date"] = financials.Date.dt.tz_localize(reindex_like.index.tz.zone)

    deduped_source_dates = financials.Date.drop_duplicates()

    # Create a unioned index of input DataFrame and statement SourceDates
    union_date_idx = reindex_like.index.union(pd.DatetimeIndex(deduped_source_dates)).sort_values()

    all_financials = {}
    for code in coa_codes:
        financials_for_code = financials.loc[financials.CoaCode == code]
        if financials_for_code.empty:
            continue
        if "CoaCode" not in fields:
            financials_for_code = financials_for_code.drop("CoaCode", axis=1)
        # There might be duplicate SourceDates if a company announced
        # reports for several fiscal periods at once. In this case we keep
        # only the last value (i.e. latest fiscal period)
        financials_for_code = financials_for_code.drop_duplicates(subset=["Sid", "Date"], keep="last")
        financials_for_code = financials_for_code.pivot(index="Sid",columns="Date").T
        multiidx = pd.MultiIndex.from_product(
            (financials_for_code.index.get_level_values(0).unique(), union_date_idx),
            names=["Field", "Date"])
        financials_for_code = financials_for_code.reindex(index=multiidx, columns=reindex_like.columns)

        # financial values are sparse so ffill (one field at a time)
        all_fields_for_code = {}
        for field in financials_for_code.index.get_level_values("Field").unique():
            field_for_code = financials_for_code.loc[field].ffill()

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
                               universes=None, sids=None,
                               exclude_universes=None, exclude_sids=None,
                               period_types=None, fields=None):
    """
    Query estimates and actuals from the Reuters estimates database and
    download to file.

    DEPRECATED. This data is no longer available from Interactive Brokers. Only
    data that was previously saved to the local database can be queried.

    Parameters
    ----------
    codes : list of str, required
        the indicator code(s) to query

    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json or csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to estimates and actuals on or after this fiscal period end date

    end_date : str (YYYY-MM-DD), optional
        limit to estimates and actuals on or before this fiscal period end date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

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
    >>> estimates = pd.read_csv(f, parse_dates=["FiscalPeriodEndDate", "UpdatedDate"])
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
    if sids:
        params["sids"] = sids
    if exclude_universes:
        params["exclude_universes"] = exclude_universes
    if exclude_sids:
        params["exclude_sids"] = exclude_sids
    if period_types:
        params["period_types"] = period_types
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/reuters/estimates.{0}".format(output), params=params,
                           timeout=60*15)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        if "no estimates match the query parameters" in repr(e).lower():
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_reuters_estimates(*args, **kwargs):
    return json_to_cli(download_reuters_estimates, *args, **kwargs)

def get_reuters_estimates_reindexed_like(reindex_like, codes, fields=["Actual"],
                                         period_types=["Q"], ffill=True, shift=True,
                                         max_lag=None):
    """
    Return a multiindex (Indicator, Field, Date) DataFrame of point-in-time
    Reuters estimates and actuals for one or more indicator codes, reindexed
    to match the index (dates) and columns (sids) of `reindex_like`.
    Estimates and actuals are forward-filled in order to provide the latest
    reading at any given date, indexed to the UpdatedDate field (which usually
    corresponds to the announcement date). By default UpdatedDate is shifted
    forward 1 day to avoid lookahead bias.

    DEPRECATED. This data is no longer available from Interactive Brokers. Only
    data that was previously saved to the local database can be queried.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
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

    ffill : bool
        forward-fill values in the resulting DataFrame so that each date reflects
        the latest available value as of that date. If False, values appear only
        on the first date they were available, followed by NaNs. Default True.

    shift : bool
        shift values forward one day from the UpdatedDate to avoid lookahead bias.
        Shifting forward one day may be overly cautious as announcements may occur
        early in the day, for example before the market open, and thus may be actionable
        the same day. Set 'shift=False' to index values to the UpdatedDate rather
        than the day after, in which case you may also want to query UpdatedDate
        (timestamped in UTC) and use it to selectively shift estimates
        and actuals yourself. Default True.

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

    Query the UpdatedDate field without shifting or forward-filling, convert UTC
    to New York time, and determine whether the earnings were announced before 9 AM
    or after 4 PM:

    >>> estimates = get_reuters_estimates_reindexed_like(
                        closes, codes="EPS", fields="UpdatedDate", ffill=False, shift=False)
    >>> announce_dates = estimates.loc["EPS"].loc["UpdatedDate"]
    >>> announce_hours = announce_dates.stack(dropna=False).dt.tz_localize("UTC").dt.tz_convert("America/New_York").dt.hour.unstack()
    >>> announced_before_market_opens = announce_hours < 9
    >>> announced_after_market_closes = announce_hours >= 16
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    # Since financial reports are sparse, start well before the reindex_like
    # min date
    start_date -= pd.Timedelta(days=365+180)
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    if not isinstance(fields, (list,tuple)):
        fields = [fields]

    if period_types and not isinstance(period_types, (list, tuple)):
        period_types = [period_types]

    if not isinstance(codes, (list, tuple)):
        codes = [codes]

    f = six.StringIO()
    query_fields = list(fields) # copy fields on Py2 or 3: https://stackoverflow.com/a/2612815/417414
    if "UpdatedDate" not in query_fields:
        query_fields.append("UpdatedDate")
    download_reuters_estimates(
        codes, f, sids=sids, start_date=start_date, end_date=end_date,
        fields=query_fields, period_types=period_types)
    parse_dates = ["UpdatedDate"]
    if "FiscalPeriodEndDate" in fields or max_lag:
        parse_dates.append("FiscalPeriodEndDate")
    if "AnnounceDate" in fields:
        parse_dates.append("AnnounceDate")
    estimates = pd.read_csv(
        f, parse_dates=parse_dates)

    # Drop records with no actuals
    estimates = estimates.loc[estimates.UpdatedDate.notnull()]

    # Convert UTC UpdatedDate to security timezone, and cast to date for
    # index
    f = six.StringIO()
    download_master_file(f, sids=list(estimates.Sid.unique()),
                         fields=["Timezone"])
    timezones = pd.read_csv(f, index_col="Sid")
    estimates = estimates.join(timezones, on="Sid")
    if estimates.Timezone.isnull().any():
        sids_missing_timezones = list(estimates.Sid[estimates.Timezone.isnull()].unique())
        raise MissingData("timezones are missing for some sids so cannot convert UTC "
                          "estimates to timezone of security (sids missing timezone: {0})".format(
                              ",".join(sids_missing_timezones)
                          ))

    # If only 1 timezone in data, use a faster method
    if len(estimates.Timezone.unique()) == 1:
        timezone = list(estimates.Timezone.unique())[0]
        estimates["Date"] = pd.to_datetime(estimates.UpdatedDate.values).tz_localize("UTC").tz_convert(timezone)
    else:
        estimates["Date"] = estimates.apply(lambda row: row.UpdatedDate.tz_localize("UTC").tz_convert(row.Timezone), axis=1)

    # Convert to dates (i.e. time = 00:00:00)
    estimates["Date"] = pd.to_datetime(estimates.Date.apply(
        lambda x: datetime.datetime.combine(x, pd.Timestamp("00:00:00").time())))

    # Drop any fields we don't need
    needed_fields = set(fields)
    needed_fields.update(set(("Sid", "Date", "Indicator")))
    if max_lag:
        needed_fields.add("FiscalPeriodEndDate")

    unneeded_fields = set(estimates.columns) - needed_fields
    if unneeded_fields:
        estimates = estimates.drop(unneeded_fields, axis=1)

    # if reindex_like.index is tz-aware, make estimates tz-aware so they can
    # be joined (tz-aware or tz-naive are both fine, as UpdatedDate was
    # already converted above to the local timezone of the reported company)
    if reindex_like.index.tz:
        estimates["Date"] = estimates.Date.dt.tz_localize(reindex_like.index.tz.zone)

    deduped_announce_dates = estimates.Date.drop_duplicates()

    # Create a unioned index of input DataFrame and UpdatedDate
    union_date_idx = reindex_like.index.union(pd.DatetimeIndex(deduped_announce_dates)).sort_values()

    all_estimates = {}
    for code in codes:
        estimates_for_code = estimates.loc[estimates.Indicator == code]
        if estimates_for_code.empty:
            continue
        if "Indicator" not in fields:
            estimates_for_code = estimates_for_code.drop("Indicator", axis=1)
        # There might be duplicate UpdatedDates if a company announced
        # reports for several fiscal periods at once. In this case we keep
        # only the last value (i.e. latest fiscal period)
        estimates_for_code = estimates_for_code.drop_duplicates(subset=["Sid","Date"], keep="last")
        estimates_for_code = estimates_for_code.pivot(index="Sid",columns="Date").T
        multiidx = pd.MultiIndex.from_product(
            (estimates_for_code.index.get_level_values(0).unique(), union_date_idx),
            names=["Field", "Date"])
        estimates_for_code = estimates_for_code.reindex(index=multiidx, columns=reindex_like.columns)

        # estimates are sparse so ffill (one field at a time)
        all_fields_for_code = {}
        for field in estimates_for_code.index.get_level_values("Field").unique():
            field_for_code = estimates_for_code.loc[field]

            if ffill:
                field_for_code = field_for_code.ffill()

            # Shift to avoid lookahead bias
            if shift:
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

def collect_sharadar_fundamentals(
    country: Literal["US", "FREE"] = "US"
    ) -> dict[str, str]:
    """
    Collect fundamental data from Sharadar and save to database.

    Parameters
    ----------
    country : str, required
        country to collect fundamentals for. Possible choices: US, FREE

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Sharadar Fundamentals: https://qrok.it/dl/qr/sharadar-fundamentals
    """
    params = {}
    if country:
        params["country"] = country
    response = houston.post("/fundamental/sharadar/fundamentals", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_sharadar_fundamentals(*args, **kwargs):
    return json_to_cli(collect_sharadar_fundamentals, *args, **kwargs)

def collect_sharadar_insiders(
    country: Literal["US", "FREE"] = "US"
    ) -> dict[str, str]:
    """
    Collect insider holdings data from Sharadar and save to database.

    Parameters
    ----------
    country : str, required
        country to collect insider holdings data for. Possible choices:
        US, FREE

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Sharadar Insiders: https://qrok.it/dl/qr/sharadar-insiders
    """
    params = {}
    if country:
        params["country"] = country
    response = houston.post("/fundamental/sharadar/insiders", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_sharadar_insiders(*args, **kwargs):
    return json_to_cli(collect_sharadar_insiders, *args, **kwargs)

def collect_sharadar_institutions(
    country: Literal["US", "FREE"] = "US",
    detail: bool = False
    ) -> dict[str, str]:
    """
    Collect institutional investor data from Sharadar and save to database.

    Parameters
    ----------
    country : str, required
        country to collect institutional investor data for. Possible
        choices: US, FREE

    detail : bool
        if true, collect detailed investor data (separate record per
        investor per security per quarter). If false (the default),
        collect data aggregated by security (separate record per
        security per quarter).

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Sharadar Institutions: https://qrok.it/dl/qr/sharadar-institutions
    """
    params = {}
    if country:
        params["country"] = country
    if detail:
        params["detail"] = detail
    response = houston.post("/fundamental/sharadar/institutions", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_sharadar_institutions(*args, **kwargs):
    return json_to_cli(collect_sharadar_institutions, *args, **kwargs)

def collect_sharadar_sec8(
    country: Literal["US", "FREE"] = "US"
    ) -> dict[str, str]:
    """
    Collect SEC Form 8-K events from Sharadar and save to database.

    Parameters
    ----------
    country : str, required
        country to collect events data for. Possible choices: US, FREE

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Sharadar SEC Form 8-K: https://qrok.it/dl/qr/sharadar-sec8
    """
    params = {}
    if country:
        params["country"] = country
    response = houston.post("/fundamental/sharadar/sec8", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_sharadar_sec8(*args, **kwargs):
    return json_to_cli(collect_sharadar_sec8, *args, **kwargs)

def collect_sharadar_sp500(
    country: Literal["US", "FREE"] = "US"
    ) -> dict[str, str]:
    """
    Collect historical S&P 500 index constituents from Sharadar and save to
    database.

    Parameters
    ----------
    country : str, required
        country to collect S&P 500 constituents data for. Possible
        choices: US, FREE

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Sharadar S&P 500: https://qrok.it/dl/qr/sharadar-sp500
    """
    params = {}
    if country:
        params["country"] = country
    response = houston.post("/fundamental/sharadar/sp500", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_sharadar_sp500(*args, **kwargs):
    return json_to_cli(collect_sharadar_sp500, *args, **kwargs)

SharadarFundamentalsField = Literal[
    'ACCOCI',
    'ASSETS',
    'ASSETSAVG',
    'ASSETSC',
    'ASSETSNC',
    'ASSETTURNOVER',
    'BVPS',
    'CALENDARDATE',
    'CAPEX',
    'CASHNEQ',
    'CASHNEQUSD',
    'CONSOLINC',
    'COR',
    'CURRENTRATIO',
    'DATEKEY',
    'DE',
    'DEBT',
    'DEBTC',
    'DEBTNC',
    'DEBTUSD',
    'DEFERREDREV',
    'DEPAMOR',
    'DEPOSITS',
    'DIMENSION',
    'DIVYIELD',
    'DPS',
    'EBIT',
    'EBITDA',
    'EBITDAMARGIN',
    'EBITDAUSD',
    'EBITUSD',
    'EBT',
    'EPS',
    'EPSDIL',
    'EPSUSD',
    'EQUITY',
    'EQUITYAVG',
    'EQUITYUSD',
    'EV',
    'EVEBIT',
    'EVEBITDA',
    'FCF',
    'FCFPS',
    'FXUSD',
    'GP',
    'GROSSMARGIN',
    'INTANGIBLES',
    'INTEXP',
    'INVCAP',
    'INVCAPAVG',
    'INVENTORY',
    'INVESTMENTS',
    'INVESTMENTSC',
    'INVESTMENTSNC',
    'LASTUPDATED',
    'LIABILITIES',
    'LIABILITIESC',
    'LIABILITIESNC',
    'MARKETCAP',
    'NCF',
    'NCFBUS',
    'NCFCOMMON',
    'NCFDEBT',
    'NCFDIV',
    'NCFF',
    'NCFI',
    'NCFINV',
    'NCFO',
    'NCFX',
    'NETINC',
    'NETINCCMN',
    'NETINCCMNUSD',
    'NETINCDIS',
    'NETINCNCI',
    'NETMARGIN',
    'OPEX',
    'OPINC',
    'PAYABLES',
    'PAYOUTRATIO',
    'PB',
    'PE',
    'PE1',
    'PPNENET',
    'PREFDIVIS',
    'PRICE',
    'PS',
    'PS1',
    'RECEIVABLES',
    'REPORTPERIOD',
    'RETEARN',
    'REVENUE',
    'REVENUEUSD',
    'RND',
    'ROA',
    'ROE',
    'ROIC',
    'ROS',
    'SBCOMP',
    'SGNA',
    'SHAREFACTOR',
    'SHARESBAS',
    'SHARESWA',
    'SHARESWADIL',
    'SPS',
    'TANGIBLES',
    'TAXASSETS',
    'TAXEXP',
    'TAXLIABILITIES',
    'TBVPS',
    'TICKER',
    'WORKINGCAPITAL']

def download_sharadar_fundamentals(
    filepath_or_buffer: FilepathOrBuffer = None,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    dimensions: Union[Literal[
        "ARQ", "ARY", "ART", "MRQ", "MRY", "MRT"],
        list[str]] = None,
    fields: Union[SharadarFundamentalsField, list[str]] = None,
    output: Literal["csv", "json"] = "csv"
    ) -> None:
    """
    Query Sharadar fundamentals from the local database and download
    to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to fundamentals on or after this fiscal period end date

    end_date : str (YYYY-MM-DD), optional
        limit to fundamentals on or before this fiscal period end date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    dimensions : list of str, optional
        limit to these dimensions. Possible choices: ARQ, ARY, ART, MRQ,
        MRY, MRT. AR=As Reported, MR=Most Recent Reported, Q=Quarterly,
        Y=Annual, T=Trailing Twelve Month.

    fields : list of str, optional
        only return these fields (pass '?' or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Sharadar Fundamentals: https://qrok.it/dl/qr/sharadar-fundamentals

    Examples
    --------
    Query as-reported trailing twelve month (ART) fundamentals for all indicators
    for a particular sid, then load the CSV into Pandas:

    >>> download_sharadar_fundamentals(filepath_or_buffer="aapl_fundamentals.csv",
                                       sids="FIBBG265598", dimensions="ART")
    >>> fundamentals = pd.read_csv("aapl_fundamentals.csv", parse_dates=["REPORTPERIOD", "DATEKEY", "CALENDARDATE"])

    Query as-reported quarterly (ARQ) fundamentals for select indicators for a
    universe:

    >>> download_sharadar_fundamentals(filepath_or_buffer="sharadar_fundamentals.csv",
                                       universes="usa-stk",
                                       dimensions="ARQ", fields=["REVENUE", "EPS"])
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
    if dimensions:
        params["dimensions"] = dimensions
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/sharadar/fundamentals.{0}".format(output), params=params,
                           timeout=60*15)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_sharadar_fundamentals(*args, **kwargs):
    return json_to_cli(download_sharadar_fundamentals, *args, **kwargs)

SharadarInsidersField = Literal[
    'DATEEXERCISABLE',
    'DIRECTORINDIRECT',
    'EXPIRATIONDATE',
    'FILINGDATE',
    'FORMTYPE',
    'ISDIRECTOR',
    'ISOFFICER',
    'ISSUERNAME',
    'ISTENPERCENTOWNER',
    'NATUREOFOWNERSHIP',
    'OFFICERTITLE',
    'OWNERNAME',
    'PRICEEXERCISABLE',
    'ROWNUM',
    'SECURITYADCODE',
    'SECURITYTITLE',
    'SHARESOWNEDBEFORETRANSACTION',
    'SHARESOWNEDFOLLOWINGTRANSACTION',
    'TICKER',
    'TRANSACTIONCODE',
    'TRANSACTIONDATE',
    'TRANSACTIONPRICEPERSHARE',
    'TRANSACTIONSHARES',
    'TRANSACTIONVALUE']

def download_sharadar_insiders(
    filepath_or_buffer: FilepathOrBuffer = None,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    fields: Union[SharadarInsidersField, list[str]] = None,
    output: Literal["csv", "json"] = "csv"
    ) -> None:
    """
    Query Sharadar insider holdings data from the local database and download
    to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this filing date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this filing date

    universes : list of str, optional
        limit to these universes

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

    Notes
    -----
    Usage Guide:

    * Sharadar Insiders: https://qrok.it/dl/qr/sharadar-insiders

    Examples
    --------
    Query insider holdings data for a particular sid, then load the CSV into Pandas:

    >>> download_sharadar_insiders(filepath_or_buffer="aapl_insiders.csv",
                                    sids="FIBBG000B9XRY4")
    >>> insiders = pd.read_csv("aapl_insiders.csv", parse_dates=["FILINGDATE", "TRANSACTIONDATE"])
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

    response = houston.get("/fundamental/sharadar/insiders.{0}".format(output), params=params,
                           timeout=60*15)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_sharadar_insiders(*args, **kwargs):
    return json_to_cli(download_sharadar_insiders, *args, **kwargs)

SharadarInstitutionsField = Literal[
    'CALENDARDATE',
    'CLLHOLDERS',
    'CLLUNITS',
    'CLLVALUE',
    'DBTHOLDERS',
    'DBTUNITS',
    'DBTVALUE',
    'FNDHOLDERS',
    'FNDUNITS',
    'FNDVALUE',
    'NAME',
    'PERCENTOFTOTAL',
    'PRFHOLDERS',
    'PRFUNITS',
    'PRFVALUE',
    'PUTHOLDERS',
    'PUTUNITS',
    'PUTVALUE',
    'SHRHOLDERS',
    'SHRUNITS',
    'SHRVALUE',
    'TICKER',
    'TOTALVALUE',
    'UNDHOLDERS',
    'UNDUNITS',
    'UNDVALUE',
    'WNTHOLDERS',
    'WNTUNITS',
    'WNTVALUE']

def download_sharadar_institutions(
    filepath_or_buffer: FilepathOrBuffer = None,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    detail: Union[list[str], str] = False,
    fields: Union[SharadarInstitutionsField, list[str]] = None,
    output: Literal["csv", "json"] = "csv"
    ) -> None:
    """
    Query Sharadar institutional investor data from the local database and
    download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this quarter end date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this quarter end date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    detail : bool
        if true, query detailed investor data (separate record per
        investor per security per quarter). If false (the default),
        query data aggregated by security (separate record per
        security per quarter).

    fields : list of str, optional
        only return these fields (pass '?' or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Sharadar Institutions: https://qrok.it/dl/qr/sharadar-institutions

    Examples
    --------
    Query institutional investor data aggregated by security and load the
    CSV into Pandas:

    >>> download_sharadar_institutions(filepath_or_buffer="institutions.csv",
                                        universes="usa-stk", start_date="2019-01-01")
    >>> institutions = pd.read_csv("institutions.csv", parse_dates=["CALENDARDATE"])
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
    if detail:
        params["detail"] = detail
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/sharadar/institutions.{0}".format(output), params=params,
                           timeout=60*15)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_sharadar_institutions(*args, **kwargs):
    return json_to_cli(download_sharadar_institutions, *args, **kwargs)

def download_sharadar_sec8(
    filepath_or_buffer: FilepathOrBuffer = None,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    event_codes: Union[list[int], int] = None,
    fields: Union[list[str], str] = None,
    output: Literal["csv", "json"] = "csv"
    ) -> None:
    """
    Query Sharadar SEC Form 8-K events data from the local database and
    download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this filing date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this filing date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    event_codes : list of int, optional
        limit to these event codes

    fields : list of str, optional
        only return these fields (pass '?' or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Sharadar SEC Form 8-K: https://qrok.it/dl/qr/sharadar-sec8

    Examples
    --------
    Query event code 13 (Bankruptcy) for a universe of securities and load
    into Pandas:

    >>> download_sharadar_sec8(filepath_or_buffer="bankruptcies.csv",
                                universes="usa-stk", event_codes=13)
    >>> bankruptcies = pd.read_csv("bankruptcies.csv", parse_dates=["DATE"])
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
    if event_codes:
        params["event_codes"] = event_codes
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/sharadar/sec8.{0}".format(output), params=params,
                           timeout=60*15)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_sharadar_sec8(*args, **kwargs):
    return json_to_cli(download_sharadar_sec8, *args, **kwargs)

def download_sharadar_sp500(
    filepath_or_buffer: FilepathOrBuffer = None,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    fields: Union[list[str], str] = None,
    output: Literal["csv", "json"] = "csv"
    ) -> None:
    """
    Query Sharadar S&P 500 index changes (additions and removals) from the
    local database and download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to index changes on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to index changes on or before this date

    universes : list of str, optional
        limit to these universes

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

    Notes
    -----
    Usage Guide:

    * Sharadar S&P 500: https://qrok.it/dl/qr/sharadar-sp500

    Examples
    --------
    Query S&P 500 index changes since 2010 and load into Pandas:

    >>> download_sharadar_sp500(filepath_or_buffer="sp500_changes.csv", start_date="2010-01-01")
    >>> sp500_changes = pd.read_csv("sp500_changes.csv", parse_dates=["DATE"])

    Get the current members of the S&P 500:

    >>> download_sharadar_sp500(filepath_or_buffer="sp500_changes.csv")
    >>> sp500_changes = pd.read_csv("sp500_changes.csv", parse_dates=["DATE"])
    >>> latest_changes = sp500_changes.drop_duplicates(subset="Sid", keep="last")
    >>> current_members = latest_changes[latest_changes.ACTION == "added"]
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

    response = houston.get("/fundamental/sharadar/sp500.{0}".format(output), params=params,
                           timeout=60*15)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "match the query parameters",
            "no free securities match",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_sharadar_sp500(*args, **kwargs):
    return json_to_cli(download_sharadar_sp500, *args, **kwargs)

def get_sharadar_fundamentals_reindexed_like(
    reindex_like: 'pd.DataFrame',
    fields: Union[SharadarFundamentalsField, list[str]] = None,
    dimension: Literal[
        "ARQ", "ARY", "ART", "MRQ", "MRY", "MRT"] = "ART",
    period_offset: int = 0
    ) -> 'pd.DataFrame':
    """
    Return a multiindex (Field, Date) DataFrame of point-in-time
    Sharadar fundamentals, reindexed to match the index (dates)
    and columns (sids) of `reindex_like`. Financial indicators are
    forward-filled in order to provide the latest reading at any given
    date. Indicators are indexed to the Sharadar DATEKEY field, i.e. the
    filing date. DATEKEY is shifted forward 1 day to avoid lookahead bias.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        including all fields. For faster performance, limiting fields to
        those needed is highly recommended, especially for large universes.

    dimension : str
        the dimension of the data. Defaults to As Reported Trailing Twelve
        Month (ART). Possible choices: ARQ, ARY, ART, MRQ,
        MRY, MRT. AR=As Reported, MR=Most Recent Reported, Q=Quarterly,
        Y=Annual, T=Trailing Twelve Month.

    period_offset : int, optional
        which fiscal period to return data for. If period_offset is 0 (the default),
        returns the most recent point-in-time fundamentals as of each date in
        `reindex_like`. If period_offset is -1, returns fundamentals for the prior
        fiscal period as of each date; if -2, two fiscal periods ago, etc. For
        quarterly and trailing-twelve-month dimensions, previous period means
        previous quarter, while for annual dimensions, previous period means
        previous year. Value should be a negative integer or 0.

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of fundamentals, shaped like
        the input DataFrame

    Notes
    -----
    Usage Guide:

    * Sharadar Fundamentals: https://qrok.it/dl/qr/sharadar-fundamentals

    Examples
    --------
    Query several trailing twelve month indicators using a DataFrame of
    historical prices:

    >>> closes = prices.loc["Close"]
    >>> fundamentals = get_sharadar_fundamentals_reindexed_like(closes, fields=["EPS", "REVENUE"])
    >>> eps = fundamentals.loc["EPS"]
    >>> revenue = fundamentals.loc["REVENUE"]

    Query quarterly book value per share using a DataFrame of historical prices:

    >>> closes = prices.loc["Close"]
    >>> fundamentals = get_sharadar_fundamentals_reindexed_like(closes, fields=["BVPS"],
                                                                 dimension="ARQ")
    >>> bvps = fundamentals.loc["BVPS"]

    Query outstanding shares using a DataFrame of historical prices:

    >>> closes = prices.loc["Close"]
    >>> fundamentals = get_sharadar_fundamentals_reindexed_like(closes,
                                                                fields=["SHARESWA"])
    >>> shares_out = fundamentals.loc["SHARESWA"]

    Query outstanding shares as of the previous fiscal period:

    >>> closes = prices.loc["Close"]
    >>> fundamentals = get_sharadar_fundamentals_reindexed_like(closes,
                                                                fields=["SHARESWA"].
                                                                period_offset=-1)
    >>> previous_shares_out = fundamentals.loc["SHARESWA"]
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    period_offset = period_offset or 0
    if period_offset > 0:
        raise ParameterError("period_offset must be a negative integer or 0")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    # Since financial reports are sparse, start well before the reindex_like
    # min date
    start_date -= pd.Timedelta(days=365+180)
    # If there's a period_offset, rewind the start date by a corresponding amount
    if period_offset < 0:
        if dimension.endswith("Y"):
            start_date -= pd.Timedelta(days=365 * abs(period_offset))
        else:
            start_date -= pd.Timedelta(days=92 * abs(period_offset))
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    if fields and not isinstance(fields, (list,tuple)):
        fields = [fields]

    f = six.StringIO()
    download_sharadar_fundamentals(
        filepath_or_buffer=f, sids=sids, start_date=start_date, end_date=end_date,
        fields=fields, dimensions=dimension)
    date_fields = ["DATEKEY"]
    if fields:
        for date_field in ("CALENDARDATE", "REPORTPERIOD"):
            if date_field in fields:
                date_fields.append(date_field)
    financials = pd.read_csv(
        f, parse_dates=date_fields)

    # Rename DATEKEY to match price history index name
    financials = financials.rename(columns={"DATEKEY": "Date"})

    # Drop any fields we don't need
    if fields:
        needed_fields = set(fields)
        needed_fields.update(set(("Sid", "Date")))
        unneeded_fields = set(financials.columns) - needed_fields
        if unneeded_fields:
            financials = financials.drop(unneeded_fields, axis=1)

    # if reindex_like.index is tz-aware, make financials tz-aware so they can
    # be joined (tz-aware or tz-naive are both fine, as DATEKEY represents
    # dates which are assumed to be in the local timezone of the reported
    # company)
    if reindex_like.index.tz:
        financials["Date"] = financials.Date.dt.tz_localize(reindex_like.index.tz.zone)

    deduped_datekeys = financials.Date.drop_duplicates()

    if period_offset != 0:
        # add a column to mark report dates
        financials["IsReportDate"] = 1.0

    # Create a unioned index of input DataFrame and statement DATEKEYs
    union_date_idx = reindex_like.index.union(pd.DatetimeIndex(deduped_datekeys)).sort_values()

    # There might be duplicate DATEKEYs if a company announced
    # reports for several fiscal periods at once. In this case we keep
    # only the last value (i.e. latest fiscal period)
    financials = financials.drop_duplicates(subset=["Sid", "Date"], keep="last")
    financials = financials.pivot(index="Sid",columns="Date").T
    multiidx = pd.MultiIndex.from_product(
        (financials.index.get_level_values(0).unique(), union_date_idx),
        names=["Field", "Date"])
    financials = financials.reindex(index=multiidx, columns=reindex_like.columns)

    # financial values are sparse so ffill (one field at a time)
    all_fields = {}
    if period_offset != 0:
        are_report_dates = financials.loc["IsReportDate"].notnull()

    for fieldname in financials.index.get_level_values("Field").unique():
        if fieldname == "IsReportDate":
            continue

        field = financials.loc[fieldname]

        for _ in range(abs(period_offset)):
            # to get the previous period, we forward-fill, shift, then keep
            # only the shifted values falling on report dates
            field = field.ffill().shift().where(are_report_dates)

        # forward-fill values
        field = field.ffill()

        # Shift to avoid lookahead bias
        field = field.shift()

        all_fields[fieldname] = field

    financials = pd.concat(all_fields, names=["Field", "Date"])

    # In cases the statements included dates not in the input
    # DataFrame, drop those now that we've ffilled
    extra_dates = union_date_idx.difference(reindex_like.index)
    if not extra_dates.empty:
        financials.drop(extra_dates, axis=0, level="Date", inplace=True)

    return financials

def get_sharadar_institutions_reindexed_like(
    reindex_like: 'pd.DataFrame',
    fields: Union[SharadarInstitutionsField, list[str]] = None,
    shift: int = 45
    ) -> 'pd.DataFrame':
    """
    Return a multiindex (Field, Date) DataFrame of Sharadar institutional
    investor data, reindexed to match the index (dates) and columns (sids) of
    `reindex_like`. Values are forward-filled in order to provide the latest
    reading at any given date. Data are indexed to the quarter end date.
    Because the reporting deadline is 45 days after the end of the quarter
    the values are shifted forward 45 calendar days by default (see the
    `shift` parameter to control this).

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        including all fields. For faster performance, limiting fields to
        those needed is highly recommended, especially for large universes.

    shift: int, optional
        shift the data forward this many period to account for the 45-day
        lag between the quarter end date and the reporting deadline. Defaults
        to 45.

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of institutional investor data,
        shaped like the input DataFrame

    Notes
    -----
    Usage Guide:

    * Sharadar Institutions: https://qrok.it/dl/qr/sharadar-institutions

    Examples
    --------
    Calculate institutional ownership as a percentage of total market cap:

    >>> closes = prices.loc["Close"]
    >>> insti = get_sharadar_institutions_reindexed_like(closes, fields="SHRVALUE")
    >>> insti_share_values = insti.loc["SHRVALUE"]
    >>> fundamentals = get_sharadar_fundamentals_reindexed_like(closes, dimension="ARQ", fields="MARKETCAP")
    >>> market_caps = fundamentals.loc["MARKETCAP"]
    >>> insti_pct = insti_share_values/market_caps
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    # Since financial reports are sparse, start well before the reindex_like
    # min date
    start_date -= pd.Timedelta(days=365+180)
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    if fields and not isinstance(fields, (list,tuple)):
        fields = [fields]

    f = six.StringIO()
    download_sharadar_institutions(
        filepath_or_buffer=f, sids=sids, start_date=start_date, end_date=end_date,
        fields=fields)
    institutions = pd.read_csv(
        f, parse_dates=["CALENDARDATE"])

    # Rename CALENDARDATE to match price history index name
    institutions = institutions.rename(columns={"CALENDARDATE": "Date"})

    # Drop any fields we don't need
    if fields:
        needed_fields = set(fields)
        needed_fields.update(set(("Sid", "Date")))
        unneeded_fields = set(institutions.columns) - needed_fields
        if unneeded_fields:
            institutions = institutions.drop(unneeded_fields, axis=1)

    # if reindex_like.index is tz-aware, make institutions tz-aware so they can
    # be joined
    if reindex_like.index.tz:
        institutions["Date"] = institutions.Date.dt.tz_localize(reindex_like.index.tz.zone)

    deduped_datekeys = institutions.Date.drop_duplicates()

    # Create a unioned index of input DataFrame and statement DATEKEYs
    union_date_idx = reindex_like.index.union(pd.DatetimeIndex(deduped_datekeys)).sort_values()

    # Interpolate calendar days into unioned index
    union_date_idx = pd.date_range(start=union_date_idx.min(), end=union_date_idx.max(),
                                   freq="D", tz=union_date_idx.tz)

    institutions = institutions.pivot(index="Sid",columns="Date").T
    multiidx = pd.MultiIndex.from_product(
        (institutions.index.get_level_values(0).unique(), union_date_idx),
        names=["Field", "Date"])
    institutions = institutions.reindex(index=multiidx, columns=reindex_like.columns)

    # values are sparse so ffill (one field at a time)
    all_fields = {}
    for fieldname in institutions.index.get_level_values("Field").unique():
        field = institutions.loc[fieldname].ffill()

        # Shift to avoid lookahead bias
        if shift:
            field = field.shift(shift)

        all_fields[fieldname] = field

    institutions = pd.concat(all_fields, names=["Field", "Date"])

    # In cases the institutions included dates not in the input
    # DataFrame, drop those now that we've ffilled
    extra_dates = union_date_idx.difference(reindex_like.index)
    if not extra_dates.empty:
        institutions.drop(extra_dates, axis=0, level="Date", inplace=True)

    return institutions

def get_sharadar_sec8_reindexed_like(
    reindex_like: 'pd.DataFrame',
    event_codes: Union[list[int], int] = None
    ) -> 'pd.DataFrame':
    """
    Return a Boolean DataFrame indicating whether securities filed SEC Form
    8-K for specified event codes on given dates. The resulting DataFrame
    will be reindexed to match the index (dates) and columns (sids) of
    `reindex_like`.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    event_codes : list of int, optional
        limit to these event codes

    Returns
    -------
    DataFrame
        a Boolean DataFrame shaped like the input DataFrame

    Notes
    -----
    Usage Guide:

    * Sharadar SEC Form 8-K: https://qrok.it/dl/qr/sharadar-sec8

    Examples
    --------
    Query bankruptcies (event code 13) and use it to mask a prices
    DataFrame:

    >>> closes = prices.loc["Close"]
    >>> filed_for_bankruptcy = get_sharadar_sec8_reindexed_like(closes, event_codes=13)
    >>> closes.where(filed_for_bankruptcy)
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date().isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    f = six.StringIO()
    try:
        download_sharadar_sec8(
            filepath_or_buffer=f, sids=sids,
            start_date=start_date, end_date=end_date,
            event_codes=event_codes,
            fields=["Sid","DATE","EVENTCODE"]
        )
    except NoFundamentalData:
        # If no data for these securities, there were no events
        return pd.DataFrame(False, index=reindex_like.index, columns=reindex_like.columns)

    events = pd.read_csv(f, parse_dates=["DATE"])

    # Rename DATE to match price history index name
    events = events.rename(columns={"DATE": "Date"})

    # if reindex_like.index is tz-aware, make financials tz-aware so they can
    # be joined
    if reindex_like.index.tz:
        events["Date"] = events.Date.dt.tz_localize(reindex_like.index.tz.zone)

    # If multiple events, drop duplicates
    events = events.drop_duplicates(subset=["Sid","Date"])

    # Pivot and reindex
    events = events.pivot(index="Sid",columns="Date").T
    events = events.loc["EVENTCODE"]
    events = events.reindex(index=reindex_like.index, columns=reindex_like.columns)

    have_events = events.notnull()
    return have_events

def get_sharadar_sp500_reindexed_like(
    reindex_like: 'pd.DataFrame'
    ) -> 'pd.DataFrame':
    """
    Return a Boolean DataFrame indicating whether securities were in the S&P
    500 on the given dates. The resulting DataFrame will be reindexed to
    match the index (dates) and columns (sids) of `reindex_like`.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    Returns
    -------
    DataFrame
        a Boolean DataFrame shaped like the input DataFrame

    Notes
    -----
    Usage Guide:

    * Sharadar S&P 500: https://qrok.it/dl/qr/sharadar-sp500

    Examples
    --------
    Query S&P 500 membership and use it to mask a prices DataFrame:

    >>> closes = prices.loc["Close"]
    >>> are_in_sp500 = get_sharadar_sp500_reindexed_like(closes)
    >>> closes.where(are_in_sp500)
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    # in case constituents have never left the index, don't include a start
    # date
    end_date = reindex_like.index.max().date().isoformat()

    f = six.StringIO()
    try:
        download_sharadar_sp500(
            filepath_or_buffer=f, sids=sids, end_date=end_date,
            fields=["Sid","DATE","ACTION"])
    except NoFundamentalData:
        # If no data for these securities, they're not in the index
        return pd.DataFrame(False, index=reindex_like.index, columns=reindex_like.columns)

    sp500_changes = pd.read_csv(
        f, parse_dates=["DATE"])

    # Rename DATE to match price history index name
    sp500_changes = sp500_changes.rename(columns={"DATE": "Date"})

    # if reindex_like.index is tz-aware, make financials tz-aware so they can
    # be joined
    if reindex_like.index.tz:
        sp500_changes["Date"] = sp500_changes.Date.dt.tz_localize(reindex_like.index.tz.zone)

    deduped_datekeys = sp500_changes.Date.drop_duplicates()

    # Create a unioned index of input DataFrame and statement DATEs
    union_date_idx = reindex_like.index.union(pd.DatetimeIndex(deduped_datekeys)).sort_values()

    # Reindex with unioned index and ffill to create Boolean dataframe
    sp500_changes = sp500_changes.pivot(index="Sid",columns="Date").T
    sp500_changes = sp500_changes.loc["ACTION"]
    sp500_changes = sp500_changes.reindex(index=union_date_idx)
    are_in_sp500 = sp500_changes.ffill().fillna("removed") == "added"

    # reindex like input DataFrame
    are_in_sp500 = are_in_sp500.reindex(
        index=reindex_like.index,
        columns=reindex_like.columns,
        fill_value=False)

    return are_in_sp500

def download_wsh_earnings_dates(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    statuses: Union[list[str], str] = None,
    fields: Union[list[str], str] = None
    ) -> None:
    """
    Query earnings announcement dates from the Wall Street Horizon
    announcements database and download to file.

    DEPRECATED. This data is no longer available from Interactive Brokers. Only
    data that was previously saved to the local database can be queried.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json or csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to announcements on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to announcements on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    statuses : list of str, optional
        limit to these confirmation statuses. Possible choices: Confirmed, Unconfirmed

    fields : list of str, optional
        only return these fields (pass ['?'] or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    Examples
    --------
    Query earnings dates for a universe of US stocks:

    >>> download_wsh_earnings_dates("announcements.csv", universes=["usa-stk"],
                                    start_date="2019-01-01"
                                    end_date="2019-04-01")
    >>> announcements = pd.read_csv("announcements.csv", parse_dates=["Date"])
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
    if statuses:
        params["statuses"] = statuses
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/fundamental/wsh/calendar.{0}".format(output), params=params,
                           timeout=60*5)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        if "no earnings dates match the query parameters" in repr(e).lower():
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_wsh_earnings_dates(*args, **kwargs):
    return json_to_cli(download_wsh_earnings_dates, *args, **kwargs)

def get_wsh_earnings_dates_reindexed_like(
    reindex_like: 'pd.DataFrame',
    fields: Union[list[str], str] = ["Time"],
    statuses: Union[list[str], str] = ["Confirmed"]
    ) -> 'pd.DataFrame':
    """
    Return a multiindex (Field, Date) DataFrame of earnings announcement dates,
    reindexed to match the index (dates) and columns (sids) of `reindex_like`.

    DEPRECATED. This data is no longer available from Interactive Brokers. Only
    data that was previously saved to the local database can be queried.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        including the Time field.

    statuses : list of str, optional
        limit to these confirmation statuses. By default only confirmed
        announcements are returned. Possible choices: Confirmed, Unconfirmed.

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of earnings dates, shaped like the
        input DataFrame

    Examples
    --------
    Get a boolean DataFrame indicating announcements that occurred since the prior
    close:

    >>> closes = prices.loc["Close"]
    >>> announcements = get_wsh_earnings_dates_reindexed_like(closes)
    >>> announce_times = announcements.loc["Time"]
    >>> announced_since_prior_close = (announce_times == "Before Market") | (announce_times.shift() == "After Market")

    For live trading, to get a boolean DataFrame indicating announcements that will occur
    before the next session's open, first extend the index of the input DataFrame to include
    the next session:

    >>> from exchange_calendars import get_calendar
    >>> nyse_cal = get_calendar("NYSE")
    >>> latest_session = closes.index.max()
    >>> # wind latest session to end of day and use calendar to get next session
    >>> next_session = nyse_cal.next_open(latest_session.replace(hour=23, minute=59)).date()
    >>> closes = closes.reindex(closes.index.union([next_session]))
    >>> closes.index.name = "Date" # reindex loses name attribute

    Then get the announcements, shifting pre-market announcements backward:

    >>> announcements = get_wsh_earnings_dates_reindexed_like(closes)
    >>> announce_times = announcements.loc["Time"]
    >>> announces_before_next_open = (announce_times == "After Market") | (announce_times.shift(-1) == "Before Market")

    Finally, if needed, restore the DataFrame indexes to their original shape:

    >>> closes = closes.drop(next_session)
    >>> announces_before_next_open = announces_before_next_open.drop(next_session)
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    if not isinstance(fields, (list,tuple)):
        fields = [fields]

    if statuses and not isinstance(statuses, (list, tuple)):
        statuses = [statuses]

    f = six.StringIO()
    query_fields = list(fields) # copy fields on Py2 or 3: https://stackoverflow.com/a/2612815/417414
    if "LastUpdated" not in fields:
        query_fields.append("LastUpdated")
    download_wsh_earnings_dates(
        f, sids=sids, start_date=start_date, end_date=end_date,
        fields=query_fields, statuses=statuses)
    announcements = pd.read_csv(f, parse_dates=["Date", "LastUpdated"])

    # if reindex_like.index is tz-aware, make announcements tz-aware too
    if reindex_like.index.tz:
        announcements["Date"] = announcements.Date.dt.tz_localize(reindex_like.index.tz.zone)

    # There might be duplicate Dates for confirmed vs unconfirmed announcements (or other changes to
    # confirmed or unconfirmed announcements). In this case we keep only the most recently updated
    # record
    announcements = announcements.sort_values(["LastUpdated"]).drop_duplicates(subset=["Sid","Date"], keep="last")

    # Drop any fields we don't need
    needed_fields = set(fields)
    needed_fields.update(set(("Sid", "Date")))

    unneeded_fields = set(announcements.columns) - needed_fields
    if unneeded_fields:
        announcements = announcements.drop(unneeded_fields, axis=1)

    announcements = announcements.pivot(index="Sid",columns="Date").T

    multiidx = pd.MultiIndex.from_product(
        (announcements.index.get_level_values(0).unique(), reindex_like.index),
        names=["Field", "Date"])
    announcements = announcements.reindex(index=multiidx, columns=reindex_like.columns)

    return announcements

def collect_brain_bsi() -> dict[str, str]:
    """
    Collect Brain Sentiment Indicator (BSI) data and save to database.

    This dataset provides news sentiment scores for US stocks, with history
    back to August 2, 2016.

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Brain Sentiment Indicator: https://qrok.it/dl/qr/brain-bsi
    """
    response = houston.post("/fundamental/brain/bsi")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_brain_bsi(*args, **kwargs):
    return json_to_cli(collect_brain_bsi, *args, **kwargs)

def collect_brain_blmcf() -> dict[str, str]:
    """
    Collect Brain Language Metrics on Company Filings (BLMCF) data and
    save to database.

    This dataset provides sentiment scores and other language metrics for
    10-K and 10-Q company filings for US stocks, with history back to
    January 1, 2010.

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Brain Language Metrics on Company Filings: https://qrok.it/dl/qr/brain-blmcf
    """
    response = houston.post("/fundamental/brain/blmcf")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_brain_blmcf(*args, **kwargs):
    return json_to_cli(collect_brain_blmcf, *args, **kwargs)

def collect_brain_blmect() -> dict[str, str]:
    """
    Collect Brain Language Metrics on Earnings Call Transcripts (BLMECT) data
    and save to database.

    This dataset provides sentiment scores and other language metrics for
    earnings call transcripts for US stocks, with history back to January 1, 2012.

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Brain Language Metrics on Earnings Call Transcripts: https://qrok.it/dl/qr/brain-blmect
    """
    response = houston.post("/fundamental/brain/blmect")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_brain_blmect(*args, **kwargs):
    return json_to_cli(collect_brain_blmect, *args, **kwargs)

def download_brain_bsi(
    filepath_or_buffer: FilepathOrBuffer = None,
    N: Literal[1, 7, 30] = None,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    fields: Union[list[str], str] = None,
    output: Literal["csv", "json"] = "csv"
    ) -> None:
    """
    Query Brain Sentiment Indicator (BSI) data from the local database and download
    to file.

    The sentiment scores are averaged using news articles published over the last
    1, 7, or 30 days. The date of a record means the sentiment score was available
    before the market open on that date.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    N : int, optional
        limit to records with this calculation window. Choices are 1, 7, or 30.

    start_date : str (YYYY-MM-DD), optional
        limit to records on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to records on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    fields : list of str, optional
        only return these fields (pass '?' or any invalid fieldname to see
        available fields)

    output : str
        output format (json, csv, default is csv)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Brain Sentiment Indicator: https://qrok.it/dl/qr/brain-bsi

    Examples
    --------
    Download news sentiment scores averaged over the last 7 days for
    a universe of tech stocks:

    >>> download_brain_bsi("bsi7.csv", N=7, universes=["usa-tech"], start_date="2024-01-01")
    >>> bsi = pd.read_csv("bsi7.csv", parse_dates=["Date"])
    """
    params = {}
    if N:
        params["N"] = N
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

    response = houston.get("/fundamental/brain/bsi.{0}".format(output), params=params)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "match the query parameters",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_brain_bsi(*args, **kwargs):
    return json_to_cli(download_brain_bsi, *args, **kwargs)

def get_brain_bsi_reindexed_like(
    reindex_like: 'pd.DataFrame',
    N: Literal[1, 7, 30] = 1,
    fields: Union[list[str], str] = None,
    ) -> 'pd.DataFrame':
    """
    Return a multiindex (Field, Date) DataFrame of Brain Sentiment Indicator
    (BSI) data, reindexed to match the index (dates) and columns (sids) of
    the input DataFrame.

    The sentiment scores are averaged using news articles published over the last
    1, 7, or 30 days. The date of a record means the sentiment score was available
    before the market open on that date.

    Data are not forward-filled; NaNs mean that no scores were calculated for that
    security on that date.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    N : int, optional
        The calculation window over which news articles are aggregated to calculate
        sentiment scores. Choices are 1, 7, or 30. Defaults to 1.

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        including all fields.

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of sentiment data, shaped like
        the input DataFrame

    Notes
    -----
    Usage Guide:

    * Brain Sentiment Indicator: https://qrok.it/dl/qr/brain-bsi

    Examples
    --------
    Query 1-day sentiment scores using a DataFrame of historical prices:

    >>> closes = prices.loc["Close"]
    >>> bsi = get_brain_bsi_reindexed_like(closes, N=1, fields=["SENTIMENT_SCORE"])
    >>> scores = bsi.loc["SENTIMENT_SCORE"]
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    start_date -= pd.Timedelta(days=10)
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    if fields and not isinstance(fields, (list,tuple)):
        fields = [fields]

    f = six.StringIO()
    download_brain_bsi(
        filepath_or_buffer=f, N=N, sids=sids, start_date=start_date, end_date=end_date,
        fields=fields)
    bsi = pd.read_csv(
        f, parse_dates=["Date"])

    # if reindex_like.index is tz-aware, make BSI tz-aware so they can
    # be joined
    if reindex_like.index.tz:
        bsi["Date"] = bsi.Date.dt.tz_localize(reindex_like.index.tz.zone)

    bsi = bsi.pivot(index="Sid",columns="Date").T
    multiidx = pd.MultiIndex.from_product(
        (bsi.index.get_level_values(0).unique(), reindex_like.index),
        names=["Field", "Date"])
    bsi = bsi.reindex(index=multiidx, columns=reindex_like.columns)

    return bsi

def download_brain_blmcf(
    filepath_or_buffer: FilepathOrBuffer = None,
    report_category: Literal['10-K', '10-Q'] = None,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    fields: Union[list[str], str] = None,
    output: Literal["csv", "json"] = "csv"
    ) -> None:
    """
    Query Brain Language Metrics on Company Filings (BLMCF) data from the local
    database and download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    report_category : str, optional
        limit to this report category. Choices are 10-K or 10-Q. If omitted,
        both report categories are returned.

    start_date : str (YYYY-MM-DD), optional
        limit to records on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to records on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    fields : list of str, optional
        only return these fields (pass '?' or any invalid fieldname to see
        available fields). Metrics are calculated separately for the Risk
        Factors section of the report (fields starting with RF), the Management
        Discussion and Analysis section (fields starting with MD), and the
        report as a whole (fields not starting with RF or MD). Fields
        containing "DELTA" or "SIMILARITY" in the name compare the current
        report with the previous report of the same period and category.

    output : str
        output format (json, csv, default is csv)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Brain Language Metrics on Company Filings: https://qrok.it/dl/qr/brain-blmcf

    Examples
    --------
    Download language metrics on company filings for all available stocks for a single
    year:

    >>> download_brain_blmcf("blmcf.csv", start_date="2023-01-01", end_date="2024-01-01")
    >>> blmcf = pd.read_csv("blmcf.csv", parse_dates=["Date"])
    """
    params = {}
    if report_category:
        params["report_categories"] = report_category
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

    response = houston.get("/fundamental/brain/blmcf.{0}".format(output), params=params)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "match the query parameters",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_brain_blmcf(*args, **kwargs):
    return json_to_cli(download_brain_blmcf, *args, **kwargs)

def _get_brain_blm_reindexed_like(
    reindex_like, download_func, fields=None, **kwargs
    ) -> 'pd.DataFrame':
    """
    Common function for get_brain_blmcf_reindexed_like and
    get_brain_blmect_reindexed_like.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    download_func : function
        the download function to use

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        including all fields. For faster performance, limiting fields to
        those needed is highly recommended, especially for large universes.

    kwargs : dict, optional
        additional keyword arguments to pass to the download function

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of metrics, shaped like
        the input DataFrame

    Notes
    -----
    Usage Guide:

    * Brain Language Metrics on Company Filings: https://qrok.it/dl/qr/brain-blmcf

    Examples
    --------
    Query metrics using a DataFrame of historical prices:

    >>> closes = prices.loc["Close"]
    >>> metrics = get_brain_blmcf_reindexed_like(closes, fields=["SENTIMENT", "SCORE_LITIGIOUS"])
    >>> sentiment_scores = metrics.loc["SENTIMENT"]
    >>> litigious_scores = metrics.loc["SCORE_LITIGIOUS"]

    Query metrics for 10-K filings only:

    >>> metrics = get_brain_blmcf_reindexed_like(closes, report_category="10-K", fields=["SENTIMENT"])
    >>> sentiment_scores = metrics.loc["SENTIMENT"]
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
                ",".join([str(name) for name in index_levels])))

    if not hasattr(reindex_like.index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like.index.min().date()
    # Since company filings are sparse, start well before the reindex_like
    # min date
    start_date -= pd.Timedelta(days=365 + 180)
    start_date = start_date.isoformat()
    end_date = reindex_like.index.max().date().isoformat()

    if fields and not isinstance(fields, (list,tuple)):
        fields = [fields]

    f = six.StringIO()
    download_func(
        filepath_or_buffer=f, sids=sids, start_date=start_date, end_date=end_date,
        fields=fields, **kwargs)
    date_fields = ["Date"]
    if fields:
        for date_field in ("LAST_REPORT_DATE", "PREV_REPORT_DATE", "LAST_TRANSCRIPT_DATE", "PREV_TRANSCRIPT_DATE"):
            if date_field in fields:
                date_fields.append(date_field)
    metrics = pd.read_csv(
        f, parse_dates=date_fields)

    # if reindex_like.index is tz-aware, make metrics tz-aware so they can
    # be joined (tz-aware or tz-naive are both fine, as DATEKEY represents
    # dates which are assumed to be in the local timezone of the reported
    # company)
    if reindex_like.index.tz:
        metrics["Date"] = metrics.Date.dt.tz_localize(reindex_like.index.tz.zone)

    deduped_dates = metrics.Date.drop_duplicates()

    # Create a unioned index of input DataFrame and statement dates
    union_date_idx = reindex_like.index.union(pd.DatetimeIndex(deduped_dates)).sort_values()

    # There might be duplicate dates if a company announced
    # reports for several fiscal periods at once. In this case we keep
    # only the last value (i.e. latest fiscal period)
    metrics = metrics.drop_duplicates(subset=["Sid", "Date"], keep="last")
    metrics = metrics.pivot(index="Sid",columns="Date").T
    multiidx = pd.MultiIndex.from_product(
        (metrics.index.get_level_values(0).unique(), union_date_idx),
        names=["Field", "Date"])
    metrics = metrics.reindex(index=multiidx, columns=reindex_like.columns)

    # reports are sparse so ffill (one field at a time)
    all_fields = {}

    for fieldname in metrics.index.get_level_values("Field").unique():

        field = metrics.loc[fieldname]

        # forward-fill values (we don't shift because Brain already does that)
        field = field.ffill()

        all_fields[fieldname] = field

    metrics = pd.concat(all_fields, names=["Field", "Date"])

    # In cases the statements included dates not in the input
    # DataFrame, drop those now that we've ffilled
    extra_dates = union_date_idx.difference(reindex_like.index)
    if not extra_dates.empty:
        metrics.drop(extra_dates, axis=0, level="Date", inplace=True)

    return metrics

def get_brain_blmcf_reindexed_like(
    reindex_like: 'pd.DataFrame',
    fields: Union[list[str], str] = None,
    report_category: Literal['10-K', '10-Q'] = None,
    ) -> 'pd.DataFrame':
    """
    Return a multiindex (Field, Date) DataFrame of Brain Language Metrics
    on Company Filings (BLMCF) data, reindexed to match the index (dates)
    and columns (sids) of `reindex_like`. Language metrics are forward-filled
    to provide the latest reading at any given date.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        including all fields. For faster performance, limiting fields to
        those needed is highly recommended, especially for large universes.
        Pass '?' or any invalid fieldname to see available fields. Metrics
        are calculated separately for the Risk Factors section of the report
        (fields starting with RF), the Management Discussion and Analysis
        section (fields starting with MD), and the report as a whole (fields
        not starting with RF or MD). Fields containing "DELTA" or "SIMILARITY"
        in the name compare the current report with the previous report of
        the same period and category.

    report_category : str, optional
        limit to this report category. Choices are 10-K or 10-Q. If omitted,
        both report categories are returned.

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of metrics, shaped like
        the input DataFrame

    Notes
    -----
    Usage Guide:

    * Brain Language Metrics on Company Filings: https://qrok.it/dl/qr/brain-blmcf

    Examples
    --------
    Query metrics using a DataFrame of historical prices:

    >>> closes = prices.loc["Close"]
    >>> metrics = get_brain_blmcf_reindexed_like(closes, fields=["SENTIMENT", "SCORE_LITIGIOUS"])
    >>> sentiment_scores = metrics.loc["SENTIMENT"]
    >>> litigious_scores = metrics.loc["SCORE_LITIGIOUS"]

    Query metrics for 10-K filings only:

    >>> metrics = get_brain_blmcf_reindexed_like(closes, report_category="10-K", fields=["SENTIMENT"])
    >>> sentiment_scores = metrics.loc["SENTIMENT"]
    """
    return _get_brain_blm_reindexed_like(
        reindex_like, download_brain_blmcf, fields=fields, report_category=report_category
    )

def download_brain_blmect(
    filepath_or_buffer: FilepathOrBuffer = None,
    start_date: str = None,
    end_date: str = None,
    universes: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    exclude_universes: Union[list[str], str] = None,
    exclude_sids: Union[list[str], str] = None,
    fields: Union[list[str], str] = None,
    output: Literal["csv", "json"] = "csv"
    ) -> None:
    """
    Query Brain Language Metrics on Earnings Call Transcripts (BLMECT) data from
    the local database and download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    start_date : str (YYYY-MM-DD), optional
        limit to records on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to records on or before this date

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    fields : list of str, optional
        only return these fields (pass '?' or any invalid fieldname to see
        available fields). Fields are organized into three sections,
        corresponding to three sections of the earnings call transcripts:
        "Management Discussion" (MD), "Analyst Questions" (AQ), and
        "Management Answers" (MA). Fields containing "DELTA" or "SIMILARITY"
        in the name compare the current earnings call transcript to the
        previous earnings call transcript.

    output : str
        output format (json, csv, default is csv)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Brain Language Metrics on Earnings Call Transcripts: https://qrok.it/dl/qr/brain-blmect

    Examples
    --------
    Download language metrics on earnings call transcripts for all available stocks
    for a single year:

    >>> download_brain_blmect("blmect.csv", start_date="2023-01-01", end_date="2024-01-01")
    >>> blmect = pd.read_csv("blmect.csv", parse_dates=["Date"])
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

    response = houston.get("/fundamental/brain/blmect.{0}".format(output), params=params)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        no_data_messages = (
            "match the query parameters",
        )
        if any([msg in repr(e).lower() for msg in no_data_messages]):
            raise NoFundamentalData(e)
        raise

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_brain_blmect(*args, **kwargs):
    return json_to_cli(download_brain_blmect, *args, **kwargs)

def get_brain_blmect_reindexed_like(
    reindex_like: 'pd.DataFrame',
    fields: Union[list[str], str] = None,
    ) -> 'pd.DataFrame':
    """
    Return a multiindex (Field, Date) DataFrame of Brain Language Metrics
    on Earnings Call Transcripts (BLMECT) data, reindexed to match the
    index (dates) and columns (sids) of `reindex_like`. Language metrics
    are forward-filled to provide the latest reading at any given date.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    fields : list of str
        a list of fields to include in the resulting DataFrame. Defaults to
        including all fields. For faster performance, limiting fields to
        those needed is highly recommended, especially for large universes.
        Pass '?' or any invalid fieldname to see available fields. Fields
        are organized into three sections, corresponding to three sections
        of the earnings call transcripts: "Management Discussion" (MD),
        "Analyst Questions" (AQ), and "Management Answers" (MA). Fields
        containing "DELTA" or "SIMILARITY" in the name compare the current
        earnings call transcript to the previous earnings call transcript.

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of metrics, shaped like
        the input DataFrame

    Notes
    -----
    Usage Guide:

    * Brain Language Metrics on Earnings Call Transcripts: https://qrok.it/dl/qr/brain-blmect

    Examples
    --------
    Query metrics using a DataFrame of historical prices:

    >>> closes = prices.loc["Close"]
    >>> metrics = get_brain_blmect_reindexed_like(closes, fields=["MD_SENTIMENT", "MD_SCORE_LITIGIOUS"])
    >>> sentiment_scores = metrics.loc["MD_SENTIMENT"]
    >>> litigious_scores = metrics.loc["MD_SCORE_LITIGIOUS"]
    """
    return _get_brain_blm_reindexed_like(
        reindex_like, download_brain_blmect, fields=fields
    )