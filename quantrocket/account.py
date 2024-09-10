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
Functions for querying account data.

Functions
---------
download_account_balances
    Query account balances.

download_account_portfolio
    Download account portfolio.

download_exchange_rates
    Query exchange rates for the base currency.

Notes
-----
Usage Guide:

* Account Monitoring: https://qrok.it/dl/qr/account
"""
import sys
import requests
from typing import Union, Literal
from quantrocket.utils._typing import FilepathOrBuffer
from quantrocket.houston import houston
from quantrocket.exceptions import NoAccountData
from quantrocket._cli.utils.output import json_to_cli
from quantrocket._cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket._cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs

__all__ = [
    "download_account_balances",
    "download_account_portfolio",
    "download_exchange_rates",
]

AccountField = Literal[
    'Account',
    'AccountType',
    'AccruedCash',
    'AvailableFunds',
    'Broker',
    'BuyingPower',
    'Currency',
    'Cushion',
    'Date',
    'DayTradeBuyingPower',
    'DayTradeCount',
    'DayTradesRemaining',
    'EquityWithLoanValue',
    'ExcessLiquidity',
    'FullAvailableFunds',
    'FullExcessLiquidity',
    'FullInitMarginReq',
    'FullMaintMarginReq',
    'GrossPositionValue',
    'HighestSeverity',
    'InitMarginReq',
    'LastUpdated',
    'Leverage',
    'LongMarketValue',
    'LookAheadAvailableFunds',
    'LookAheadExcessLiquidity',
    'LookAheadInitMarginReq',
    'LookAheadMaintMarginReq',
    'LookAheadNextChange',
    'MaintMarginReq',
    'MarginMultiplier',
    'NetLiquidation',
    'Paper',
    'PatternDayTrader',
    'PreviousEquity',
    'PreviousEquityWithLoanValue',
    'PreviousMaintMarginReq',
    'RegTBuyingPower',
    'RegTEquity',
    'RegTMargin',
    'SMA',
    'SettledCash',
    'ShortMarketValue',
    'TotalCashValue']

def download_account_balances(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    start_date: str = None,
    end_date: str = None,
    latest: bool = False,
    accounts: Union[list[str], str] = None,
    below: dict[str, float] = None,
    fields: Union[AccountField, list[str]] = None,
    force_refresh: bool = False
    ) -> None:
    """
    Query account balances.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json or csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to account balance snapshots taken on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to account balance snapshots taken on or before this date

    latest : bool
        return the latest account balance snapshot

    accounts : list of str, optional
        limit to these accounts

    below : dict of FIELD:AMOUNT, optional
        limit to accounts where the specified field is below the specified
        amount (pass as {field:amount}, for example {'Cushion':0.05})

    fields : list of str, optional
        only return these fields. By default a core set of fields is returned.
        Pass a list of fields, or '*' to return all fields. Pass ['?'] or any
        invalid fieldname to see available fields.

    force_refresh : bool
        refresh account balances to ensure the latest data (default is to query the
        database, which is refreshed every minute)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Account Monitoring: https://qrok.it/dl/qr/account

    Examples
    --------
    Query latest balances. You can use StringIO to load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_account_balances(f, latest=True)
    >>> balances = pd.read_csv(f, parse_dates=["LastUpdated"])
    """
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if latest:
        params["latest"] = latest
    if accounts:
        params["accounts"] = accounts
    if below:
        params["below"] = dict_to_dict_strs(below)
    if fields:
        params["fields"] = fields
    if force_refresh:
        params["force_refresh"] = force_refresh

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/account/balances.{0}".format(output), params=params)

    try:
        houston.raise_for_status_with_json(response)
    except requests.HTTPError as e:
        # Raise a dedicated exception
        if "no account balances match the query parameters" in repr(e).lower():
            raise NoAccountData(e)
        raise

    # Don't write a null response to file when using below filters
    if below and response.content[:4] == b"null":
        return

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_account_balances(*args, **kwargs):
    below = kwargs.get("below", None)
    if below:
        kwargs["below"] = dict_strs_to_dict(*below)
    return json_to_cli(download_account_balances, *args, **kwargs)

PortfolioField = Literal[
    'Account',
    'AssetId',
    'AverageCost',
    'Broker',
    'ConId',
    'CostBasis',
    'Currency',
    'Description',
    'Exchange',
    'LastUpdated',
    'MarketPrice',
    'MarketValue',
    'PctChange',
    'Position',
    'PriorClose',
    'RealizedPnl',
    'SecType',
    'Sid',
    'Symbol',
    'UnrealizedIntradayPnl',
    'UnrealizedIntradayPnlPct',
    'UnrealizedPnl',
    'UnrealizedPnlPct']

def download_account_portfolio(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    brokers: Union[Literal["alpaca", "ibkr"], list[str]] = None,
    accounts: Union[list[str], str] = None,
    sec_types: Union[list[str], str] = None,
    exchanges: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    symbols: Union[list[str], str] = None,
    include_zero: bool = False,
    fields: Union[PortfolioField, list[str]] = None
    ) -> None:
    """
    Download current portfolio.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json or csv, default is csv)

    brokers : list of str, optional
        limit to these brokers. Possible choices: alpaca, ibkr

    accounts : list of str, optional
        limit to these accounts

    sec_types : list of str, optional
        limit to these security types

    exchanges : list of str, optional
        limit to these exchanges

    sids : list of str, optional
        limit to these sids

    symbols : list of str, optional
        limit to these symbols

    include_zero : bool
        include zero position rows (default is to exclude them). Only
        supported for Interactive Brokers.

    fields : list of str, optional
        only return these fields. By default a core set of fields is returned.
        Pass a list of fields, or '*' to return all fields. Pass '?' or any
        invalid fieldname to see available fields.

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Account Monitoring: https://qrok.it/dl/qr/account

    Examples
    --------
    Download current portfolio. You can use StringIO to load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_account_portfolio(f)
    >>> portfolio = pd.read_csv(f, parse_dates=["LastUpdated"])
    """
    params = {}
    if brokers:
        params["brokers"] = brokers
    if accounts:
        params["accounts"] = accounts
    if sec_types:
        params["sec_types"] = sec_types
    if exchanges:
        params["exchanges"] = exchanges
    if sids:
        params["sids"] = sids
    if symbols:
        params["symbols"] = symbols
    if include_zero:
        params["include_zero"] = include_zero
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/account/portfolio.{0}".format(output), params=params)

    houston.raise_for_status_with_json(response)

    # Don't write a null response to file
    if response.content[:4] == b"null":
        return

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_account_portfolio(*args, **kwargs):
    return json_to_cli(download_account_portfolio, *args, **kwargs)

def download_exchange_rates(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    start_date: str = None,
    end_date: str = None,
    latest: bool = False,
    base_currencies: Union[list[str], str] = None,
    quote_currencies: Union[list[str], str] = None
    ) -> None:
    """
    Query exchange rates for the base currency.

    The exchange rates in the exchange rate database are sourced from the
    European Central Bank's reference rates, which are updated each day at 4 PM
    CET.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, default is csv)

    start_date : str (YYYY-MM-DD), optional
        limit to exchange rates on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to exchange rates on or before this date

    latest : bool
        return the latest exchange rates

    base_currencies : list of str, optional
        limit to these base currencies

    quote_currencies : list of str, optional
        limit to these quote currencies

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Account Monitoring: https://qrok.it/dl/qr/account

    Examples
    --------
    Query latest exchange rates. You can use StringIO to load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_exchange_rates(f, latest=True)
    >>> rates = pd.read_csv(f, parse_dates=["Date"])
    """
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if latest:
        params["latest"] = latest
    if base_currencies:
        params["base_currencies"] = base_currencies
    if quote_currencies:
        params["quote_currencies"] = quote_currencies

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/account/rates.{0}".format(output), params=params)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_exchange_rates(*args, **kwargs):
    return json_to_cli(download_exchange_rates, *args, **kwargs)
