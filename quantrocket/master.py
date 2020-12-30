# Copyright 2020 QuantRocket - All Rights Reserved
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
import json
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.stream import to_bytes
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.exceptions import ParameterError

def list_ibkr_exchanges(regions=None, sec_types=None):
    """
    List exchanges by security type and country as found on the IBKR website.

    Parameters
    ----------
    regions : list of str, optional
        limit to these regions. Possible choices: north_america, europe, asia, global

    sec_types : list of str, optional
        limit to these securitiy types. Possible choices: STK, ETF, FUT, CASH, IND

    Returns
    -------
    dict
    """
    params = {}
    if sec_types:
        params["sec_types"] = sec_types
    if regions:
        params["regions"] = regions

    response = houston.get("/master/exchanges/ibkr", params=params, timeout=180)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_ibkr_exchanges(*args, **kwargs):
    return json_to_cli(list_ibkr_exchanges, *args, **kwargs)

def collect_alpaca_listings():
    """
    Collect securities listings from Alpaca and store in securities master
    database.

    Returns
    -------
    dict
        status message
    """
    response = houston.post("/master/securities/alpaca")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_alpaca_listings(*args, **kwargs):
    return json_to_cli(collect_alpaca_listings, *args, **kwargs)

def collect_edi_listings(exchanges=None):
    """
    Collect securities listings from EDI and store in securities master
    database.

    Parameters
    ----------
    exchanges : list or str, required
        collect listings for these exchanges (identified by MICs)

    Returns
    -------
    dict
        status message

    Examples
    --------
    Collect sample listings:

    >>> collect_edi_listings(exchanges="FREE")

    Collect listings for all permitted exchanges:

    >>> collect_edi_listings()

    Collect all Chinese stock listings:

    >>> collect_edi_listings(exchanges=["XSHG", "XSHE"])
    """
    params = {}
    if exchanges:
        params["exchanges"] = exchanges

    response = houston.post("/master/securities/edi", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_edi_listings(*args, **kwargs):
    return json_to_cli(collect_edi_listings, *args, **kwargs)

def collect_figi_listings():
    """
    Collect securities listings from Bloomberg OpenFIGI and store
    in securities master database.

    OpenFIGI provides several useful security attributes including
    market sector, a detailed security type, and share class-level
    FIGI identifier.

    The collected data fields show up in the master file with the
    prefix "figi_*".

    This function does not directly query the OpenFIGI API but rather
    downloads a dump of all FIGIs which QuantRocket has previously
    mapped to securities from other vendors.

    Returns
    -------
    dict
        status message

    Examples
    --------
    Collect all available FIGI listings:

    >>> collect_figi_listings()
    """
    response = houston.post("/master/securities/figi")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_figi_listings(*args, **kwargs):
    return json_to_cli(collect_figi_listings, *args, **kwargs)

def collect_ibkr_listings(exchanges=None, sec_types=None, currencies=None,
                          symbols=None, universes=None, sids=None):
    """
    Collect securities listings from Interactive Brokers and store in
    securities master database.

    Specify an exchange (optionally filtering by security type, currency,
    and/or symbol) to collect listings from the IBKR website and collect
    associated contract details from the IBKR API. Or, specify universes or
    sids to collect details from the IBKR API, bypassing the website.

    Parameters
    ----------
    exchanges : list or str
        one or more IBKR exchange codes to collect listings for (required
        unless providing universes or sids). For sample data use exchange
        code 'FREE'

    sec_types : list of str, optional
        limit to these security types. Possible choices: STK, ETF, FUT, CASH, IND

    currencies : list of str, optional
        limit to these currencies

    symbols : list of str, optional
        limit to these symbols

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    Returns
    -------
    dict
        status message

    Examples
    --------
    Collect free sample listings:

    >>> collect_ibkr_listings(exchanges="FREE")

    Collect all Toronto Stock Exchange stock listings:

    >>> collect_ibkr_listings(exchanges="TSE", sec_types="STK")

    Collect all NYSE ARCA ETF listings:

    >>> collect_ibkr_listings(exchanges="ARCA", sec_types="ETF")

    Collect specific symbols from Nasdaq:

    >>> collect_ibkr_listings(exchanges="NASDAQ", symbols=["AAPL", "GOOG", "NFLX"])

    Re-collect contract details for an existing universe called "japan-fin":

    >>> collect_ibkr_listings(universes="japan-fin")
    """
    params = {}
    if exchanges:
        params["exchanges"] = exchanges
    if sec_types:
        params["sec_types"] = sec_types
    if currencies:
        params["currencies"] = currencies
    if symbols:
        params["symbols"] = symbols
    if universes:
        params["universes"] = universes
    if sids:
        params["sids"] = sids

    response = houston.post("/master/securities/ibkr", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_ibkr_listings(*args, **kwargs):
    return json_to_cli(collect_ibkr_listings, *args, **kwargs)

def collect_sharadar_listings(countries="US"):
    """
    Collect securities listings from Sharadar and store in securities master
    database.

    Parameters
    ----------
    countries : list of str, required
        countries to collect listings for. Possible choices: US, FREE

    Returns
    -------
    dict
        status message
    """
    params = {}
    if countries:
        params["countries"] = countries

    response = houston.post("/master/securities/sharadar", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_sharadar_listings(*args, **kwargs):
    return json_to_cli(collect_sharadar_listings, *args, **kwargs)

def collect_usstock_listings():
    """
    Collect US stock listings from QuantRocket and store in securities
    master database.

    Returns
    -------
    dict
        status message
    """
    response = houston.post("/master/securities/usstock")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_usstock_listings(*args, **kwargs):
    return json_to_cli(collect_usstock_listings, *args, **kwargs)

def collect_ibkr_option_chains(universes=None, sids=None, infilepath_or_buffer=None):
    """
    Collect IBKR option chains for underlying securities.

    Note: option chains often consist of hundreds, sometimes thousands of
    options per underlying security. Be aware that requesting option chains
    for large universes of underlying securities, such as all stocks on the
    NYSE, can take numerous hours to complete.

    Parameters
    ----------
    universes : list of str, optional
        collect options for these universes of underlying securities

    sids : list of str, optional
        collect options for these underlying sids

    infilepath_or_buffer : str or file-like object, optional
        collect options for the sids in this file (specify '-' to read file
        from stdin)

    Returns
    -------
    dict
        status message
    """
    params = {}
    if universes:
        params["universes"] = universes
    if sids:
        params["sids"] = sids

    if infilepath_or_buffer == "-":
        response = houston.post("/master/options/ibkr", params=params, data=to_bytes(sys.stdin))

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.post("/master/options/ibkr", params=params, data=to_bytes(infilepath_or_buffer))

    elif infilepath_or_buffer:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.post("/master/options/ibkr", params=params, data=f)
    else:
        response = houston.post("/master/options/ibkr", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_ibkr_option_chains(*args, **kwargs):
    return json_to_cli(collect_ibkr_option_chains, *args, **kwargs)

def diff_ibkr_securities(universes=None, sids=None, infilepath_or_buffer=None,
                         fields=None, delist_missing=False, delist_exchanges=None,
                         wait=False):
    """
    Flag security details that have changed in IBKR's system since the time they
    were last collected into the securities master database.

    Diff can be run synchronously or asynchronously (asynchronous is the default
    and is recommended if diffing more than a handful of securities).

    Parameters
    ----------
    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    infilepath_or_buffer : str or file-like object, optional
        limit to the sids in this file (specify '-' to read file from stdin)

    fields : list of str, optional
        only diff these fields (field name should start with "ibkr")

    delist_missing : bool
        auto-delist securities that are no longer available from IBKR

    delist_exchanges : list of str, optional
        auto-delist securities that are associated with these exchanges

    wait : bool
        run the diff synchronously and return the diff (otherwise run
        asynchronously and log the results, if any, to flightlog)

    Returns
    -------
    dict
        dict of sids and fields that have changed (if wait), or status message

    """
    params = {}
    if universes:
        params["universes"] = universes
    if sids:
        params["sids"] = sids
    if fields:
        params["fields"] = fields
    if delist_missing:
        params["delist_missing"] = delist_missing
    if delist_exchanges:
        params["delist_exchanges"] = delist_exchanges
    if wait:
        params["wait"] = wait

    # if run synchronously use a high timeout
    timeout = 60*60*10 if wait else None
    if infilepath_or_buffer == "-":
        response = houston.get("/master/diff/ibkr", params=params, data=to_bytes(sys.stdin), timeout=timeout)

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.get("/master/diff/ibkr", params=params, data=to_bytes(infilepath_or_buffer), timeout=timeout)

    elif infilepath_or_buffer:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.get("/master/diff/ibkr", params=params, data=f, timeout=timeout)
    else:
        response = houston.get("/master/diff/ibkr", params=params, timeout=timeout)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_diff_ibkr_securities(*args, **kwargs):
    return json_to_cli(diff_ibkr_securities, *args, **kwargs)

def download_master_file(filepath_or_buffer=None, output="csv", exchanges=None, sec_types=None,
                         currencies=None, universes=None, symbols=None, sids=None,
                         exclude_universes=None, exclude_sids=None,
                         exclude_delisted=False, exclude_expired=False,
                         frontmonth=False, vendors=None, fields=None):
    """
    Query security details from the securities master database and download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (csv or json, default is csv)

    exchanges : list of str, optional
        limit to these exchanges. You can specify exchanges using the MIC or the
        vendor's exchange code.

    sec_types : list of str, optional
        limit to these security types. Possible choices: STK, ETF, FUT, CASH, IND, OPT, FOP, BAG

    currencies : list of str, optional
        limit to these currencies

    universes : list of str, optional
        limit to these universes

    symbols : list of str, optional
        limit to these symbols

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    exclude_delisted : bool
        exclude delisted securities (default is to include them)

    exclude_expired : bool
        exclude expired contracts (default is to include them)

    frontmonth : bool
        exclude backmonth and expired futures contracts (default False)

    vendors : list of str, optional
        limit to these vendors. Possible choices: alpaca, edi, ibkr,
        sharadar, usstock

    fields : list of str, optional
        Return specific fields. By default a core set of fields is
        returned, but additional vendor-specific fields are also available.
        To return non-core fields, you can reference them by name, or pass "*"
        to return all available fields. To return all fields for a specific
        vendor, pass the vendor prefix followed by "*", for example "edi*"
        for all EDI fields. Pass "?*" (or any invalid vendor prefix plus "*")
        to see available vendor prefixes. Pass "?" or any invalid fieldname
        to see all available fields.

    Returns
    -------
    None

    Notes
    -----
    Parameters for filtering query results are combined according to the following
    rules. First, the master service determines what to include in the result set,
    based on the inclusion filters: `exchanges`, `sec_types`, `currencies`, `universes`,
    `symbols`, and `sids`. With the exception of `sids`, these parameters are ANDed
    together. That is, securities must satisfy all of the parameters to be included.
    If `vendors` is provided, only those vendors are searched for the purpose of
    determining matches.

    The `sids` parameter is treated differently. Securities matching `sids` are always
    included, regardless of whether they meet the other inclusion criteria.

    After determining what to include, the master service then applies the exclusion
    filters (`exclude_sids`, `exclude_universes`, `exclude_delisted`, `exclude_expired`,
    and `frontmonth`) to determine what (if anything) should be removed from the result
    set. Exclusion filters are ORed, that is, securities are excluded if they match any
    of the exclusion criteria.

    Examples
    --------
    Download NYSE and NASDAQ securities to file, using MICs to specify
    the exchanges:

    >>> download_master_file("securities.csv", exchanges=["XNYS","XNAS"])

    Download NYSE and NASDAQ securities to file, using IBKR exchange codes
    to specify the exchanges, and include all IBKR fields:

    >>> download_master_file("securities.csv", exchanges=["NYSE","NASDAQ"], fields="ibkr*")

    Download securities for a particular universe to in-memory file, including
    all possible fields, and load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_master_file(f, fields="*", universes="my-universe")
    >>> securities = pd.read_csv(f)

    See Also
    --------
    quantrocket.master.get_securities : load securities into a DataFrame
    """
    params = {}
    if exchanges:
        params["exchanges"] = exchanges
    if sec_types:
        params["sec_types"] = sec_types
    if currencies:
        params["currencies"] = currencies
    if universes:
        params["universes"] = universes
    if symbols:
        params["symbols"] = symbols
    if sids:
        params["sids"] = sids
    if exclude_universes:
        params["exclude_universes"] = exclude_universes
    if exclude_sids:
        params["exclude_sids"] = exclude_sids
    if exclude_delisted:
        params["exclude_delisted"] = exclude_delisted
    if exclude_expired:
        params["exclude_expired"] = exclude_expired
    if frontmonth:
        params["frontmonth"] = frontmonth
    if vendors:
        params["vendors"] = vendors
    if fields:
        params["fields"] = fields

    output = output or "csv"

    url = "/master/securities.{0}".format(output)

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get(url, params=params)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_master_file(*args, **kwargs):
    return json_to_cli(download_master_file, *args, **kwargs)

def get_securities(symbols=None, exchanges=None, sec_types=None,
                   currencies=None, universes=None, sids=None,
                   exclude_universes=None, exclude_sids=None,
                   exclude_delisted=False, exclude_expired=False,
                   frontmonth=False, vendors=None, fields=None):
    """
    Return a DataFrame of security details from the securities master database.

    Parameters
    ----------
    symbols : list of str, optional
        limit to these symbols

    exchanges : list of str, optional
        limit to these exchanges. You can specify exchanges using the MIC or the
        vendor's exchange code.

    sec_types : list of str, optional
        limit to these security types. Possible choices: STK, ETF, FUT, CASH, IND, OPT, FOP, BAG

    currencies : list of str, optional
        limit to these currencies

    universes : list of str, optional
        limit to these universes

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    exclude_delisted : bool
        exclude delisted securities (default is to include them)

    exclude_expired : bool
        exclude expired contracts (default is to include them)

    frontmonth : bool
        exclude backmonth and expired futures contracts (default False)

    vendors : list of str, optional
        limit to these vendors. Possible choices: alpaca, edi, ibkr,
        sharadar, usstock

    fields : list of str, optional
        Return specific fields. By default a core set of fields is
        returned, but additional vendor-specific fields are also available.
        To return non-core fields, you can reference them by name, or pass "*"
        to return all available fields. To return all fields for a specific
        vendor, pass the vendor prefix followed by "*", for example "edi*"
        for all EDI fields. Pass "?*" (or any invalid vendor prefix plus "*")
        to see available vendor prefixes. Pass "?" or any invalid fieldname
        to see all available fields.

    Returns
    -------
    DataFrame
        a DataFrame of securities, with Sids as the index

    Notes
    -----
    Parameters for filtering query results are combined according to the following
    rules. First, the master service determines what to include in the result set,
    based on the inclusion filters: `exchanges`, `sec_types`, `currencies`, `universes`,
    `symbols`, and `sids`. With the exception of `sids`, these parameters are ANDed
    together. That is, securities must satisfy all of the parameters to be included.
    If `vendors` is provided, only those vendors are searched for the purpose of
    determining matches.

    The `sids` parameter is treated differently. Securities matching `sids` are always
    included, regardless of whether they meet the other inclusion criteria.

    After determining what to include, the master service then applies the exclusion
    filters (`exclude_sids`, `exclude_universes`, `exclude_delisted`, `exclude_expired`,
    and `frontmonth`) to determine what (if anything) should be removed from the result
    set. Exclusion filters are ORed, that is, securities are excluded if they match any
    of the exclusion criteria.

    Examples
    --------
    Load default fields for NYSE and NASDAQ securities, using MICs to specify
    the exchanges:

    >>> securities = get_securities(exchanges=["XNYS","XNAS"])

    Load sids for MSFT and AAPL:
    >>> sids = get_securities(symbols=["MSFT", "AAPL"]).index.tolist()

    Load NYSE and NASDAQ securities, using IBKR exchange codes to specify the
    exchanges, and include all IBKR fields:

    >>> securities = get_securities(exchanges=["NYSE","NASDAQ"], fields="ibkr*")
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    f = six.StringIO()
    download_master_file(
        f, exchanges=exchanges, sec_types=sec_types,
        currencies=currencies, universes=universes,
        symbols=symbols, sids=sids,
        exclude_universes=exclude_universes,
        exclude_sids=exclude_sids,
        exclude_delisted=exclude_delisted,
        exclude_expired=exclude_expired, frontmonth=frontmonth,
        vendors=vendors, fields=fields)

    securities = pd.read_csv(f, index_col="Sid")

    for col in securities.columns:
        col_without_vendor_prefix = col.split("_")[-1]
        if col_without_vendor_prefix in (
            "Delisted", "Etf", "EasyToBorrow", "Marginable", "Tradable", "Shortable",
            "IsPrimaryListing"):
            securities[col] = securities[col].fillna(0).astype(bool)
        elif (
            col_without_vendor_prefix.endswith("Date")
            or col_without_vendor_prefix.startswith("Date")
            or col_without_vendor_prefix in (
                "FirstAdded", "LastAdded", "RecordCreated", "RecordModified",
                "LastUpdated", "FirstQuarter", "LastQuarter")):
            # pd.to_datetime handles NaNs in earlier pandas versions (0.22)
            # while .astype("datetime64[ns]") does not
            securities[col] = pd.to_datetime(securities[col])

    return securities

def get_securities_reindexed_like(reindex_like, fields=None):
    """
    Return a multiindex DataFrame of securities master data, reindexed to
    match the index and columns (sids) of `reindex_like`.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    fields : list of str
        a list of fields to include in the resulting DataFrame. By default a
        core set of fields is returned, but additional vendor-specific fields
        are also available. To return non-core fields, you can reference them
        by name, or pass "*" to return all available fields. To return all
        fields for a specific vendor, pass the vendor prefix followed by "*",
        for example "edi*" for all EDI fields. Pass "?*" (or any invalid
        vendor prefix plus "*") to see available vendor prefixes. Pass "?" or
        any invalid fieldname to see all available fields.

    Returns
    -------
    DataFrame
        a multiindex (Field, Date) DataFrame of securities master data, shaped
        like the input DataFrame

    Examples
    --------
    Get exchanges (MICs) using a DataFrame of prices:

    >>> closes = prices.loc["Close"]
    >>> securities = get_securities_reindexed_like(
            closes, fields=["Exchange"])
    >>> exchanges = securities.loc["Exchange"]
    >>> nyse_closes = closes.where(exchanges == "XNYS")
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    sids = list(reindex_like.columns)

    securities = get_securities(sids=sids, fields=fields)

    if "Sid" in fields:
        securities["Sid"] = securities.index

    all_master_fields = {}

    for col in sorted(securities.columns):
        this_col = securities[col]
        all_master_fields[col] = reindex_like.apply(lambda x: this_col, axis=1)

    names = list(reindex_like.index.names)
    names.insert(0, "Field")

    securities = pd.concat(all_master_fields, names=names)
    securities = securities.reindex(columns=reindex_like.columns)
    return securities

def get_contract_nums_reindexed_like(reindex_like, limit=5):
    """
    From a DataFrame of futures (with dates as the index and sids as columns),
    return a DataFrame of integers representing each sid's sequence in the
    futures chain as of each date, where 1 is the front contract, 2 is the second
    nearest contract, etc.

    Sequences are based on the RolloverDate field in the securities master
    file, which is based on configurable rollover rules.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame (usually of prices) with dates for the index and sids
        for the columns, to which the shape of the resulting DataFrame will
        be conformed

    limit : int
        how many contracts ahead to sequence. For example, assuming quarterly
        contracts, a limit of 5 will sequence 5 quarters out. Default 5.

    Returns
    -------
    DataFrame
        a DataFrame of futures chain sequence numbers, shaped like the input
        DataFrame

    Examples
    --------
    Get a Boolean mask of front-month contracts:

    >>> closes = prices.loc["Close"]
    >>> contract_nums = get_contract_nums_reindexed_like(closes)
    >>> are_front_months = contract_nums == 1
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    index_levels = reindex_like.index.names

    if "Date" not in index_levels:
        raise ParameterError(
            "reindex_like must have index called 'Date', but has {0}".format(
                ",".join([str(name) for name in index_levels])))

    reindex_like_dt_index = reindex_like.index.get_level_values("Date")

    if not hasattr(reindex_like_dt_index, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    f = six.StringIO()
    download_master_file(f, sids=list(reindex_like.columns),
                         fields=["RolloverDate","ibkr_UnderConId","SecType"])
    rollover_dates = pd.read_csv(f, parse_dates=["RolloverDate"])
    rollover_dates = rollover_dates[rollover_dates.SecType=="FUT"].drop("SecType", axis=1)

    if rollover_dates.empty:
        raise ParameterError("input DataFrame does not appear to contain any futures contracts")

    if reindex_like_dt_index.tz:
        rollover_dates.loc[:, "RolloverDate"] = rollover_dates.RolloverDate.dt.tz_localize(reindex_like_dt_index.tz.zone)

    min_date = reindex_like_dt_index.min()
    max_date = max([rollover_dates.RolloverDate.max(),
                    reindex_like_dt_index.max()])

    # Stack sids by underlying (1 column per underlying)
    rollover_dates = rollover_dates.set_index(["RolloverDate","ibkr_UnderConId"]).Sid.unstack()

    contract_nums = None

    for i in range(limit):

        # backshift conids
        _rollover_dates = rollover_dates.apply(lambda col: col.dropna().shift(-i))

        # Reindex to daily frequency
        _rollover_dates = _rollover_dates.reindex(
            index=pd.date_range(start=min_date, end=max_date))

        # RolloverDate is when we roll out of the contract, hence we backfill
        _rollover_dates = _rollover_dates.fillna(method="bfill")

        # Stack to Series of Date, nth sid
        _rollover_dates = _rollover_dates.stack()
        _rollover_dates.index = _rollover_dates.index.droplevel("ibkr_UnderConId")
        _rollover_dates.index.name = "Date"

        # Pivot Series to DataFrame
        _rollover_dates = _rollover_dates.reset_index(name="Sid")
        _rollover_dates["ContractNum"] = i + 1
        _rollover_dates = _rollover_dates.set_index(["Date","Sid"])
        _contract_nums = _rollover_dates.ContractNum.unstack()

        # If MultiIndex input, broadcast across Time level
        if len(index_levels) > 1:
            _contract_nums = _contract_nums.reindex(index=reindex_like.index,
                                                level="Date")
            _contract_nums = _contract_nums.reindex(columns=reindex_like.columns)
        else:
            _contract_nums = _contract_nums.reindex(index=reindex_like_dt_index,
                                                    columns=reindex_like.columns)

        if contract_nums is None:
            contract_nums = _contract_nums
        else:
            contract_nums = contract_nums.fillna(_contract_nums)

    return contract_nums

def create_universe(code, infilepath_or_buffer=None, sids=None, from_universes=None,
                    exclude_delisted=False, append=False, replace=False):
    """
    Create a universe of securities.

    Parameters
    ----------
    code : str, required
        the code to assign to the universe (lowercase alphanumerics and hyphens only)

    infilepath_or_buffer : str or file-like object, optional
        create the universe from the sids in this file (specify '-' to read file from stdin)

    sids : list of str, optional
        create the universe from these sids

    from_universes : list of str, optional
        create the universe from these existing universes

    exclude_delisted : bool
        exclude delisted securities and expired contracts that would otherwise be
        included (default is not to exclude them)

    append : bool
        append to universe if universe already exists (default False)

    replace : bool
        replace universe if universe already exists (default False)

    Returns
    -------
    dict
        status message

    Examples
    --------
    Create a universe called 'nyse-stk' from a CSV file:

    >>> create_universe("usa-stk", "nyse_securities.csv")

    Create a universe from a DataFrame of securities:

    >>> securities = get_securities(exchanges="TSEJ")
    >>> create_universe("japan-stk", sids=securities.index.tolist())
    """
    if append and replace:
        raise ValueError("append and replace are mutually exclusive")

    params = {}
    if from_universes:
        params["from_universes"] = from_universes
    if exclude_delisted:
        params["exclude_delisted"] = exclude_delisted
    if replace:
        params["replace"] = replace
    if sids:
        params["sids"] = sids

    url = "/master/universes/{0}".format(code)

    if append:
        method = "PATCH"
    else:
        method = "PUT"

    if infilepath_or_buffer == "-":
        response = houston.request(method, url, params=params, data=to_bytes(sys.stdin))

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.request(method, url, params=params, data=to_bytes(infilepath_or_buffer))

    elif infilepath_or_buffer:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.request(method, url, params=params, data=f)
    else:
        response = houston.request(method, url, params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_universe(*args, **kwargs):
    return json_to_cli(create_universe, *args, **kwargs)

def delete_universe(code):
    """
    Delete a universe.

    The listings details of the member securities won't be deleted, only
    their grouping as a universe.

    Parameters
    ----------
    code : str, required
        the universe code

    Returns
    -------
    dict
        status message
    """

    url = "/master/universes/{0}".format(code)

    response = houston.delete(url)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_delete_universe(*args, **kwargs):
    return json_to_cli(delete_universe, *args, **kwargs)

def list_universes():
    """
    List universes and their size.

    Returns
    -------
    dict
        dict of universe:size
    """
    response = houston.get("/master/universes")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_universes(*args, **kwargs):
    return json_to_cli(list_universes, *args, **kwargs)

def delist_ibkr_security(sid=None, symbol=None, exchange=None, currency=None, sec_type=None):
    """
    Mark an IBKR security as delisted.

    This does not remove any data but simply marks the security as delisted so
    that data services won't attempt to collect data for the security and so
    that the security can be optionally excluded from query results.

    The security can be specified by sid or a combination of other
    parameters (for example, symbol + exchange). As a precaution, the request
    will fail if the parameters match more than one security.

    Parameters
    ----------
    sid : str, optional
        the sid of the security to be delisted

    symbol : str, optional
        the symbol to be delisted (if sid not provided)

    exchange : str, optional
        the exchange of the security to be delisted (if needed to disambiguate)

    currency : str, optional
        the currency of the security to be delisted (if needed to disambiguate)

    sec_type : str, optional
        the security type of the security to be delisted (if needed to disambiguate).
        Possible choices: STK, ETF, FUT, CASH, IND

    Returns
    -------
    dict
        status message
    """
    params = {}
    if sid:
        params["sids"] = sid
    if symbol:
        params["symbols"] = symbol
    if exchange:
        params["exchanges"] = exchange
    if currency:
        params["currencies"] = currency
    if sec_type:
        params["sec_types"] = sec_type

    response = houston.delete("/master/securities/ibkr", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_delist_ibkr_security(*args, **kwargs):
    return json_to_cli(delist_ibkr_security, *args, **kwargs)

def create_ibkr_combo(combo_legs):
    """
    Create an IBKR combo (aka spread), which is a composite instrument consisting
    of two or more individual instruments (legs) that are traded as a single
    instrument.

    Each user-defined combo is stored in the securities master database with a
    SecType of "BAG". The combo legs are stored in the ComboLegs field as a JSON
    array. QuantRocket assigns a sid for the combo consisting of a prefix 'IC'
    followed by an autoincrementing digit, for example: IC1, IC2, IC3, ...

    If the combo already exists, its sid will be returned instead of creating a
    duplicate record.

    Parameters
    ----------
    combo_legs : list, required
        a list of the combo legs, where each leg is a list specifying action, ratio,
        and sid

    Returns
    -------
    dict
        returns a dict containing the generated sid of the combo, and whether a new
        record was created

    Examples
    --------
    To create a calendar spread on VX, first retrieve the sids of the legs:

    >>> from quantrocket.master import download_master_file
    >>> download_master_file("vx.csv", symbols="VIX", exchanges="CFE", sec_types="FUT")
    >>> vx_sids = pd.read_csv("vx.csv", index_col="Symbol").Sid.to_dict()

    Then create the combo:

    >>> create_ibkr_combo([
            ["BUY", 1, vx_sids["VXV9"]],
            ["SELL", 1, vx_sids["VXQ9"]]
        ])
        {"sid": IC1, "created": True}
    """

    f = six.StringIO()
    json.dump(combo_legs, f)
    f.seek(0)

    response = houston.put("/master/combos/ibkr", data=f)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_create_ibkr_combo(combo_filepath):
    with open(combo_filepath) as f:
        combo_legs = json.load(f)
    return json_to_cli(create_ibkr_combo, combo_legs)

def load_rollrules_config(filename):
    """
    Upload a new rollover rules config.

    Parameters
    ----------
    filename : str, required
        the rollover rules YAML config file to upload

    Returns
    -------
    dict
        status message
    """
    with open(filename) as file:
        response = houston.put("/master/config/rollover", data=file.read())
    houston.raise_for_status_with_json(response)
    return response.json()

def get_rollrules_config():
    """
    Returns the current rollover rules config.

    Returns
    -------
    dict
        the config as a dict
    """
    response = houston.get("/master/config/rollover")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def _cli_load_or_show_rollrules(filename=None):
    if filename:
        return json_to_cli(load_rollrules_config, filename)
    else:
        return json_to_cli(get_rollrules_config)

def collect_ibkr_calendar(exchanges=None):
    """
    Collect upcoming trading hours from IBKR for exchanges and save to
    securities master database.

    Parameters
    ----------
    exchanges : list of str, optional
        limit to these exchanges

    Returns
    -------
    dict
        status message
    """
    params = {}
    if exchanges:
        params["exchanges"] = exchanges

    response = houston.post("/master/calendar/ibkr", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_collect_ibkr_calendar(*args, **kwargs):
    return json_to_cli(collect_ibkr_calendar, *args, **kwargs)

def list_calendar_statuses(exchanges, sec_type=None, in_=None, ago=None, outside_rth=False):
    """
    Check whether exchanges are open or closed.

    Parameters
    ----------
    exchanges : list of str, required
        the exchange(s) to check

    sec_type : str, optional
        the security type, if needed to disambiguate for exchanges that
        trade multiple security types. Possible choices: STK, FUT, CASH, OPT

    in_ : str, optional
        check whether exchanges will be open or closed at this point in the
        future (use Pandas timedelta string, e.g. 2h or 30min or 1d)

    ago : str, optional
        check whether exchanges were open or closed this long ago
        (use Pandas timedelta string, e.g. 2h or 30min or 1d)

    outside_rth : bool
        check extended hours calendar (default is to check regular
        trading hours calendar)

    Returns
    -------
    dict
        exchange calendar status
    """
    params = {}
    if exchanges:
        params["exchanges"] = exchanges
    if sec_type:
        params["sec_type"] = sec_type
    if in_:
        params["in"] = in_
    if ago:
        params["ago"] = ago
    if outside_rth:
        params["outside_rth"] = outside_rth

    response = houston.get("/master/calendar", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_calendar_statuses(*args, **kwargs):
    return json_to_cli(list_calendar_statuses, *args, **kwargs)

def _cli_in_status_since(status, since=None, in_=None, ago=None):

    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this command")

    dt = pd.Timestamp.now(status["timezone"])
    if in_:
        dt += pd.Timedelta(in_)
    elif ago:
        dt -= pd.Timedelta(ago)

    dt = dt.tz_localize(None)

    required_since = pd.date_range(periods=5, end=dt,
                                   freq=since, normalize=False)

    # For >1D freq, normalize to midnight
    if required_since.freq.is_anchored() or required_since.freq.rule_code == "D":
        required_since = pd.date_range(periods=5, end=dt, freq=since, normalize=True)
        required_since = required_since[-1]
    else:
        # If not normalized, the last value is dt, so use the penultimate value
        required_since = required_since[-2]

    actual_since = pd.Timestamp(status["since"])
    return actual_since <= required_since

def _cli_in_status_until(status, until=None, in_=None, ago=None):

    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this command")

    dt = pd.Timestamp.now(status["timezone"])
    if in_:
        dt += pd.Timedelta(in_)
    elif ago:
        dt -= pd.Timedelta(ago)

    dt = dt.tz_localize(None)

    required_until = pd.date_range(start=dt, periods=5,
                                   freq=until, normalize=False)

    # For >1D freq, normalize to midnight
    if required_until.freq.is_anchored() or required_until.freq.rule_code == "D":
        required_until = pd.date_range(start=dt, periods=5,
                                   freq=until, normalize=True)
        # due to normalize=True, the date range might include a time before the
        # start dt; filter it out
        required_until = required_until[required_until > dt]
        required_until = required_until[0]
    else:
        # If not normalized, the first value is dt, so use the second value
        required_until = required_until[1]

    actual_until = pd.Timestamp(status["until"])
    return actual_until >= required_until

def _cli_isopen(exchanges, sec_type=None, in_=None, ago=None, since=None, until=None, outside_rth=False):
    statuses = list_calendar_statuses(exchanges, sec_type=sec_type, in_=in_, ago=ago, outside_rth=outside_rth)
    is_open = all([
        calendar["status"] == "open" for calendar in statuses.values()
    ])
    if is_open and since:
        is_open = all([
            _cli_in_status_since(status, since=since, in_=in_, ago=ago)
            for status in statuses.values()])

    elif is_open and until:
        is_open = all([
            _cli_in_status_until(status, until=until, in_=in_, ago=ago)
            for status in statuses.values()])

    return '', int(not is_open)

def _cli_isclosed(exchanges, sec_type=None, in_=None, ago=None, since=None, until=None, outside_rth=False):
    statuses = list_calendar_statuses(exchanges, sec_type=sec_type, in_=in_, ago=ago, outside_rth=outside_rth)
    is_closed = all([
        calendar["status"] == "closed" for calendar in statuses.values()
    ])
    if is_closed and since:
        is_closed = all([
            _cli_in_status_since(status, since=since, in_=in_, ago=ago)
            for status in statuses.values()])

    elif is_closed and until:
        is_closed = all([
            _cli_in_status_until(status, until=until, in_=in_, ago=ago)
            for status in statuses.values()])

    return '', int(not is_closed)

def round_to_tick_sizes(infilepath_or_buffer, round_fields,
                        how=None, append_ticksize=False,
                        outfilepath_or_buffer=None):
    """
    Round prices in a CSV file to valid tick sizes.

    CSV should contain columns `Sid`, `Exchange`, and the columns to be rounded
    (e.g. `LmtPrice`). Additional columns will be ignored and returned unchanged.

    Parameters
    ----------
    infilepath_or_buffer : str or file-like object, required
        CSV file with prices to be rounded (specify '-' to read file from stdin)

    round_fields : list of str, required
        columns to be rounded

    how : str, optional
        which direction to round to. Possible choices: 'up', 'down', 'nearest'
        (default is 'nearest')

    append_ticksize : bool
        append a column of tick sizes for each field to be rounded (default False)

    outfilepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    Returns
    -------
    None
    """

    params = {}
    if round_fields:
        params["round_fields"] = round_fields
    if how:
        params["how"] = how
    if append_ticksize:
        params["append_ticksize"] = append_ticksize

    url = "/master/ticksizes.csv"

    if infilepath_or_buffer == "-":
        # No-op if an empty file is passed on stdin
        f = six.StringIO(sys.stdin.read())
        if not f.getvalue():
            return

        response = houston.get(url, params=params, data=to_bytes(f))

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.get(url, params=params, data=to_bytes(infilepath_or_buffer))

    elif infilepath_or_buffer:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.get(url, params=params, data=f)
    else:
        raise ValueError("infilepath_or_buffer is required")

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = outfilepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_round_to_tick_sizes(*args, **kwargs):
    return json_to_cli(round_to_tick_sizes, *args, **kwargs)
