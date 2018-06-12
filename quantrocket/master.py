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

def list_exchanges(regions=None, sec_types=None):
    """
    List exchanges by security type and country as found on the IB website.

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

    response = houston.get("/master/exchanges", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_exchanges(*args, **kwargs):
    return json_to_cli(list_exchanges, *args, **kwargs)

def fetch_listings(exchange=None, sec_types=None, currencies=None, symbols=None,
                        universes=None, conids=None):
    """
    Fetch securities listings from IB into securities master database, either by exchange or by universes/conids.


    Specify an exchange (optionally filtering by security type, currency, and/or symbol) to fetch
    listings from the IB website and fetch associated contract details from the IB API. Or, specify universes
    or conids to fetch details from the IB API, bypassing the website.

    Parameters
    ----------
    exchange : str
        the exchange code to fetch listings for (required unless providing universes or conids)

    sec_types : list of str, optional
        limit to these security types. Possible choices: STK, ETF, FUT, CASH, IND

    currencies : list of str, optional
        limit to these currencies

    symbols : list of str, optional
        limit to these symbols

    universes : list of str, optional
        limit to these universes

    conids : list of int, optional
        limit to these conids

    Returns
    -------
    dict
        status message

    """
    params = {}
    if exchange:
        params["exchange"] = exchange
    if sec_types:
        params["sec_types"] = sec_types
    if currencies:
        params["currencies"] = currencies
    if symbols:
        params["symbols"] = symbols
    if universes:
        params["universes"] = universes
    if conids:
        params["conids"] = conids

    response = houston.post("/master/listings", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_fetch_listings(*args, **kwargs):
    return json_to_cli(fetch_listings, *args, **kwargs)

def fetch_option_chains(universes=None, conids=None, infilepath_or_buffer=None):
    """
    Fetch option chains for underlying securities.

    Note: option chains often consist of hundreds, sometimes thousands of
    options per underlying security. Be aware that requesting option chains
    for large universes of underlying securities, such as all stocks on the
    NYSE, can take numerous hours to complete, add hundreds of thousands of
    rows to the securities master database, increase the database file size
    by several hundred megabytes, and potentially add latency to database
    queries.

    Parameters
    ----------
    universes : list of str, optional
        fetch options for these universes of underlying securities

    conids : list of int, optional
        fetch options for these underlying conids

    infilepath_or_buffer : str or file-like object, optional
        fetch options for the conids in this file (specify '-' to read file from stdin)

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

    if infilepath_or_buffer == "-":
        response = houston.post("/master/options", params=params, data=to_bytes(sys.stdin))

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.post("/master/options", params=params, data=to_bytes(infilepath_or_buffer))

    elif infilepath_or_buffer:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.post("/master/options", params=params, data=f)
    else:
        response = houston.post("/master/options", params=params)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_fetch_option_chains(*args, **kwargs):
    return json_to_cli(fetch_option_chains, *args, **kwargs)

def diff_securities(universes=None, conids=None, infilepath_or_buffer=None,
                    fields=None, delist_missing=False, delist_exchanges=None, wait=False):
    """
    Flag security details that have changed in IB's system since the time they
    were last loaded into the securities master database.

    Diff can be run synchronously or asynchronously (asynchronous is the default
    and is recommended if diffing more than a handful of securities).

    Parameters
    ----------
    universes : list of str, optional
        limit to these universes

    conids : list of int, optional
        limit to these conids

    infilepath_or_buffer : str or file-like object, optional
        limit to the conids in this file (specify '-' to read file from stdin)

    fields : list of str, optional
        only diff these fields

    delist_missing : bool
        auto-delist securities that are no longer available from IB

    delist_exchanges : list of str, optional
        auto-delist securities that are associated with these exchanges

    wait : bool
        run the diff synchronously and return the diff (otherwise run
        asynchronously and log the results, if any, to flightlog)

    Returns
    -------
    dict
        dict of conids and fields that have changed (if wait), or status message

    """
    params = {}
    if universes:
        params["universes"] = universes
    if conids:
        params["conids"] = conids
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
        response = houston.get("/master/diff", params=params, data=to_bytes(sys.stdin), timeout=timeout)

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.get("/master/diff", params=params, data=to_bytes(infilepath_or_buffer), timeout=timeout)

    elif infilepath_or_buffer:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.get("/master/diff", params=params, data=f, timeout=timeout)
    else:
        response = houston.get("/master/diff", params=params, timeout=timeout)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_diff_securities(*args, **kwargs):
    return json_to_cli(diff_securities, *args, **kwargs)

def download_master_file(filepath_or_buffer=None, output="csv", exchanges=None, sec_types=None,
                         currencies=None, universes=None, symbols=None, conids=None,
                         exclude_universes=None, exclude_conids=None,
                         sectors=None, industries=None, categories=None,
                         delisted=False, frontmonth=False, fields=None):
    """
    Query security details from the securities master database and download to file.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, txt, default is csv)

    exchanges : list of str, optional
        limit to these exchanges

    sec_types : list of str, optional
        limit to these security types. Possible choices: STK, ETF, FUT, CASH, IND, OPT, FOP

    currencies : list of str, optional
        limit to these currencies

    universes : list of str, optional
        limit to these universes

    symbols : list of str, optional
        limit to these symbols

    conids : list of int, optional
        limit to these conids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_conids : list of int, optional
        exclude these conids

    sectors : list of str, optional
        limit to these sectors

    industries : list of str, optional
        limit to these industries

    categories : list of str, optional
        limit to these categories

    delisted : bool
        include delisted securities (default False)

    frontmonth : bool
        exclude backmonth and expired futures contracts (default False)

    fields : list of str, optional
        only return these fields (pass ['?'] or any invalid fieldname to see
        available fields)

    Returns
    -------
    None

    Examples
    --------
    You can use StringIO to load the CSV into pandas.

    >>> f = io.StringIO()
    >>> download_master_file(f, universes=["my-universe"])
    >>> securities = pd.read_csv(f)
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
    if conids:
        params["conids"] = conids
    if exclude_universes:
        params["exclude_universes"] = exclude_universes
    if exclude_conids:
        params["exclude_conids"] = exclude_conids
    if sectors:
        params["sectors"] = sectors
    if industries:
        params["industries"] = industries
    if categories:
        params["categories"] = categories
    if delisted:
        params["delisted"] = delisted
    if frontmonth:
        params["frontmonth"] = frontmonth
    if fields:
        params["fields"] = fields

    output = output or "csv"

    if output not in ("csv", "json", "txt"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/master/securities.{0}".format(output), params=params)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_master_file(*args, **kwargs):
    return json_to_cli(download_master_file, *args, **kwargs)

def create_universe(code, infilepath_or_buffer=None, from_universes=None,
                    exclude_delisted=False, append=False, replace=False):
    """
    Create a universe of securities.

    Parameters
    ----------
    code : str, required
        the code to assign to the universe (lowercase alphanumerics and hyphens only)

    infilepath_or_buffer : str or file-like object, optional
        create the universe from the conids in this file (specify '-' to read file from stdin)

    from_universes : list of str, optional
        create the universe from these existing universes

    exclude_delisted : bool
        exclude delisted securities that would otherwise be included (default is not to exclude them)

    append : bool
        append to universe if universe already exists (default False)

    replace : bool
        replace universe if universe already exists (default False)

    Returns
    -------
    dict
        status message

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
    Delete a universe. (The listings details of the member securities won't be deleted,
    only their grouping as a universe).

    Parameters
    ----------
    code : str, required
        the universe code

    Returns
    -------
    dict
        status message
    """
    response = houston.delete("/master/universes/{0}".format(code))
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

def delist_security(conid=None, symbol=None, exchange=None, currency=None, sec_type=None):
    """
    Mark a security as delisted.

    The security can be specified by conid or a combination of other parameters (for
    example, symbol + exchange). As a precaution, the request will fail if the parameters
    match more than one security.

    Parameters
    ----------
    conid : int, optional
        the conid of the security to be delisted

    symbol : str, optional
        the symbol to be delisted (if conid not provided)

    exchange : str, optional
        the exchange of the security to be delisted (if needed to disambiguate)

    currency : str, optional
        the currency of the security to be delisted (if needed to disambiguate)

    sec_type : str, optional
        the security type of the security to be delisted (if needed to disambiguate). Possible
        choices: STK, ETF, FUT, CASH, IND

    Returns
    -------
    dict
        status message
    """
    params = {}
    if conid:
        params["conids"] = conid
    if symbol:
        params["symbols"] = symbol
    if exchange:
        params["exchanges"] = exchange
    if currency:
        params["currencies"] = currency
    if sec_type:
        params["sec_types"] = sec_type

    response = houston.delete("/master/securities", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_delist_security(*args, **kwargs):
    return json_to_cli(delist_security, *args, **kwargs)

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

def fetch_calendar(exchanges=None):
    """
    Fetch upcoming trading hours for exchanges and save to securites master database.

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

    response = houston.post("/master/calendar", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_fetch_calendar(*args, **kwargs):
    return json_to_cli(fetch_calendar, *args, **kwargs)

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
    if required_since.freq.isAnchored() or required_since.freq.rule_code == "D":
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
    if required_until.freq.isAnchored() or required_until.freq.rule_code == "D":
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

    CSV should contain columns `ConId`, `Exchange`, and the columns to be rounded
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