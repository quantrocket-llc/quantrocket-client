# Copyright 2018 QuantRocket - All Rights Reserved
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
from quantrocket.cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer

def place_orders(orders=None, infilepath_or_buffer=None):
    """
    Place one or more orders.

    Returns a list of order IDs, which can be used to cancel the orders or check
    their status.

    Parameters
    ----------
    orders : list of dict of PARAM:VALUE, optional
        a list of one or more orders, where each order is a dict specifying the
        order parameters (see examples)

    infilepath_or_buffer : str or file-like object, optional
        place orders from this CSV or JSON file (specify '-' to read file
        from stdin). Mutually exclusive with `orders` argument.

    Returns
    -------
    list
        order IDs

    Examples
    --------
    >>> orders = []
    >>> order1 = {
            'ConId':123456,
            'Action':'BUY',
            'Exchange':'SMART',
            'TotalQuantity':100,
            'OrderType':'MKT',
            'Tif':'Day',
            'Account':'DU12345',
            'OrderRef':'my-strategy'
        }
    >>> orders.append(order1)
    >>> order_ids = place_orders(orders)
    """
    if orders and infilepath_or_buffer:
        raise ValueError("orders and infilepath_or_buffer are mutually exclusive")

    url = "/blotter/orders"

    if orders:
        response = houston.post(url, json=orders)

    elif infilepath_or_buffer == "-":
        response = houston.post(url, data=to_bytes(sys.stdin))

    elif infilepath_or_buffer and hasattr(infilepath_or_buffer, "read"):
        if infilepath_or_buffer.seekable():
            infilepath_or_buffer.seek(0)
        response = houston.post(url, data=to_bytes(infilepath_or_buffer))

    elif infilepath_or_buffer:
        with open(infilepath_or_buffer, "rb") as f:
            response = houston.post(url, data=f)
    else:
        response = houston.post(url)

    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_place_orders(*args, **kwargs):
    params = kwargs.pop("params", None)
    if params:
        orders = []
        order1 = dict_strs_to_dict(*params)
        orders.append(order1)
        kwargs["orders"] = orders
    return json_to_cli(place_orders, *args, **kwargs)

def cancel_orders(order_ids=None, conids=None, order_refs=None, accounts=None,
                  cancel_all=None):
    """
    Cancel one or more orders by order ID, conid, or order ref.

    Parameters
    ----------
    order_ids : list of str, optional
        cancel these order IDs

    conids : list of int, optional
        cancel orders for these conids

    order_refs: list of str, optional
        cancel orders for these order refs

    accounts : list of str, optional
        cancel orders for these accounts

    cancel_all : bool
        cancel all open orders

    Returns
    -------
    dict
        status message

    Examples
    --------
    Cancel orders by order ID:

    >>> cancel_orders(order_ids=['6002:45','6002:46'])

    Cancel orders by conid:

    >>> cancel_orders(conids=[123456])

    Cancel orders by order ref:

    >>> cancel_orders(order_refs=['my-strategy'])

    Cancel all open orders:

    >>> cancel_orders(cancel_all=True)
    """
    params = {}
    if order_ids:
        params["order_ids"] = order_ids
    if conids:
        params["conids"] = conids
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if cancel_all:
        params["cancel_all"] = cancel_all

    response = houston.delete("/blotter/orders", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_cancel_orders(*args, **kwargs):
    return json_to_cli(cancel_orders, *args, **kwargs)

def download_order_statuses(filepath_or_buffer=None, output="csv",
                            order_ids=None, conids=None, order_refs=None,
                            accounts=None, open_orders=None,
                            start_date=None, end_date=None, fields=None):
    """
    Download order statuses.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json or csv, default is csv)

    order_ids : list of str, optional
        limit to these order IDs

    conids : list of int, optional
        limit to orders for these conids

    order_refs : list of str, optional
        limit to orders for these order refs

    accounts : list of str, optional
        limit to orders for these accounts

    open_orders : bool
        limit to open orders

    start_date : str (YYYY-MM-DD), optional
        limit to orders submitted on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to orders submitted on or before this date

    fields : list of str, optional
        return these fields in addition to the default fields (pass '?' or any invalid
        fieldname to see available fields)

    Returns
    -------
    None

    Examples
    --------
    Download order status by order ID and load into Pandas:

    >>> f = io.StringIO()
    >>> download_order_statuses(f, order_ids=['6001:45','6001:46'])
    >>> order_statuses = pd.read_csv(f)

    Download order status for all open orders and include extra fields in output:

    >>> download_order_statuses(open_orders=True, fields=["LmtPrice", "OcaGroup"])

    Download order status of open orders by conid:

    >>> download_order_statuses(conids=[123456], open_orders=True)

    Download order status of open orders by order ref:

    >>> download_order_statuses(order_refs=['my-strategy'], open_orders=True)
    """
    params = {}
    if order_ids:
        params["order_ids"] = order_ids
    if conids:
        params["conids"] = conids
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if open_orders:
        params["open_orders"] = open_orders
    if fields:
        params["fields"] = fields
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/blotter/orders.{0}".format(output), params=params)

    houston.raise_for_status_with_json(response)

    # Don't write a null response to file
    if response.content[:4] == b"null":
        return

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_order_statuses(*args, **kwargs):
    return json_to_cli(download_order_statuses, *args, **kwargs)

def download_positions(filepath_or_buffer=None, output="csv",
                       order_refs=None, accounts=None, conids=None,
                       view="blotter", diff=False):
    """
    Query current positions and write results to file.

    To return positions as a Python list, see list_positions.

    There are two ways to view positions: blotter view (default) and broker view.

    The default "blotter view" returns positions by account, conid, and order ref. Positions
    are tracked based on execution records saved to the blotter database.

    "Broker view" (view='broker') returns positions by account and conid (but
    not order ref) as reported directly by IB. Broker view is more authoritative but less
    informative than blotter view. Broker view is typically used to verify the accuracy
    of blotter view.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json or csv, default is csv)

    order_refs : list of str, optional
        limit to these order refs (not supported with broker view)

    accounts : list of str, optional
        limit to these accounts

    conids : list of int, optional
        limit to these conids

    view : str, optional
        whether to return 'broker' view of positions (by account and conid) or
        default 'blotter' view (by account, conid, and order ref). Choices are:
        blotter, broker

    diff : bool
        limit to positions where the blotter quantity and broker quantity disagree
        (requires `view='broker'`)

    Returns
    -------
    None

    See Also
    --------
    list_positions : load positions into Python list
    """
    params = {}
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if conids:
        params["conids"] = conids
    if view:
        params["view"] = view
    if diff:
        params["diff"] = diff

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.get("/blotter/positions.{0}".format(output), params=params)

    houston.raise_for_status_with_json(response)

    # Don't write a null response to file
    if response.content[:4] == b"null":
        return

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_positions(*args, **kwargs):
    return json_to_cli(download_positions, *args, **kwargs)

def list_positions(order_refs=None, accounts=None, conids=None,
                   view="blotter", diff=False):
    """
    Query current positions and return them as a Python list.

    There are two ways to view positions: blotter view (default) and broker view.

    The default "blotter view" returns positions by account, conid, and order ref. Positions
    are tracked based on execution records saved to the blotter database.

    "Broker view" (view='broker') returns positions by account and conid (but
    not order ref) as reported directly by IB. Broker view is more authoritative but less
    informative than blotter view. Broker view is typically used to verify the accuracy
    of blotter view.

    Parameters
    ----------
    order_refs : list of str, optional
        limit to these order refs (not supported with broker view)

    accounts : list of str, optional
        limit to these accounts

    conids : list of int, optional
        limit to these conids

    view : str, optional
        whether to return 'broker' view of positions (by account and conid) or
        default 'blotter' view (by account, conid, and order ref). Choices are:
        blotter, broker

    diff : bool
        limit to positions where the blotter quantity and broker quantity disagree
        (requires `view='broker'`)

    Returns
    -------
    list

    Examples
    --------
    Query current positions and load into Pandas:

    >>> positions = list_positions()
    >>> if positions:
    >>>     positions = pd.DataFrame(positions)
    """
    f = six.StringIO()
    download_positions(f, output="json",
                       conids=conids, accounts=accounts,
                       order_refs=order_refs, view=view,
                       diff=diff)

    if f.getvalue():
        return json.loads(f.getvalue())
    else:
        return []

def close_positions(filepath_or_buffer=None, output="csv",
                    order_refs=None, accounts=None, conids=None,
                    params=None):
    """
    Generate orders to close positions.

    Doesn't actually place any orders but returns an orders file that can be placed
    separately. Additional order parameters can be appended with the `params` argument.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json or csv, default is csv)

    order_refs : list of str, optional
        limit to these order refs

    accounts : list of str, optional
        limit to these accounts

    conids : list of int, optional
        limit to these conids

    params : dict of PARAM:VALUE, optional
        additional parameters to append to each row in output (pass as {param:value},
        for example {"OrderType":"MKT"})

    Returns
    -------
    None

    Examples
    --------
    Get orders to close positions, then place the orders:

    >>> from quantrocket.blotter import place_orders, close_positions
    >>> import io
    >>> orders_file = io.StringIO()
    >>> close_positions(orders_file, params={"OrderType":"MKT", "Tif":"DAY", "Exchange":"SMART"})
    >>> place_orders(infilepath_or_buffer=orders_file)
    """
    _params = {}
    if order_refs:
        _params["order_refs"] = order_refs
    if accounts:
        _params["accounts"] = accounts
    if conids:
        _params["conids"] = conids
    if params:
        _params["params"] = dict_to_dict_strs(params)

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("Invalid ouput: {0}".format(output))

    response = houston.delete("/blotter/positions.{0}".format(output), params=_params)

    houston.raise_for_status_with_json(response)

    # Don't write a null response to file
    if response.content[:4] == b"null":
        return

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_close_positions(*args, **kwargs):
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(close_positions, *args, **kwargs)

def download_executions(filepath_or_buffer=None,
                        order_refs=None, accounts=None, conids=None,
                        start_date=None, end_date=None):
    """
    Query executions from the executions database.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    order_refs : list of str, optional
        limit to these order refs

    accounts : list of str, optional
        limit to these accounts

    conids : list of int, optional
        limit to these conids

    start_date : str (YYYY-MM-DD), optional
        limit to executions on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to executions on or before this date

    Returns
    -------
    None
    """
    params = {}
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if conids:
        params["conids"] = conids
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    response = houston.get("/blotter/executions.csv", params=params)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_executions(*args, **kwargs):
    return json_to_cli(download_executions, *args, **kwargs)

def download_pnl(filepath_or_buffer=None,
                 order_refs=None, accounts=None, conids=None,
                 start_date=None, end_date=None, time=None,
                 details=False, output="csv"):
    """
    Query trading performance and return a CSV of results or PDF tearsheet.

    Trading performance is broken down by account and order ref and optionally by
    conid.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    order_refs : list of str, optional
        limit to these order refs

    accounts : list of str, optional
        limit to these accounts

    conids : list of int, optional
        limit to these conids

    start_date : str (YYYY-MM-DD), optional
        limit to pnl on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to pnl on or before this date

    time : str (HH:MM:SS [TZ]), optional
        time of day (with optional timezone) for which to calculate daily PNL (default is
        11:59:59 UTC)

    details : bool
        return detailed results for all securities instead of aggregating to
        account/order ref level (only supported for a single account and order ref
        at a time)

    output : str, required
        the output format (choices are csv or pdf, default is csv)

    Returns
    -------
    None
    """
    params = {}
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if conids:
        params["conids"] = conids
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if time:
        params["time"] = time
    if details:
        params["details"] = details

    output = output or "csv"

    if output not in ("csv", "pdf"):
        raise ValueError("invalid output: {0} (choices are csv or pdf".format(output))

    response = houston.get("/blotter/pnl.{0}".format(output), params=params, timeout=60*10)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_pnl(*args, **kwargs):
    return json_to_cli(download_pnl, *args, **kwargs)
