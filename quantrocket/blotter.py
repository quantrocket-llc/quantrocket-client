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
from quantrocket.cli.utils.parse import dict_strs_to_dict
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

def list_order_statuses(order_ids=None, conids=None, order_refs=None,
                        accounts=None, open_orders=None, fields=None):
    """
    List order status for one or more orders by order ID, conid, order ref, or account.

    Parameters
    ----------
    order_ids : list of str, optional
        limit to these order IDs

    conids : list of int, optional
        limit to orders for these conids

    order_refs : list of str, optional
        limit to orders for these order refs

    accounts : list of str, optional
        limit to orders for these accounts

    open_orders : bool
        limit to open orders (default False, must be True if order_ids not provided)

    fields : list of str, optional
        return these fields in addition to the default fields (pass '?' or any invalid
        fieldname to see available fields)

    Returns
    -------
    dict
        order statuses

    Examples
    --------
    List order status by order ID:

    >>> list_order_statuses(order_ids=['6002:45','6002:46'])

    List order status for all open orders and include extra fields in output:

    >>> list_order_statuses(open_orders=True, fields=["LmtPrice", "OcaGroup"])

    List order status of open orders by conid:

    >>> list_order_statuses(conids=[123456], open_orders=True)

    List order status of open orders by order ref:

    >>> list_order_statuses(order_refs=['my-strategy'], open_orders=True)

    Load order statuses into pandas (with order IDs as index):

    >>> statuses = list_order_statuses(open_orders=True)
    >>> statuses = pd.DataFrame(statuses).T
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

    response = houston.get("/blotter/orders", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_order_statuses(*args, **kwargs):
    return json_to_cli(list_order_statuses, *args, **kwargs)

def download_positions(filepath_or_buffer=None, output="csv",
                       order_refs=None, accounts=None, conids=None):
    """
    Query current positions and write results to file.

    To return positions as a Python list, see list_positions.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, txt, default is csv)

    order_refs : list of str, optional
        limit to these order refs

    accounts : list of str, optional
        limit to these accounts

    conids : list of int, optional
        limit to these conids

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

    output = output or "csv"

    if output not in ("csv", "json", "txt"):
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

def list_positions(order_refs=None, accounts=None, conids=None):
    """
    Query current positions and return them as a Python list.

    Parameters
    ----------
    order_refs : list of str, optional
        limit to these order refs

    accounts : list of str, optional
        limit to these accounts

    conids : list of int, optional
        limit to these conids

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
                       order_refs=order_refs)

    if f.getvalue():
        return json.loads(f.getvalue())
    else:
        return []

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
                 details=False, csv=False):
    """
    Query trading performance and return a PDF tearsheet or CSV of results.

    Trading performance is broken down by account and order ref and optionally by
    conid.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    output : str
        output format (json, csv, txt, default is csv)

    order_refs : list of str, optional
        limit to these order refs

    accounts : list of str, optional
        limit to these accounts

    conids : list of int, optional
        limit to these conids

    start_date : str (YYYY-MM-DD), optional
        limit to history on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to history on or before this date

    time : str (HH:MM:SS [TZ]), optional
        time of day with optional timezone to calculate daily PNL (default is
        11:59:59 UTC)

    details : bool
        return detailed results for all securities instead of aggregating to
        account/order ref level (only supported for a single account and order ref
        at a time)

    csv : bool
        return a CSV of PNL (default is to return a PDF performance tear sheet)

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

    output = "csv" if csv else "pdf"

    response = houston.get("/blotter/pnl.{0}".format(output), params=params)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_pnl(*args, **kwargs):
    return json_to_cli(download_pnl, *args, **kwargs)