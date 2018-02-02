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
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.stream import to_bytes
from quantrocket.cli.utils.parse import dict_strs_to_dict

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
            'Quantity':100,
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
                        accounts=None, open_orders=None):
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

    Returns
    -------
    dict
        order statuses

    Examples
    --------
    List order status by order ID:

    >>> list_order_statuses(order_ids=['6002:45','6002:46'])

    List order status for all open orders:

    >>> list_order_statuses(open_orders=True)

    List order status of open orders by conid:

    >>> list_order_statuses(conids=[123456], open_orders=True)

    List order status of open orders by order ref:

    >>> list_order_statuses(order_refs=['my-strategy'], open_orders=True)
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

    response = houston.get("/blotter/orders", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_order_statuses(*args, **kwargs):
    return json_to_cli(list_order_statuses, *args, **kwargs)
