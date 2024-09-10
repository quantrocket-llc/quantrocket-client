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
Functions for placing and managing orders and tracking positions and
executions.

Functions
---------
place_orders
    Place one or more orders.

cancel_orders
    Cancel one or more orders by order ID, sid, or order ref.

download_order_statuses
    Download order statuses.

download_positions
    Query current positions and write results to file.

list_positions
    Query current positions and return them as a Python list.

close_positions
    Generate orders to close positions.

download_executions
    Query executions from the executions database.

record_executions
    Record executions that happened outside of QuantRocket's knowledge.

apply_split
    Apply a stock split to an open position.

download_pnl
    Query trading performance and return a CSV of results or PDF tearsheet.

read_pnl_csv
    Load a PNL CSV into a DataFrame.

Notes
-----
Usage Guide:

* Orders and Positions: https://qrok.it/dl/qr/orders
* Performance Tracking: https://qrok.it/dl/qr/performance
"""
import sys
import six
import json
from typing import TYPE_CHECKING, Union, Literal
if TYPE_CHECKING:
    import pandas as pd
from quantrocket.houston import houston
from quantrocket.utils._typing import FilepathOrBuffer
from quantrocket._cli.utils.output import json_to_cli
from quantrocket._cli.utils.stream import to_bytes
from quantrocket._cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs
from quantrocket._cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.utils._parse import _read_moonshot_or_pnl_csv

__all__ = [
    "place_orders",
    "cancel_orders",
    "download_order_statuses",
    "download_positions",
    "list_positions",
    "close_positions",
    "download_executions",
    "record_executions",
    "apply_split",
    "download_pnl",
    "read_pnl_csv",
]

def place_orders(
    orders: list[dict[str, Union[str, float]]] = None,
    infilepath_or_buffer: FilepathOrBuffer = None
    ) -> list[str]:
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

    Notes
    -----
    Usage Guide:

    * Orders and Positions: https://qrok.it/dl/qr/orders

    Examples
    --------
    >>> orders = []
    >>> order1 = {
            'Sid':'FIBBG123456',
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

def cancel_orders(
    order_ids: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    order_refs: Union[list[str], str] = None,
    accounts: Union[list[str], str] = None,
    cancel_all: bool = None,
    cancel_cfd_by_underlying: bool = None
    ) -> dict[str, str]:
    """
    Cancel one or more orders by order ID, sid, or order ref.

    Parameters
    ----------
    order_ids : list of str, optional
        cancel these order IDs

    sids : list of str, optional
        cancel orders for these sids

    order_refs: list of str, optional
        cancel orders for these order refs

    accounts : list of str, optional
        cancel orders for these accounts

    cancel_all : bool
        cancel all open orders

    cancel_cfd_by_underlying : bool
        if True, cancel CFD orders by specifying the sid of the underlying security
        rather than the CFD sid when using the `sids` parameter. If False, cancel CFD
        orders using the CFD sid. Has no effect on non-CFD orders or if the `sids`
        parameter is not used.

    Returns
    -------
    dict
        status message

    Notes
    -----
    Usage Guide:

    * Orders and Positions: https://qrok.it/dl/qr/orders

    Examples
    --------
    Cancel orders by order ID:

    >>> cancel_orders(order_ids=['6002:45','6002:46'])

    Cancel orders by sid:

    >>> cancel_orders(sids=["FIBBG123456"])

    Cancel orders by order ref:

    >>> cancel_orders(order_refs=['my-strategy'])

    Cancel all open orders:

    >>> cancel_orders(cancel_all=True)
    """
    params = {}
    if order_ids:
        params["order_ids"] = order_ids
    if sids:
        params["sids"] = sids
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if cancel_all:
        params["cancel_all"] = cancel_all
    if cancel_cfd_by_underlying:
        params["cancel_cfd_by_underlying"] = cancel_cfd_by_underlying

    response = houston.delete("/blotter/orders", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_cancel_orders(*args, **kwargs):
    return json_to_cli(cancel_orders, *args, **kwargs)

OrderStatusField = Literal[
    'Account',
    'Action',
    'AdjustableTrailingUnit',
    'AdjustedStopLimitPrice',
    'AdjustedStopPrice',
    'AdjustedTrailingAmount',
    'AlgoId',
    'AlgoStrategy',
    'AllOrNone',
    'AuxPrice',
    'BlockOrder',
    'Broker',
    'ClientId',
    'Commission',
    'ConId',
    'DiscretionaryAmt',
    'DisplaySize',
    'EquityWithLoanAfter',
    'EquityWithLoanBefore',
    'EquityWithLoanChange',
    'Errors',
    'Exchange',
    'FaGroup',
    'FaMethod',
    'FaPercentage',
    'FaProfile',
    'Filled',
    'GoodAfterTime',
    'GoodTillDate',
    'Hidden',
    'InitMarginAfter',
    'InitMarginBefore',
    'InitMarginChange',
    'LmtPrice',
    'LmtPriceOffset',
    'MaintMarginAfter',
    'MaintMarginBefore',
    'MaintMarginChange',
    'MaxCommission',
    'MinCommission',
    'MinQty',
    'NotHeld',
    'OcaGroup',
    'OcaType',
    'OpenClose',
    'OrderDetailsJson',
    'OrderId',
    'OrderNum',
    'OrderRef',
    'OrderType',
    'Origin',
    'OutsideRth',
    'ParentId',
    'PercentOffset',
    'PermId',
    'Remaining',
    'Sid',
    'Status',
    'Submitted',
    'SweepToFill',
    'Tif',
    'TotalQuantity',
    'TrailStopPrice',
    'TrailingPercent',
    'Transmit',
    'TriggerMethod',
    'TriggerPrice',
    'WarningText',
    'WhatIf']

def download_order_statuses(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    order_ids: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    order_refs: Union[list[str], str] = None,
    accounts: Union[list[str], str] = None,
    open_orders: bool = None,
    start_date: str = None,
    end_date: str = None,
    fields: Union[OrderStatusField, list[str]] = None,
    map_cfd_to_underlying: bool = None
    ) -> None:
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

    sids : list of str, optional
        limit to orders for these sids

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

    map_cfd_to_underlying : bool
        if True, return CFD order statuses (if any) with the sid of the underlying
        security rather than the CFD sid. (In this case, if using the `sids` parameter,
        specifying the underlying sid will retrieve the CFD.) If False, return CFD
        order statuses with the CFD sid. Has no effect on non-CFD order statuses.

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Orders and Positions: https://qrok.it/dl/qr/orders

    Examples
    --------
    Download order status by order ID and load into Pandas:

    >>> f = io.StringIO()
    >>> download_order_statuses(f, order_ids=['6001:45','6001:46'])
    >>> order_statuses = pd.read_csv(f)

    Download order status for all open orders and include extra fields in output:

    >>> download_order_statuses(open_orders=True, fields=["LmtPrice", "OcaGroup"])

    Download order status of open orders by sid:

    >>> download_order_statuses(sids=["FIBBG123456"], open_orders=True)

    Download order status of open orders by order ref:

    >>> download_order_statuses(order_refs=['my-strategy'], open_orders=True)
    """
    params = {}
    if order_ids:
        params["order_ids"] = order_ids
    if sids:
        params["sids"] = sids
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
    if map_cfd_to_underlying:
        params["map_cfd_to_underlying"] = map_cfd_to_underlying

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

def download_positions(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    order_refs: Union[list[str], str] = None,
    accounts: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    view: str = "blotter",
    diff: bool = False,
    map_cfd_to_underlying: bool = None,
    ) -> None:
    """
    Query current positions and write results to file.

    To return positions as a Python list, see list_positions.

    There are two ways to view positions: blotter view (default) and broker view.

    The default "blotter view" returns positions by account, sid, and order ref. Positions
    are tracked based on execution records saved to the blotter database.

    "Broker view" (view='broker') returns positions by account and sid (but
    not order ref) as reported directly by the broker.

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

    sids : list of str, optional
        limit to these sids

    view : str, optional
        whether to return 'broker' view of positions (by account and sid) or
        default 'blotter' view (by account, sid, and order ref). Choices are:
        blotter, broker

    diff : bool
        limit to positions where the blotter quantity and broker quantity disagree
        (requires `view='broker'`)

    map_cfd_to_underlying : bool
        if True, return CFD positions (if any) with the sid of the underlying
        security rather than the CFD sid. (In this case, if using the `sids` parameter,
        specifying the underlying sid will retrieve the CFD.) If False, return CFD
        positions with the CFD sid. Has no effect on non-CFD positions.

    Returns
    -------
    None

    See Also
    --------
    list_positions : load positions into Python list

    Notes
    -----
    Usage Guide:

    * Orders and Positions: https://qrok.it/dl/qr/orders
    """
    params = {}
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if sids:
        params["sids"] = sids
    if view:
        params["view"] = view
    if diff:
        params["diff"] = diff
    if map_cfd_to_underlying:
        params["map_cfd_to_underlying"] = map_cfd_to_underlying

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

def list_positions(
    order_refs: Union[list[str], str] = None,
    accounts: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    view: str = "blotter",
    diff: bool = False,
    map_cfd_to_underlying: bool = None
    ) -> list[dict[str, Union[str, float]]]:
    """
    Query current positions and return them as a Python list.

    There are two ways to view positions: blotter view (default) and broker view.

    The default "blotter view" returns positions by account, sid, and order ref. Positions
    are tracked based on execution records saved to the blotter database.

    "Broker view" (view='broker') returns positions by account and sid (but
    not order ref) as reported directly by the broker.

    Parameters
    ----------
    order_refs : list of str, optional
        limit to these order refs (not supported with broker view)

    accounts : list of str, optional
        limit to these accounts

    sids : list of str, optional
        limit to these sids

    view : str, optional
        whether to return 'broker' view of positions (by account and sid) or
        default 'blotter' view (by account, sid, and order ref). Choices are:
        blotter, broker

    diff : bool
        limit to positions where the blotter quantity and broker quantity disagree
        (requires `view='broker'`)

    map_cfd_to_underlying : bool
        if True, return CFD positions (if any) with the sid of the underlying
        security rather than the CFD sid. (In this case, if using the `sids` parameter,
        specifying the underlying sid will retrieve the CFD.) If False, return CFD
        positions with the CFD sid. Has no effect on non-CFD positions.

    Returns
    -------
    list

    Notes
    -----
    Usage Guide:

    * Orders and Positions: https://qrok.it/dl/qr/orders

    Examples
    --------
    Query current positions and load into Pandas:

    >>> positions = list_positions()
    >>> if positions:
    >>>     positions = pd.DataFrame(positions)
    """
    f = six.StringIO()
    download_positions(f, output="json",
                       sids=sids, accounts=accounts,
                       order_refs=order_refs, view=view,
                       diff=diff, map_cfd_to_underlying=map_cfd_to_underlying)

    if f.getvalue():
        return json.loads(f.getvalue())
    else:
        return []

def close_positions(
    filepath_or_buffer: FilepathOrBuffer = None,
    output: Literal["csv", "json"] = "csv",
    order_refs: Union[list[str], str] = None,
    accounts: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    params: dict[str, Union[str, float]] = None
    ) -> None:
    """
    Generate orders to close positions.

    Doesn't actually place any orders but returns an orders file that can be placed
    separately. Additional order parameters can be appended with the `params` argument.

    This endpoint can also be used to generate executions for marking a position
    as closed due to a tender offer, merger/acquisition, etc. (See `quantrocket.blotter.record_executions`
    for more info.)

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

    sids : list of str, optional
        limit to these sids

    params : dict of PARAM:VALUE, optional
        additional parameters to append to each row in output (pass as {param:value},
        for example {"OrderType":"MKT"})

    Returns
    -------
    None

    See Also
    --------
    place_orders : place one or more orders
    record_executions : record executions that happened outside of QuantRocket's knowledge

    Notes
    -----
    Usage Guide:

    * Orders and Positions: https://qrok.it/dl/qr/orders

    Examples
    --------
    Get orders to close positions, then place the orders:

    >>> from quantrocket.blotter import place_orders, close_positions
    >>> import io
    >>> orders_file = io.StringIO()
    >>> close_positions(orders_file, params={"OrderType":"MKT", "Tif":"DAY", "Exchange":"SMART"})
    >>> place_orders(infilepath_or_buffer=orders_file)

    After receiving 23.50 per share in a tender offer for a position, record the
    execution in the blotter in order to mark the position as closed:

    >>> from quantrocket.blotter import record_executions
    >>> executions_file = io.StringIO()
    >>> close_positions(executions_file, sids="FIBBG123456", params={"Price": 23.50})
    >>> record_executions(infilepath_or_buffer=executions_file)
    """
    _params = {}
    if order_refs:
        _params["order_refs"] = order_refs
    if accounts:
        _params["accounts"] = accounts
    if sids:
        _params["sids"] = sids
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

def download_executions(
    filepath_or_buffer: FilepathOrBuffer = None,
    order_refs: Union[list[str], str] = None,
    accounts: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    start_date: str = None,
    end_date: str = None,
    map_cfd_to_underlying: bool = None
    ) -> None:
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

    sids : list of str, optional
        limit to these sids

    start_date : str (YYYY-MM-DD), optional
        limit to executions on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to executions on or before this date

    map_cfd_to_underlying : bool
        if True, return CFD executions (if any) with the sid of the underlying
        security rather than the CFD sid. (In this case, if using the `sids` parameter,
        specifying the underlying sid will retrieve the CFD.) If False, return CFD
        executions with the CFD sid. Has no effect on non-CFD executions.

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Performance Tracking: https://qrok.it/dl/qr/performance
    """
    params = {}
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if sids:
        params["sids"] = sids
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if map_cfd_to_underlying:
        params["map_cfd_to_underlying"] = map_cfd_to_underlying

    response = houston.get("/blotter/executions.csv", params=params)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_executions(*args, **kwargs):
    return json_to_cli(download_executions, *args, **kwargs)

def record_executions(
    executions: list[dict[str, Union[str, float]]] = None,
    infilepath_or_buffer: FilepathOrBuffer = None
    ) -> list[str]:
    """
    Record executions that happened outside of QuantRocket's knowledge.

    This endpoint does not interact with the broker but simply adds one or more
    executions to the blotter database and updates the blotter's record of current
    positions accordingly. It can be used to bring the blotter in line with the broker
    when they differ. For example, when a position is liquidated because of a tender
    offer or merger/acquisition, you can use this endpoint to record the price
    received for your shares.

    Parameters
    ----------
    executions : list of dict of PARAM:VALUE, optional
        a list of one or more executions, where each execution is a dict specifying
        the execution parameters. The required params are:

            - Account
            - Action ("BUY" or "SELL")
            - OrderRef
            - Price
            - Sid
            - TotalQuantity

        Optional params (rarely needed):

            - Commission (default is 0)
            - OrderId (default is an auto-generated ID)
            - Time (the time of execution, default is now)

    infilepath_or_buffer : str or file-like object, optional
        record executions from this CSV or JSON file (specify '-' to read file
        from stdin). Mutually exclusive with `executions` argument.

    Returns
    -------
    list
        a list of execution IDs generated by the blotter and inserted in the
        database

    See Also
    --------
    close_positions : generate orders to close positions, or generate executions
      to mark positions as closed

    Notes
    -----
    Usage Guide:

    * Delisted Positions: https://qrok.it/dl/qr/delisted-positions

    Examples
    --------
    >>> executions = []
    >>> execution1 = {
            'Sid':'FIBBG123456',
            'Action':'BUY',
            'TotalQuantity':100,
            'Account':'DU12345',
            'OrderRef':'my-strategy',
            'Price': 23.50
        }
    >>> executions.append(execution1)
    >>> execution_ids = record_executions(executions)
    """
    if executions and infilepath_or_buffer:
        raise ValueError("executions and infilepath_or_buffer are mutually exclusive")

    url = "/blotter/executions"

    if executions:
        response = houston.post(url, json=executions)

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

def _cli_record_executions(*args, **kwargs):
    params = kwargs.pop("params", None)
    if params:
        executions = []
        execution1 = dict_strs_to_dict(*params)
        executions.append(execution1)
        kwargs["executions"] = executions
    return json_to_cli(record_executions, *args, **kwargs)

def apply_split(
    sid: str,
    old_shares: int,
    new_shares: int
    ) -> list[dict[str, Union[str, int]]]:
    """
    Apply a stock split to an open position.

    This endpoint does not interact with the broker but simply applies the
    split in the blotter database to bring the blotter in line with the broker.
    The split is also applied to the executions that created the open
    position, so that PNL calculations will be accurate.

    The old_shares and new_shares parameters can be specified either using the
    published split ratio (for example, 2-for-1) or the actual number of pre-
    and post-split shares in your account.

    Parameters
    ----------
    sid : str, required
        the sid that underwent a split. There must currently be an open
        position in this security.

    old_shares : int, required
        the number of pre-split shares

    new_shares : int, required
        the number of post-split shares

    Returns
    -------
    list
        the old and new position for this sid, by account and order ref

    Notes
    -----
    Usage Guide:

    * Stock Splits: https://qrok.it/dl/qr/apply-split

    Examples
    --------
    Record a 2-for-1 split:

    >>> record_split("FIBBG12345", old_shares=1, new_shares=2)

    Record a 1-for-10 reverse split:

    >>> record_split("FIBBG98765", old_shares=10, new_shares=1)
    """
    params = {
        "sid": sid,
        "old_shares": old_shares,
        "new_shares": new_shares
    }
    response = houston.patch("/blotter/positions", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_apply_split(*args, **kwargs):
    return json_to_cli(apply_split, *args, **kwargs)

def download_pnl(
    filepath_or_buffer: FilepathOrBuffer = None,
    order_refs: Union[list[str], str] = None,
    accounts: Union[list[str], str] = None,
    sids: Union[list[str], str] = None,
    start_date: str = None,
    end_date: str = None,
    timezone: str = None,
    details: bool = False,
    output: Literal["csv", "pdf"] = "csv"
    ) -> None:
    """
    Query trading performance and return a CSV of results or PDF tearsheet.

    Trading performance is broken down by account and order ref and optionally by
    sid.

    Parameters
    ----------
    filepath_or_buffer : str or file-like object
        filepath to write the data to, or file-like object (defaults to stdout)

    order_refs : list of str, optional
        limit to these order refs

    accounts : list of str, optional
        limit to these accounts

    sids : list of str, optional
        limit to these sids

    start_date : str (YYYY-MM-DD), optional
        limit to pnl on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to pnl on or before this date

    details : bool
        return detailed results for all securities instead of aggregating to
        account/order ref level (only supported for a single account and order ref
        at a time)

    timezone : str, optional
        return execution times in this timezone (default UTC)

    output : str, required
        the output format (choices are csv or pdf, default is csv)

    Returns
    -------
    None

    Notes
    -----
    Usage Guide:

    * Performance Tracking: https://qrok.it/dl/qr/performance
    """
    params = {}
    if order_refs:
        params["order_refs"] = order_refs
    if accounts:
        params["accounts"] = accounts
    if sids:
        params["sids"] = sids
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    if details:
        params["details"] = details
    if timezone:
        params["timezone"] = timezone

    output = output or "csv"

    if output not in ("csv", "pdf"):
        raise ValueError("invalid output: {0} (choices are csv or pdf".format(output))

    response = houston.get("/blotter/pnl.{0}".format(output), params=params, timeout=60*10)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_download_pnl(*args, **kwargs):
    return json_to_cli(download_pnl, *args, **kwargs)

def read_pnl_csv(
    filepath_or_buffer: FilepathOrBuffer
    ) -> 'pd.DataFrame':
    """
    Load a PNL CSV into a DataFrame.

    This is a light wrapper around pd.read_csv that handles setting index
    columns and casting to proper data types.

    Parameters
    ----------
    filepath_or_buffer : string or file-like, required
        path to CSV

    Returns
    -------
    DataFrame
        a multi-index (Field, Date[, Time]) DataFrame of backtest
        results, with sids or strategy codes as columns

    Notes
    -----
    Usage Guide:

    * Performance Tracking: https://qrok.it/dl/qr/performance

    Examples
    --------
    >>> results = read_pnl_csv("pnl.csv")
    >>> returns = results.loc["Return"]
    """
    return _read_moonshot_or_pnl_csv(filepath_or_buffer)