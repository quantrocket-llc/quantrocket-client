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

from quantrocket.houston import houston
from quantrocket.constants.capital import ACCOUNT_FIELDS

def get_balance(nlv=None, cushion=None, fields=None, accounts=None,
                         below_cushion=None, save=False, gateways=None):
    """
    Returns a snapshot of current capital balance info.

    Parameters
    ----------
    nlv : bool
        only return NLV (equivalent to `fields=['NetLiquidation']`)

    cushion : bool
        only return margin cushion (equivalent to `fields=['Cushion']`)

    fields : list, optional
        only return the specified fields (default all fields). Possible choices:
        AccountType, NetLiquidation, TotalCashValue, SettledCash, AccruedCash, BuyingPower,
        EquityWithLoanValue, PreviousEquityWithLoanValue, GrossPositionValue, RegTEquity,
        RegTMargin, SMA, InitMarginReq, MaintMarginReq, AvailableFunds, ExcessLiquidity,
        Cushion, FullInitMarginReq, FullMaintMarginReq, FullAvailableFunds, FullExcessLiquidity,
        LookAheadNextChange, LookAheadInitMarginReq, LookAheadMaintMarginReq, LookAheadAvailableFunds,
        LookAheadExcessLiquidity, HighestSeverity, DayTradesRemaining, Leverage

    accounts : list, optional
        only return info for the specified accounts (default all connected accounts);
        account IDs or nicknames can be provided

    below_cushion : float, optional
        only return accounts where the cushion is below this level (e.g. 0.05)

    save : bool
        save a snapshot of capital balance info to the capital database

    gateways : list, optional
        only query these IB Gateway services (identified by service name)

    Returns
    -------
    DataFrame
    """
    raise NotImplementedError("This service is not yet available")
    response = houston.get("/capital/balance")
    response.raise_for_status()

    # Require pandas, or warn if missing?
    try:
        import pandas as pd
    except ImportError:
        # Use a packaged stub of pandas that can load and repr a dataframe? Probably not...
        # Or print JSON with a warning?
        pass
    else:
        raise NotImplementedError()

    return response.text

def get_balance_history(start_date=None, end_date=None, nlv=None, fields=None,
                        accounts=None, latest=False):
    """
    Returns historical capital balance snapshots from the capital database.

    Parameters
    ----------
    start_date : date or str
        limit to history on or after this date

    end_date : date or str
        limit to history on or before this date

    nlv : bool
        only return NLV (equivalent to `fields=['NetLiquidation']`)

    fields : list, optional
        only return the specified fields (default all fields). Possible choices:
        AccountType, NetLiquidation, TotalCashValue, SettledCash, AccruedCash, BuyingPower,
        EquityWithLoanValue, PreviousEquityWithLoanValue, GrossPositionValue, RegTEquity,
        RegTMargin, SMA, InitMarginReq, MaintMarginReq, AvailableFunds, ExcessLiquidity,
        Cushion, FullInitMarginReq, FullMaintMarginReq, FullAvailableFunds, FullExcessLiquidity,
        LookAheadNextChange, LookAheadInitMarginReq, LookAheadMaintMarginReq, LookAheadAvailableFunds,
        LookAheadExcessLiquidity, HighestSeverity, DayTradesRemaining, Leverage

    accounts : list, optional
        only return info for the specified accounts (default all accounts);
        account IDs or nicknames can be provided

    latest : bool
        only return the latest historical snapshot in the date range

    Returns
    -------
    DataFrame
    """
    raise NotImplementedError("This service is not yet available")
    response = houston.get("/capital/balance/history")
    response.raise_for_status()
    return response.json()
