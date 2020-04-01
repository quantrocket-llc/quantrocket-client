# Copyright 2019 QuantRocket LLC - All Rights Reserved
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

from quantrocket.exceptions import ParameterError

def _read_moonshot_or_pnl_csv(filepath_or_buffer):
    """
    Load a Moonshot backtest CSV or PNL CSV into a DataFrame.

    This is a light wrapper around pd.read_csv that handles setting index
    columns and casting to proper data types.

    Parameters
    ----------
    filepath_or_buffer : string or file-like, required
        path to CSV

    Returns
    -------
    DataFrame
        a multi-index (Field, Date[, Time]) DataFrame of backtest or PNL
        results, with sids or strategy codes as columns
    """
    try:
        import pandas as pd
        import numpy as np
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    results = pd.read_csv(filepath_or_buffer, parse_dates=["Date"])
    index_cols = ["Field", "Date"]
    if "Time" in results.columns:
        index_cols.append("Time")
    results = results.set_index(index_cols)

    fields_in_results = results.index.get_level_values("Field").unique()

    # Cast to float
    float_fields = [
        "Return",
        "Pnl",
        "Signal",
        "Weight",
        "NetExposure",
        "PositionQuantity",
        "PositionValue",
        "NetLiquidation",
        "Turnover",
        "Commission",
        "CommissionAmount",
        "Slippage",
        "Benchmark",
        "AbsWeight",
        "AbsExposure",
        "TotalHoldings",
        "Price"

    ]
    for field in float_fields:
        if field in fields_in_results:
            results.loc[[field]] = results.loc[[field]].astype(np.float64)

    return results
