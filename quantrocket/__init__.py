# Copyright 2017-2024 QuantRocket LLC - All Rights Reserved
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
QuantRocket API modules and functions.

Modules
-------
account
    Functions for querying account data.

blotter
    Functions for placing and managing orders and tracking positions and
    executions.

codeload
    Functions for working loading code.

countdown
    Functions for working with the countdown (crontab) service.

db
    Functions for managing QuantRocket databases.

flightlog
    Functions for working with QuantRocket logs.

fundamental
    Functions for collecting and querying fundamental data.

history
    Functions for collecting and querying historical data.

houston
    Classes and functions related to Houston, QuantRocket's API Gateway.

ibg
    Functions for working with IB Gateway.

license
    Functions for setting and viewing software licenses and third-party API
    keys.

master
    Functions for working with the securities master database.

moonshot
    Functions for backtesting and trading with Moonshot.

realtime
    Functions for collecting and querying real-time data.

satellite
    Functions for running custom scripts.

utils
    QuantRocket utility functions.

version
    Functions for checking the QuantRocket version number.

zipline
    Functions for backtesting and trading with Zipline, and managing Zipline
    bundles.

Functions
---------
get_prices
    Query one or more history databases, real-time aggregate databases,
    and/or Zipline bundles and load prices into a DataFrame.

get_prices_reindexed_like
    Return a multiindex (Field, Date) DataFrame of prices for one or more history
    databases, real-time aggregate databases, or Zipline bundles, reindexed to match
    the index (dates) and columns (sids) of the input DataFrame.
"""
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from quantrocket import (
    account,
    blotter,
    codeload,
    countdown,
    db,
    exceptions,
    flightlog,
    fundamental,
    history,
    houston,
    ibg,
    license,
    master,
    moonshot,
    realtime,
    satellite,
    utils,
    version,
    zipline
)
from quantrocket.price import get_prices, get_prices_reindexed_like

__all__ = [
    "account",
    "blotter",
    "codeload",
    "countdown",
    "db",
    "exceptions",
    "flightlog",
    "fundamental",
    "get_prices",
    "get_prices_reindexed_like",
    "history",
    'houston',
    "ibg",
    "license",
    "master",
    "moonshot",
    "realtime",
    "satellite",
    "utils",
    "version",
    "zipline"
]