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

def normalize_pandas_alias(alias: str) -> str:
    """
    Normalize pandas offset aliases for pandas >= 2.2.
    Handles:
      - basic aliases (M, BM, Q, Y, etc.)
      - anchored aliases (Q-NOV, Y-JUN, etc.)
      - multipliers (2M, 3Q-DEC, etc.)
    """
    import re

    deprecated_base = {
        "M": "ME",
        "BM": "BME",
        "SM": "SME",
        "CBM": "CBME",
        "Q": "QE",
        "BQ": "BQE",
        "Y": "YE",
        "A": "YE",
        "BY": "BYE",
    }

    # Pattern:
    #   optional number prefix
    #   base alias (letters)
    #   optional anchored month (-XXX)
    pattern = re.compile(r"^(\d*)([A-Z]+)(?:-([A-Z]+))?$", re.IGNORECASE)

    m = pattern.match(alias)
    if not m:
        # Unknown format; return unchanged
        return alias

    multiplier, base, anchor = m.groups()
    base_up = base.upper()

    # Replace base if deprecated
    new_base = deprecated_base.get(base_up, base_up)

    # Reconstruct alias
    if anchor:
        return f"{multiplier}{new_base}-{anchor.upper()}"
    else:
        return f"{multiplier}{new_base}"

def segmented_date_range(
    start_date: str,
    end_date: str,
    segment: str = "Y"
    ) -> list[tuple[str, str]]:
    """
    Split a date range into smaller segments.

    Parameters
    ----------

    start_date : str (YYYY-MM-DD), required
        start date, inclusive

    end_date : str (YYYY-MM-DD), required
        end date, inclusive

    segment : str, required
        split date range into segments of this size (use Pandas
        frequency string, e.g. 'Y' for yearly segments or 'Q' for quarterly
        segments; default 'Y')

    Returns
    -------
    list
        list of tuples of (start_date, end_date)

    Examples
    --------
    >>> segmented_date_range("2013-06-01", "2015-12-15")
    [('2013-06-01', '2013-12-30'),
    ('2013-12-31', '2014-12-30'),
    ('2014-12-31', '2015-12-15')]
    """

    # Import pandas lazily since it can take a moment to import
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    date_segments = []
    period_boundaries = list(pd.date_range(start_date, end_date, freq=normalize_pandas_alias(segment)))
    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)
    if start_date not in period_boundaries:
        period_boundaries.insert(0, start_date)
    if end_date not in period_boundaries:
        period_boundaries.append(end_date)

    for i in range(len(period_boundaries)-1):
        period_start_date = period_boundaries[i].date()
        period_end_date = period_boundaries[i+1].date()
        if period_end_date < end_date.date():
            period_end_date -= pd.Timedelta(days=1)

        period_start_date = period_start_date.isoformat()
        period_end_date = period_end_date.isoformat()
        date_segments.append((period_start_date, period_end_date))

    return date_segments
