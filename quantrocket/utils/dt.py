# Copyright 2018 QuantRocket LLC - All Rights Reserved
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

def segmented_date_range(start_date, end_date, segment="A"):
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
        frequency string, e.g. 'A' for annual segments or 'Q' for quarterly
        segments; default 'A')

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
    period_boundaries = list(pd.date_range(start_date, end_date, freq=segment))
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
