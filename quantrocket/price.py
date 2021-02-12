# Copyright 2019 QuantRocket - All Rights Reserved
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

import six
import os
import time
import itertools
import tempfile
import requests
from quantrocket.master import download_master_file
from quantrocket.exceptions import ParameterError, NoHistoricalData, NoRealtimeData
from quantrocket.history import (
    download_history_file,
    get_db_config as get_history_db_config,
    list_databases as list_history_databases)
from quantrocket.realtime import (
    download_market_data_file,
    get_db_config as get_realtime_db_config,
    list_databases as list_realtime_databases)
from quantrocket.zipline import (
    list_bundles,
    get_bundle_config,
    download_bundle_file)

TMP_DIR = os.environ.get("QUANTROCKET_TMP_DIR", tempfile.gettempdir())

def get_prices(codes, start_date=None, end_date=None,
               universes=None, sids=None,
               exclude_universes=None, exclude_sids=None,
               times=None, fields=None,
               timezone=None, infer_timezone=None,
               cont_fut=None, data_frequency=None):
    """
    Query one or more history databases, real-time aggregate databases,
    and/or Zipline bundles and load prices into a DataFrame.

    For bar sizes smaller than 1-day, the resulting DataFrame will have a MultiIndex
    with levels (Field, Date, Time). For bar sizes of 1-day or larger, the MultiIndex
    will have levels (Field, Date).

    Parameters
    ----------
    codes : str or list of str, required
        the code(s) of one or more databases to query. If multiple databases
        are specified, they must have the same bar size. List databases in order of
        priority (highest priority first). If multiple databases provide the same
        field for the same sid on the same datetime, the first database's value will
        be used.

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this date

    universes : list of str, optional
        limit to these universes (default is to return all securities in database)

    sids : list of str, optional
        limit to these sids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_sids : list of str, optional
        exclude these sids

    times: list of str (HH:MM:SS), optional
        limit to these times, specified in the timezone of the relevant exchange. See
        additional information in the Notes section regarding the timezone to use.

    fields : list of str, optional
        only return these fields. (If querying multiple databases that have different fields,
        provide the complete list of desired fields; only the supported fields for each
        database will be queried.)

    timezone : str, optional
        convert timestamps to this timezone, for example America/New_York (see
        `pytz.all_timezones` for choices); ignored for non-intraday bar sizes

    infer_timezone : bool
        infer the timezone from the securities master Timezone field; defaults to
        True if using intraday bars and no `timezone` specified; ignored for
        non-intraday bars, or if `timezone` is passed

    cont_fut : str
        stitch futures into continuous contracts using this method (default is not
        to stitch together). Only applicable to history databases. Possible choices:
        concat

    data_frequency : str
        for Zipline bundles, whether to query minute or daily data. If omitted,
        defaults to minute data for minute bundles and to daily data for daily bundles.
        This parameter only needs to be set to request daily data from a minute bundle.
        Possible choices: daily, minute (or aliases d, m).

    Returns
    -------
    DataFrame
        a MultiIndex (Field, Date) or (Field, Date, Time) DataFrame of prices

    Notes
    -----
    The `times` parameter, if provided, is applied differently for history databases and
    Zipline bundles vs real-time aggregate databases. For history databases and Zipline
    bundles, the parameter is applied when querying the database. For real-time aggregate
    databases, the parameter is not applied when querying the database; rather, all available
    times are retrieved and the `times` filter is applied to the resulting DataFrame after
    casting it to the appropriate timezone (as inferred from the securities master Timezone
    field or as explicitly specified with the `timezone` parameter). The rationale for this
    behavior is that history databases and Zipline bundles store intraday data in the timezone
    of the relevant exchange whereas real-time aggregate databases store data in UTC. By
    applying the `times` filter as described, users can specify the times in the timezone of
    the relevant exchange for both types of databases.

    Examples
    --------
    Load intraday prices:

    >>> prices = get_prices('stk-sample-5min', fields=["Close", "Volume"])
    >>> prices.head()
                                Sid     	FIBBG1	FIBBG2
    Field	Date	        Time
    Close	2017-07-26      09:30:00	153.62	2715.0
                                09:35:00	153.46	2730.0
                                09:40:00	153.21	2725.0
                                09:45:00	153.28	2725.0
                                09:50:00	153.18	2725.0

    Isolate the closes:

    >>> closes = prices.loc["Close"]
    >>> closes.head()
                Sid	        FIBBG1	FIBBG2
    Date        Time
    2017-07-26	09:30:00	153.62	2715.0
                09:35:00	153.46	2730.0
                09:40:00	153.21	2725.0
                09:45:00	153.28	2725.0
                09:50:00	153.18	2725.0

    Isolate the 15:45:00 prices:

    >>> session_closes = closes.xs("15:45:00", level="Time")
    >>> session_closes.head()
        Sid	FIBBG1	FIBBG2
    Date
    2017-07-26	153.29	2700.00
    2017-07-27 	150.10	2660.00
    2017-07-28	149.43	2650.02
    2017-07-31 	148.99	2650.34
    2017-08-01 	149.72	2675.50
    """
    # Import pandas lazily since it can take a moment to import
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    try:
        import pytz
    except ImportError:
        raise ImportError("pytz must be installed to use this function")

    if timezone and timezone not in pytz.all_timezones:
        raise ParameterError(
            "invalid timezone: {0} (see `pytz.all_timezones` for choices)".format(
                timezone))

    dbs = codes
    if not isinstance(dbs, (list, tuple)):
        dbs = [dbs]

    fields = fields or []
    if not isinstance(fields, (list, tuple)):
        fields = [fields]

    # separate history dbs from Zipline bundles from realtime dbs; in case one or
    # more of the services is not running, we print a warning and try the other
    # services
    try:
        history_dbs = set(list_history_databases())
    except requests.HTTPError as e:
        if e.response.status_code == 502:
            import warnings
            warnings.warn(
                f"Error while checking if {', '.join(dbs)} is a history database, "
                f"will assume it's not. Error was: {e}", RuntimeWarning)
            history_dbs = set()
        else:
            raise

    try:
        realtime_dbs = list_realtime_databases()
    except requests.HTTPError as e:
        if e.response.status_code == 502:
            import warnings
            warnings.warn(
                f"Error while checking if {', '.join(dbs)} is a realtime database, "
                f"will assume it's not. Error was: {e}", RuntimeWarning)
            realtime_dbs = {}
            realtime_agg_dbs = set()
        else:
            raise
    else:
        realtime_agg_dbs = set(itertools.chain(*realtime_dbs.values()))

    try:
        zipline_bundles = set(list_bundles())
    except requests.HTTPError as e:
        if e.response.status_code == 502:
            import warnings
            warnings.warn(
                f"Error while checking if {', '.join(dbs)} is a Zipline bundle, "
                f"will assume it's not. Error was: {e}", RuntimeWarning)
            zipline_bundles = set()
        else:
            raise

    history_dbs.intersection_update(set(dbs))
    realtime_agg_dbs.intersection_update(set(dbs))
    zipline_bundles.intersection_update(set(dbs))

    unknown_dbs = set(dbs) - history_dbs - realtime_agg_dbs - zipline_bundles

    if unknown_dbs:
        tick_dbs = set(realtime_dbs.keys()).intersection(unknown_dbs)
        # Improve error message if possible
        if tick_dbs:
            raise ParameterError("{} is a real-time tick database, only history databases or "
                                 "real-time aggregate databases are supported".format(
                                     ", ".join(tick_dbs)))
        raise ParameterError(
            "no history or real-time aggregate databases or Zipline bundles called {}".format(
                ", ".join(unknown_dbs)))

    db_bar_sizes = set()
    db_bar_sizes_parsed = set()
    history_db_fields = {}
    realtime_db_fields = {}
    zipline_bundle_fields = {}

    for db in history_dbs:
        db_config = get_history_db_config(db)
        bar_size = db_config.get("bar_size")
        db_bar_sizes.add(bar_size)
        # to validate uniform bar sizes, we need to parse them in case dbs
        # store different but equivalent timedelta strings. History db
        # strings may need massaging to be parsable.
        if bar_size.endswith("s"):
            # strip s from secs, mins, hours to get valid pandas timedelta
            bar_size = bar_size[:-1]
        elif bar_size == "1 week":
            bar_size = "7 day"
        elif bar_size == "1 month":
            bar_size = "30 day"
        db_bar_sizes_parsed.add(pd.Timedelta(bar_size))
        history_db_fields[db] = db_config.get("fields", [])

    for db in realtime_agg_dbs:
        db_config = get_realtime_db_config(db)
        bar_size = db_config.get("bar_size")
        db_bar_sizes.add(bar_size)
        db_bar_sizes_parsed.add(pd.Timedelta(bar_size))
        realtime_db_fields[db] = db_config.get("fields", [])

    for db in zipline_bundles:
        # look up bundle data_frequency if not specified
        if not data_frequency:
            bundle_config = get_bundle_config(db)
            data_frequency = bundle_config["data_frequency"]

        if data_frequency in ("daily", "d"):
            db_bar_sizes.add("1 day")
            db_bar_sizes_parsed.add(pd.Timedelta("1 day"))
        elif data_frequency in ("minute", "m"):
            db_bar_sizes.add("1 min")
            db_bar_sizes_parsed.add(pd.Timedelta("1 min"))
        else:
            raise ParameterError("invalid data_frequency: {}".format(data_frequency))
        zipline_bundle_fields[db] = ["Open", "High", "Low", "Close", "Volume"]

    if len(db_bar_sizes_parsed) > 1:
        raise ParameterError(
            "all databases must contain same bar size but {0} have different "
            "bar sizes: {1}".format(", ".join(dbs), ", ".join(db_bar_sizes))
        )

    all_prices = []

    for db in dbs:

        if db in history_dbs:
            # different DBs might support different fields so only request the
            # subset of supported fields
            fields_for_db = set(fields).intersection(set(history_db_fields[db]))

            kwargs = dict(
                start_date=start_date,
                end_date=end_date,
                universes=universes,
                sids=sids,
                exclude_universes=exclude_universes,
                exclude_sids=exclude_sids,
                times=times,
                cont_fut=cont_fut,
                fields=list(fields_for_db),
            )

            tmp_filepath = "{dir}{sep}history.{db}.{pid}.{time}.csv".format(
                dir=TMP_DIR, sep=os.path.sep, db=db, pid=os.getpid(), time=time.time())

            try:
                download_history_file(db, tmp_filepath, **kwargs)
            except NoHistoricalData:
                # don't complain about NoHistoricalData if we're checking
                # multiple databases, unless none of them have data
                if len(dbs) == 1:
                    raise
                else:
                    continue

            prices = pd.read_csv(tmp_filepath)
            prices = prices.pivot(index="Sid", columns="Date").T
            prices.index.set_names(["Field", "Date"], inplace=True)
            all_prices.append(prices)

            os.remove(tmp_filepath)

        if db in realtime_agg_dbs:

            fields_for_db = set(fields).intersection(set(realtime_db_fields[db]))

            kwargs = dict(
                start_date=start_date,
                end_date=end_date,
                universes=universes,
                sids=sids,
                exclude_universes=exclude_universes,
                exclude_sids=exclude_sids,
                fields=list(fields_for_db))

            tmp_filepath = "{dir}{sep}realtime.{db}.{pid}.{time}.csv".format(
                dir=TMP_DIR, sep=os.path.sep, db=db, pid=os.getpid(), time=time.time())

            try:
                download_market_data_file(db, tmp_filepath, **kwargs)
            except NoRealtimeData as e:
                # don't complain about NoRealtimeData if we're checking
                # multiple databases, unless none of them have data
                if len(dbs) == 1:
                    raise
                else:
                    continue

            prices = pd.read_csv(tmp_filepath)
            prices = prices.pivot(index="Sid", columns="Date").T
            prices.index.set_names(["Field", "Date"], inplace=True)
            all_prices.append(prices)

            os.remove(tmp_filepath)

        if db in zipline_bundles:

            fields_for_db = set(fields).intersection(set(zipline_bundle_fields[db]))

            kwargs = dict(
                start_date=start_date,
                end_date=end_date,
                universes=universes,
                sids=sids,
                exclude_universes=exclude_universes,
                exclude_sids=exclude_sids,
                times=times,
                data_frequency=data_frequency,
                fields=list(fields_for_db))

            tmp_filepath = "{dir}{sep}zipline.{db}.{pid}.{time}.csv".format(
                dir=TMP_DIR, sep=os.path.sep, db=db, pid=os.getpid(), time=time.time())

            try:
                download_bundle_file(db, tmp_filepath, **kwargs)
            except NoHistoricalData as e:
                # don't complain about NoHistoricalData if we're checking
                # multiple databases, unless none of them have data
                if len(dbs) == 1:
                    raise
                else:
                    continue

            prices = pd.read_csv(tmp_filepath, index_col=["Field", "Date"])
            prices.columns.name = "Sid"
            all_prices.append(prices)

            os.remove(tmp_filepath)

    # complain if multiple dbs and none had data
    if len(dbs) > 1 and not all_prices:
        raise NoHistoricalData("no price data matches the query parameters in any of {0}".format(
            ", ".join(dbs)
        ))

    prices = None
    for _prices in all_prices:
        if prices is None:
            prices = _prices
        else:
            prices = prices.combine_first(_prices)

    is_intraday = list(db_bar_sizes_parsed)[0] < pd.Timedelta("1 day")

    # For intraday dbs, infer timezone from securities master
    if is_intraday and not timezone and infer_timezone is not False:

        infer_timezone = True
        sids = list(prices.columns)

        f = six.StringIO()
        download_master_file(
            f,
            sids=sids,
            fields="Timezone")
        securities = pd.read_csv(f, index_col="Sid")

        timezones = securities.Timezone.unique()

        if len(timezones) > 1:
            raise ParameterError(
                "cannot infer timezone because multiple timezones are present "
                "in data, please specify timezone explicitly (timezones: {0})".format(
                    ", ".join(timezones)))

        timezone = timezones[0]

    if is_intraday:
        dates = pd.to_datetime(prices.index.get_level_values("Date"), utc=True)

        if timezone:
            dates = dates.tz_convert(timezone)
    else:
        # use .str[:10] because the format might be 2020-04-05 (history dbs)
        # or 2020-04-05T00:00:00-00 (realtime aggregate dbs)
        dates = pd.to_datetime(
            prices.index.get_level_values("Date").str[:10])

    prices.index = pd.MultiIndex.from_arrays((
        prices.index.get_level_values("Field"),
        dates
        ), names=("Field", "Date"))

    # Split date and time
    dts = prices.index.get_level_values("Date")
    dates = pd.to_datetime(dts.date).tz_localize(None) # drop tz-aware in Date index
    prices.index = pd.MultiIndex.from_arrays(
        (prices.index.get_level_values("Field"),
         dates,
         dts.strftime("%H:%M:%S")),
        names=["Field", "Date", "Time"]
    )

    # Align dates if there are any duplicate. Explanation: Suppose there are
    # two timezones represented in the data (e.g. history db in security
    # timezone vs real-time db in UTC). After parsing these dates into a
    # common timezone, they will align properly, but we pivoted before
    # parsing the dates (for performance reasons), so they may not be
    # aligned. Thus we need to dedupe the index.
    prices = prices.groupby(prices.index).first()
    prices.index = pd.MultiIndex.from_tuples(prices.index)
    prices.index.set_names(["Field", "Date", "Time"], inplace=True)

    # Fill missing dates and times so that each field has the
    # same set of dates and times, for easier vectorized operations.
    # Example implications for intraday dbs:
    # - if history is retrieved intraday, this ensures that today will have NaN
    #   entries for future times
    # - early close dates will have a full set of times, with NaNs after the
    #   early close
    unique_fields = prices.index.get_level_values("Field").unique()
    unique_dates = prices.index.get_level_values("Date").unique()
    unique_times = prices.index.get_level_values("Time").unique()
    interpolated_index = None
    for field in unique_fields:
        field_idx = pd.MultiIndex.from_product([[field], unique_dates, unique_times]).sort_values()
        if interpolated_index is None:
            interpolated_index = field_idx
        else:
            interpolated_index = interpolated_index.append(field_idx)

    prices = prices.reindex(interpolated_index)
    prices.index.set_names(["Field", "Date", "Time"], inplace=True)

    # Drop time if not intraday
    if not is_intraday:
        prices.index = prices.index.droplevel("Time")
        return prices

    # Apply times filter if needed (see Notes in docstring)
    if times and realtime_agg_dbs:
        if not isinstance(times, (list, tuple)):
            times = [times]
        prices = prices.loc[prices.index.get_level_values("Time").isin(times)]

    return prices

def get_prices_reindexed_like(reindex_like, codes, fields=None,
                              shift=1, ffill=True, lookback_window=10,
                              agg="last",
                              timezone=None, infer_timezone=None,
                              times=None, cont_fut=None,
                              data_frequency=None):
    """
    Return a multiindex (Field, Date) DataFrame of prices for one or more history
    databases, real-time aggregate databases, or Zipline bundles, reindexed to match
    the index (dates) and columns (sids) of `reindex_like`.

    Prices are loaded with `quantrocket.get_prices` and shifted forward one period
    (configurable with the `shift` parameter) to avoid lookahead bias. In the case
    of sparse data, values are then forward-filled by default (configurable with the
    `ffill` parameter).

    The queried databases need not contain price data. This function can be used to query
    custom history databases containing any kind of data.

    Pay attention to the `lookback_window` parameter, which controls how much back data
    in advance of the input DataFrame's start date to load. For example, if the input
    DataFrame contains daily data and you are querying quarterly fundamental data,
    the `lookback_window` must extend far enough back in time to access the most recent
    quarterly value prior to the input DataFrame's start date. Setting `lookback_window`
    too low will result in leading NaNs in the resulting DataFrame. Setting it too high
    is okay but will load a larger amount of data into memory.

    If you query an intraday database, it will be treated as a daily database. Specifically,
    the intraday values will be aggregated (using the method specified by the `agg`
    parameter) to produce a single value per day.

    If the input DataFrame has levels in the index other than Date (for example, Time, in
    the case of an intraday input DataFrame), the queried values will be broadcast across
    the additional levels of the index.

    Parameters
    ----------
    reindex_like : DataFrame, required
        a DataFrame with dates for the index (or, if DataFrame has a MultiIndex, with
        dates for one level of the index) and sids for the columns, to which the shape
        of the resulting DataFrame will be conformed.

    codes : str or list of str, required
        the code(s) of one or more databases to query. If multiple databases
        are specified, they must have the same bar size. List databases in order of
        priority (highest priority first). If multiple databases provide the same
        field for the same sid on the same datetime, the first database's value will
        be used.

    fields : list of str, optional
        only return these fields. (If querying multiple databases that have different fields,
        provide the complete list of desired fields; only the supported fields for each
        database will be queried.)

    shift : int, optional
        number of periods (in the date index) to shift the resulting data forward to
        avoid lookahead bias. Default is 1. Shifting one period implies that data
        timestamped to a particular date is available and actionable on the following
        date.

    ffill : bool
        forward-fill values in the resulting DataFrame so that each date reflects
        the latest available value as of that date. If False, values appear only
        on the first date they were available, followed by NaNs. Default True.

    lookback_window : int, optional
        how many calendar days of back data prior to the reindex_like start date
        should be loaded, to ensure an adequate cushion of data is available before
        shifting. Default is 10. Sparse data such as fundamentals will require a
        higher value.

    times: list of str (HH:MM:SS), optional
        limit to these times, specified in the timezone of the relevant exchange. Only
        applicable to querying intraday databases. See additional information in the
        Notes section of `quantrocket.get_prices` regarding the timezone to use.

    timezone : str, optional
        convert timestamps to this timezone, for example America/New_York (see
        `pytz.all_timezones` for choices); ignored for non-intraday databases

    infer_timezone : bool
        infer the timezone from the securities master Timezone field; defaults to
        True if querying intraday databases and no `timezone` specified; ignored for
        non-intraday databases, or if `timezone` is passed

    agg : str or function, optional
        when querying intraday databases, how to aggregate each day's intraday values to
        produce a single value per day. Default is "last", meaning use the last non-null
        value of each day. This parameter is passed directly to the pandas `agg` function.
        Example choices include "last", "first", "min", "max", "mean", "sum", etc. See
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.core.groupby.DataFrameGroupBy.aggregate.html
        for more info. Note that aggregation occurs after the `times` filters are applied.

    cont_fut : str
        stitch futures into continuous contracts using this method (default is not
        to stitch together). Only applicable to history databases. Possible choices:
        concat

    data_frequency : str
        for Zipline bundles, whether to query minute or daily data. If omitted,
        defaults to minute data for minute bundles and to daily data for daily bundles.
        This parameter only needs to be set to request daily data from a minute bundle.
        Possible choices: daily, minute (or aliases d, m).

    Returns
    -------
    DataFrame
        a MultiIndex DataFrame of prices shaped like reindex_like

    Examples
    --------
    Use a DataFrame of closing prices to query a DataFrame of fundamentals in a
    database called "custom-fundamentals". Since the fundamental data is sparse,
    we specify a lookback window of 180 days to ensure that a previous value can
    be forward-filled into the resulting DataFrame:

    >>> closes = prices.loc["Close"]
    >>> fundamentals = get_prices_reindexed_like(
        closes, "custom-fundamentals", fields="Revenue",
        lookback_window=180)
    >>> revenues = fundamentals.loc["Revenue"]
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas must be installed to use this function")

    index_levels = reindex_like.index.names

    if "Date" not in index_levels:
        raise ParameterError(
            "reindex_like must have index called 'Date', but has {0}".format(
                ",".join([str(name) for name in index_levels])))

    reindex_like_dates = reindex_like.index.get_level_values("Date")

    is_multiindex = len(reindex_like.index.names) > 1

    if not hasattr(reindex_like_dates, "date"):
        raise ParameterError("reindex_like must have a DatetimeIndex")

    sids = list(reindex_like.columns)
    start_date = reindex_like_dates.min().date()
    start_date -= pd.Timedelta(days=lookback_window)
    start_date = start_date.isoformat()
    end_date = reindex_like_dates.max().date().isoformat()

    prices = get_prices(
        codes,
        sids=sids,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
        timezone=timezone,
        times=times,
        cont_fut=cont_fut,
        data_frequency=data_frequency)

    all_fields = {}

    # default values are set in the signature for docstring purposes, but we
    # also set them here in case Nones were passed
    if shift is None:
        shift = 1
    if lookback_window is None:
        lookback_window = 10
    if agg is None:
        agg = "last"

    fields = prices.index.get_level_values("Field").unique()

    for field in fields:

        prices_for_field = prices.loc[field]

        # For intraday databases, drop Time and aggregate per date
        if "Time" in prices_for_field.index.names:
            prices_for_field.index = prices_for_field.index.get_level_values("Date")
            prices_for_field = prices_for_field.groupby(prices_for_field.index).agg(agg)

        # get_prices returns tz-naive dates, localize to match reindex_like
        prices_for_field = prices_for_field.tz_localize(reindex_like_dates.tz)

        # union indexes and reindex in case there are any price dates not in
        # reindex_like or vice versa
        unioned_idx = reindex_like_dates.union(prices_for_field.index).drop_duplicates()
        prices_for_field = prices_for_field.reindex(index=unioned_idx)

        if ffill:
            prices_for_field = prices_for_field.fillna(method="ffill")

        # shift forward to avoid lookahead bias
        prices_for_field = prices_for_field.shift(shift)

        # Reindex to requested shape
        if is_multiindex:
            prices_for_field = prices_for_field.reindex(
                index=reindex_like.index, level="Date").reindex(
                    columns=reindex_like.columns)
        else:
            prices_for_field = prices_for_field.reindex(
                index=reindex_like.index, columns=reindex_like.columns)

        all_fields[field] = prices_for_field

    names = ["Field"] + reindex_like.index.names
    prices = pd.concat(all_fields, names=names)

    return prices
