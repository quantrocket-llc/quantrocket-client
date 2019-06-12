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

TMP_DIR = os.environ.get("QUANTROCKET_TMP_DIR", tempfile.gettempdir())

def get_prices(codes, start_date=None, end_date=None,
               universes=None, conids=None,
               exclude_universes=None, exclude_conids=None,
               times=None, fields=None,
               timezone=None, infer_timezone=None,
               cont_fut=None, master_fields=None):
    """
    Query one or more history databases and/or real-time aggregate databases
    and load prices into a DataFrame.

    For bar sizes smaller than 1-day, the resulting DataFrame will have a MultiIndex
    with levels (Field, Date, Time). For bar sizes of 1-day or larger, the MultiIndex
    will have levels (Field, Date).

    Parameters
    ----------
    codes : str or list of str, required
        the code(s) of one or more databases to query. If multiple databases
        are specified, they must have the same bar size. List databses in order of
        priority (highest priority first). If multiple databases provide the same
        field for the same conid on the same datetime, the first database's value will
        be used.

    start_date : str (YYYY-MM-DD), optional
        limit to data on or after this date

    end_date : str (YYYY-MM-DD), optional
        limit to data on or before this date

    universes : list of str, optional
        limit to these universes (default is to return all securities in database)

    conids : list of int, optional
        limit to these conids

    exclude_universes : list of str, optional
        exclude these universes

    exclude_conids : list of int, optional
        exclude these conids

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

    master_fields : list of str, optional
        [DEPRECATED] append these fields from the securities master database (pass ['?'] or any
        invalid fieldname to see available fields). This parameter is deprecated and
        will be removed in a future release. For better performance, use
        `quantrocket.master.get_securities_reindexed_like` to get securities master
        data shaped like prices.

    Returns
    -------
    DataFrame
        a MultiIndex

    Notes
    -----
    The `times` parameter, if provided, is applied differently for history databases vs
    real-time aggregate databases. For history databases, the parameter is applied when
    querying the database. For real-time aggregate databases, the parameter is not applied
    when querying the database; rather, all available times are retrieved and the `times`
    filter is applied to the resulting DataFrame after casting it to the appropriate timezone
    (as inferred from the securities master Timezone field or as explicitly specified with
    the `timezone` parameter). The rationale for this behavior is that history databases store
    intraday data in the timezone of the relevant exchange whereas real-time aggregate
    databases store data in UTC. By applying the `times` filter as described, users can specify
    the times in the timezone of the relevant exchange for both types of databases.

    Examples
    --------
    Load intraday prices:

    >>> prices = get_prices('stk-sample-5min', fields=["Close", "Volume"])
    >>> prices.head()
                                ConId   	265598	38708077
    Field	Date	        Time
    Close	2017-07-26      09:30:00	153.62	2715.0
                                09:35:00	153.46	2730.0
                                09:40:00	153.21	2725.0
                                09:45:00	153.28	2725.0
                                09:50:00	153.18	2725.0

    Isolate the closes:

    >>> closes = prices.loc["Close"]
    >>> closes.head()
                ConId	        265598  38708077
    Date        Time
    2017-07-26	09:30:00	153.62	2715.0
                09:35:00	153.46	2730.0
                09:40:00	153.21	2725.0
                09:45:00	153.28	2725.0
                09:50:00	153.18	2725.0

    Isolate the 15:45:00 prices:

    >>> session_closes = closes.xs("15:45:00", level="Time")
    >>> session_closes.head()
        ConId	265598	38708077
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

    # separate history dbs from realtime dbs
    history_dbs = set(list_history_databases())
    realtime_dbs = list_realtime_databases()
    realtime_agg_dbs = set(itertools.chain(*realtime_dbs.values()))

    history_dbs.intersection_update(set(dbs))
    realtime_agg_dbs.intersection_update(set(dbs))

    unknown_dbs = set(dbs) - history_dbs - realtime_agg_dbs

    if unknown_dbs:
        tick_dbs = set(realtime_dbs.keys()).intersection(unknown_dbs)
        # Improve error message if possible
        if tick_dbs:
            raise ParameterError("{} is a real-time tick database, only history databases or "
                                 "real-time aggregate databases are supported".format(
                                     ", ".join(tick_dbs)))
        raise ParameterError(
            "no history or real-time aggregate databases called {}".format(
                ", ".join(unknown_dbs)))

    db_bar_sizes = set()
    db_bar_sizes_parsed = set()
    db_domains = set()
    history_db_fields = {}
    realtime_db_fields = {}

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
        db_domain = db_config.get("domain", "main")
        db_domains.add(db_domain)
        history_db_fields[db] = db_config.get("fields", [])

    for db in realtime_agg_dbs:
        db_config = get_realtime_db_config(db)
        bar_size = db_config.get("bar_size")
        db_bar_sizes.add(bar_size)
        db_bar_sizes_parsed.add(pd.Timedelta(bar_size))
        # NOTE: aggregate dbs don't include a domain key; if available, this
        # would probably have to be obtained from the associated tick db.
        # This isn't an issue unless/until real-time data comes from multiple
        # vendors. As it now stands, real-time data is always from "main".
        db_domain = db_config.get("domain", "main")
        db_domains.add(db_domain)
        realtime_db_fields[db] = db_config.get("fields", [])

    if len(db_bar_sizes_parsed) > 1:
        raise ParameterError(
            "all databases must contain same bar size but {0} have different "
            "bar sizes: {1}".format(", ".join(dbs), ", ".join(db_bar_sizes))
        )

    if len(db_domains) > 1:
        raise ParameterError(
            "all databases must use the same securities master domain but {0} "
            "use different domains: {1}".format(", ".join(dbs), ", ".join(db_domains))
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
                conids=conids,
                exclude_universes=exclude_universes,
                exclude_conids=exclude_conids,
                times=times,
                cont_fut=cont_fut,
                fields=list(fields_for_db),
                tz_naive=False
            )

            tmp_filepath = "{dir}{sep}history.{db}.{pid}.{time}.csv".format(
                dir=TMP_DIR, sep=os.path.sep, db=db, pid=os.getpid(), time=time.time())

            try:
                download_history_file(db, tmp_filepath, **kwargs)
            except NoHistoricalData as e:
                # don't complain about NoHistoricalData if we're checking
                # multiple databases, unless none of them have data
                if len(dbs) == 1:
                    raise
                else:
                    continue

            prices = pd.read_csv(tmp_filepath)
            all_prices.append(prices)

            os.remove(tmp_filepath)

        if db in realtime_agg_dbs:

            fields_for_db = set(fields).intersection(set(realtime_db_fields[db]))

            kwargs = dict(
                start_date=start_date,
                end_date=end_date,
                universes=universes,
                conids=conids,
                exclude_universes=exclude_universes,
                exclude_conids=exclude_conids,
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
            all_prices.append(prices)

            os.remove(tmp_filepath)

    # complain if multiple dbs and none had data
    if len(dbs) > 1 and not all_prices:
        raise NoHistoricalData("no price data matches the query parameters in any of {0}".format(
            ", ".join(dbs)
        ))

    prices = pd.concat(all_prices, sort=False)

    try:
        prices = prices.pivot(index="ConId", columns="Date").T
    except ValueError as e:
        if "duplicate" not in repr(e):
            raise
        # There are duplicates, likely due to querying multiple databases,
        # both of which return one or more identical dates for identical
        # conids. To resolve, we group by conid and date and take the first
        # available value for each field. This means that the orders of
        # [codes] matters. The use of groupby.first() instead of simply
        # drop_duplicates allows us to retain one field from one db and
        # another field from another db.
        grouped = prices.groupby(["ConId","Date"])
        prices = pd.concat(
            dict([(col, grouped[col].first()) for col in prices.columns
                  if col not in ("ConId","Date")]), axis=1)
        prices = prices.reset_index().pivot(index="ConId", columns="Date").T

    prices.index.set_names(["Field", "Date"], inplace=True)

    master_fields = master_fields or []
    if master_fields:
        import warnings
        # DeprecationWarning is ignored by default but we want the user
        # to see it
        warnings.simplefilter("always", DeprecationWarning)
        warnings.warn(
            "`master_fields` parameter is deprecated and will be removed in a "
            "future release. For better performance, please use "
            "`quantrocket.master.get_securities_reindexed_like` "
            "to get securities master data shaped like prices.", DeprecationWarning)

        if isinstance(master_fields, tuple):
            master_fields = list(master_fields)
        elif not isinstance(master_fields, list):
            master_fields = [master_fields]

    # master fields that are required internally but shouldn't be returned to
    # the user (potentially Timezone)
    internal_master_fields = []

    is_intraday = list(db_bar_sizes_parsed)[0] < pd.Timedelta("1 day")

    if is_intraday and not timezone and infer_timezone is not False:
        infer_timezone = True
        if not master_fields or "Timezone" not in master_fields:
            internal_master_fields.append("Timezone")

    # Next, get the master file
    if master_fields or internal_master_fields:
        conids = list(prices.columns)

        domain = list(db_domains)[0] if db_domains else None

        f = six.StringIO()
        download_master_file(
            f,
            conids=conids,
            fields=master_fields + internal_master_fields,
            domain=domain
        )
        securities = pd.read_csv(f, index_col="ConId")

        if "Delisted" in securities.columns:
            securities.loc[:, "Delisted"] = securities.Delisted.astype(bool)

        if "Etf" in securities.columns:
            securities.loc[:, "Etf"] = securities.Etf.astype(bool)

        # Infer timezone if needed
        if not timezone and infer_timezone:
            timezones = securities.Timezone.unique()

            if len(timezones) > 1:
                raise ParameterError(
                    "cannot infer timezone because multiple timezones are present "
                    "in data, please specify timezone explicitly (timezones: {0})".format(
                        ", ".join(timezones)))

            timezone = timezones[0]

        # Drop any internal-only fields
        if internal_master_fields:
            securities = securities.drop(internal_master_fields, axis=1)

        if not securities.empty:
            # Append securities, indexed to the min date, to allow easy ffill on demand
            securities = pd.DataFrame(securities.T, columns=prices.columns)
            securities.index.name = "Field"
            idx = pd.MultiIndex.from_product(
                (securities.index, [prices.index.get_level_values("Date").min()]),
                names=["Field", "Date"])

            securities = securities.reindex(index=idx, level="Field")
            prices = pd.concat((prices, securities))

    if is_intraday:
        dates = pd.to_datetime(prices.index.get_level_values("Date"), utc=True)

        if timezone:
            dates = dates.tz_convert(timezone)
    else:
        dates = pd.to_datetime(prices.index.get_level_values("Date"))

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

    # Drop time if not intraday
    if not is_intraday:
        prices.index = prices.index.droplevel("Time")
        return prices

    # If intraday, fill missing times so that each date has the same set of
    # times, allowing easier comparisons. Example implications:
    # - if history is retrieved intraday, this ensures that today will have NaN
    #   entries for future times
    # - early close dates will have a full set of times, with NaNs after the
    #   early close
    unique_fields = prices.index.get_level_values("Field").unique()
    unique_dates = prices.index.get_level_values("Date").unique()
    unique_times = prices.index.get_level_values("Time").unique()
    interpolated_index = None
    for field in unique_fields:
        if master_fields and field in master_fields:
            min_date = prices.loc[field].index.min()
            field_idx = pd.MultiIndex.from_tuples([(field,min_date[0], min_date[1])])
        else:
            field_idx = pd.MultiIndex.from_product([[field], unique_dates, unique_times]).sort_values()
        if interpolated_index is None:
            interpolated_index = field_idx
        else:
            interpolated_index = interpolated_index.append(field_idx)

    prices = prices.reindex(interpolated_index)
    prices.index.set_names(["Field", "Date", "Time"], inplace=True)

    # Apply times filter if needed (see Notes in docstring)
    if times and realtime_agg_dbs:
        if not isinstance(times, (list, tuple)):
            times = [times]
        prices = prices.loc[prices.index.get_level_values("Time").isin(times)]

    return prices

