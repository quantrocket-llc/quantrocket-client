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

# To run: python -m unittest discover -s tests/ -p test*.py -t .

import unittest
try:
    from unittest.mock import patch
except ImportError:
    # py27
    from mock import patch
import pandas as pd
import pytz
import numpy as np
from quantrocket.fundamental import (
    get_borrow_fees_reindexed_like,
    get_shortable_shares_reindexed_like
)
from quantrocket.exceptions import ParameterError

class StockloanDataReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=[12345,23456],
            index=pd.MultiIndex.from_product((
                pd.date_range(start="2018-01-01", periods=3, freq="D"),
                ["15:00:00","15:15:00"]), names=["Date", "Time"]))

        with self.assertRaises(ParameterError) as cm:
            get_shortable_shares_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

    def test_complain_if_date_level_not_in_index(self):
        """
        Tests error handling when reindex_like doesn't have an index named
        Date.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-01-01", periods=3, freq="D"))

        with self.assertRaises(ParameterError) as cm:
            get_shortable_shares_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

    def test_complain_if_not_datetime_index(self):
        """
        Tests error handling when the reindex_like index is named Date but is
        not a DatetimeIndex.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_shortable_shares_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_borrow_fees")
    @patch("quantrocket.fundamental.download_shortable_shares")
    def test_pass_conids_and_dates_based_on_reindex_like(self,
                                                         mock_download_shortable_shares,
                                                         mock_download_borrow_fees):
        """
        Tests that conids and date ranges and corrected passed to the
        download_* functions based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     ConId=[12345,
                            12345,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        mock_download_shortable_shares.side_effect = _mock_download_shortable_shares

        get_shortable_shares_reindexed_like(closes, time="00:00:00 America/New_York")

        shortable_shares_call = mock_download_shortable_shares.mock_calls[0]
        _, args, kwargs = shortable_shares_call
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2018-03-17") # 45 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-05-03")

        def _mock_download_borrow_fees(f, *args, **kwargs):
            borrow_fees = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     ConId=[12345,
                            12345,
                            23456],
                     FeeRate=[1.75,
                              1.79,
                              0.35]))
            borrow_fees.to_csv(f, index=False)
            f.seek(0)

        mock_download_borrow_fees.side_effect = _mock_download_borrow_fees

        get_borrow_fees_reindexed_like(closes, time="00:00:00 America/Toronto")

        borrow_fees_call = mock_download_borrow_fees.mock_calls[0]
        _, args, kwargs = borrow_fees_call
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2018-03-17") # 45 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-05-03")

    def test_complain_if_passed_timezone_not_match_reindex_like_timezone(self):
        """
        Tests error handling when a timezone is passed and reindex_like
        timezone is set and they do not match.
        """

        closes = pd.DataFrame(
                    np.random.rand(3,2),
                    columns=[12345,23456],
                    index=pd.date_range(start="2018-05-01",
                                        periods=3,
                                        freq="D",
                                        tz="America/New_York",
                                        name="Date"))

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T23:15:02",
                           "2018-05-03T00:30:03",
                           "2018-05-01T21:45:02",
                           "2018-05-02T23:15:02",
                           "2018-05-03T00:30:03",
                           ],
                     ConId=[12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000,
                               3500,
                               3600,
                               3800
                               ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            with self.assertRaises(ParameterError) as cm:
                get_shortable_shares_reindexed_like(closes, time="09:30:00 Europe/London")

            self.assertIn((
                "cannot use timezone Europe/London because reindex_like timezone is America/New_York, "
                "these must match"), str(cm.exception))

    def test_pass_timezone(self):
        """
        Tests that the UTC timestamps of the shortable shares data are
        correctly interpreted based on the requested timezone.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                name="Date"))

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     ConId=[12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000,
                               3500,
                               3600,
                               3800,
                               3100
                               ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            shortable_shares = get_shortable_shares_reindexed_like(
                closes,
                time="09:30:00 America/New_York")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares.loc[:, "Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', 12345: 10000.0, 23456: 3500.0},
                 {'Date': '2018-05-02T00:00:00', 12345: 80000.0, 23456: 3600.0},
                 {'Date': '2018-05-03T00:00:00', 12345: 80000.0, 23456: 3100.0}]
            )

            shortable_shares = get_shortable_shares_reindexed_like(
                closes,
                time="09:30:00 Europe/London")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares.loc[:, "Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', 12345: 10000.0, 23456: 3500.0},
                 {'Date': '2018-05-02T00:00:00', 12345: 9000.0, 23456: 3600.0},
                 {'Date': '2018-05-03T00:00:00', 12345: 80000.0, 23456: 3100.0}]
            )

            shortable_shares = get_shortable_shares_reindexed_like(
                closes,
                time="09:30:00 Japan")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares.loc[:, "Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', 12345: 10000.0, 23456: 3500.0},
                 {'Date': '2018-05-02T00:00:00', 12345: 9000.0, 23456: 3600.0},
                 {'Date': '2018-05-03T00:00:00', 12345: 80000.0, 23456: 3800.0}]
            )

    def test_use_reindex_like_timezone(self):
        """
        Tests that, when a timezone is not passed but reindex_like timezone
        is set, the latter is used.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     ConId=[12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000,
                               3500,
                               3600,
                               3800,
                               3100
                               ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            shortable_shares = get_shortable_shares_reindexed_like(
                closes,
                time="09:30:00")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares.loc[:, "Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00-0400', 12345: 10000.0, 23456: 3500.0},
                 {'Date': '2018-05-02T00:00:00-0400', 12345: 80000.0, 23456: 3600.0},
                 {'Date': '2018-05-03T00:00:00-0400', 12345: 80000.0, 23456: 3100.0}]
            )

    @patch("quantrocket.fundamental.download_master_file")
    def test_infer_timezone_from_securities(self, mock_download_master_file):
        """
        Tests that, when timezone is not passed and reindex_like timezone is
        not set, the timezone is inferred from the component securities.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456],
                                           Timezone=["Japan","Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     ConId=[12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000,
                               3500,
                               3600,
                               3800,
                               3100
                               ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            shortable_shares = get_shortable_shares_reindexed_like(
                closes,
                time="09:30:00")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares.loc[:, "Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', 12345: 10000.0, 23456: 3500.0},
                 {'Date': '2018-05-02T00:00:00', 12345: 9000.0, 23456: 3600.0},
                 {'Date': '2018-05-03T00:00:00', 12345: 80000.0, 23456: 3800.0}]
            )

    def test_complain_if_cannot_infer_timezone(self):
        """
        Tests error handling when a timezone is not passed, reindex_like
        timezone is not set, and the timezone cannot be inferred from the
        securities master because there are multiple timezones among the
        component securities.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456],
                                           Timezone=["America/New_York","Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     ConId=[12345,
                            12345,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):
            with patch("quantrocket.fundamental.download_master_file", new=mock_download_master_file):

                with self.assertRaises(ParameterError) as cm:
                    get_shortable_shares_reindexed_like(closes)

                    self.assertIn((
                        "no timezone specified and cannot infer because multiple timezones are "
                        "present in data, please specify timezone (timezones in data: America/New_York, Japan)"
                        ), str(cm.exception))

    def test_invalid_timezone(self):
        """
        Tests error handling when an invalid timezone is passed.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D",
                                name="Date"))

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     ConId=[12345,
                            12345,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            with self.assertRaises(pytz.exceptions.UnknownTimeZoneError) as cm:
                get_shortable_shares_reindexed_like(closes, time="09:30:00 Mars")

                self.assertIn("pytz.exceptions.UnknownTimeZoneError: 'Mars'", repr(cm.exception))

    def test_invalid_time(self):
        """
        Tests error handling when an invalid time is passed.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", tz="America/New_York",
                                name="Date"))

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     ConId=[12345,
                            12345,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            with self.assertRaises(ParameterError) as cm:
                get_shortable_shares_reindexed_like(closes, time="foo")

                self.assertIn("could not parse time 'foo': could not convert string to Timestamp", str(cm.exception))


    def test_pass_time(self):
        """
        Tests that, when a time arg is passed, it is used.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     ConId=[12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000,
                               3500,
                               3600,
                               3800,
                               3100
                               ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            shortable_shares = get_shortable_shares_reindexed_like(
                closes,
                time="09:30:00")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares.loc[:, "Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00-0400', 12345: 10000.0, 23456: 3500.0},
                 {'Date': '2018-05-02T00:00:00-0400', 12345: 80000.0, 23456: 3600.0},
                 {'Date': '2018-05-03T00:00:00-0400', 12345: 80000.0, 23456: 3100.0}]
            )

    def test_no_pass_time(self):
        """
        Tests that, when no time arg is passed, the reindex_like times are
        used, which for a date index are 00:00:00.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     ConId=[12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
                            23456],
                     Quantity=[10000,
                               9000,
                               80000,
                               3500,
                               3600,
                               3800,
                               3100
                               ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            shortable_shares = get_shortable_shares_reindexed_like(closes)
            shortable_shares = shortable_shares.reset_index()
            shortable_shares.loc[:, "Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00-0400', 12345: 10000.0, 23456: 3500.0},
                 {'Date': '2018-05-02T00:00:00-0400', 12345: 9000.0, 23456: 3600.0},
                 {'Date': '2018-05-03T00:00:00-0400', 12345: 80000.0, 23456: 3800.0}]
            )

    def test_fillna_0_after_start_date(self):
        """
        Tests that NaN data after 2018-04-15 is converted to 0 but NaN data
        before is not.
        """
        closes = pd.DataFrame(
            np.random.rand(5,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-04-13",
                                periods=5,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-15T21:45:02",
                           "2018-04-16T13:45:02",
                           "2018-04-17T12:30:03",
                           ],
                     ConId=[12345,
                            12345,
                            12345,
                           ],
                     Quantity=[10000,
                               9000,
                               80000,
                               ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_shortable_shares', new=mock_download_shortable_shares):

            shortable_shares = get_shortable_shares_reindexed_like(closes)
            # replace nan with "nan" to allow equality comparisons
            shortable_shares = shortable_shares.where(shortable_shares.notnull(), "nan")
            shortable_shares = shortable_shares.reset_index()
            shortable_shares.loc[:, "Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-04-13T00:00:00-0400', 12345: "nan", 23456: "nan"},
                 {'Date': '2018-04-14T00:00:00-0400', 12345: "nan", 23456: "nan"},
                 {'Date': '2018-04-15T00:00:00-0400', 12345: "nan", 23456: "nan"},
                 {'Date': '2018-04-16T00:00:00-0400', 12345: 10000.0, 23456: 0.0},
                 {'Date': '2018-04-17T00:00:00-0400', 12345: 9000.0, 23456: 0.0}]
            )

    def test_borrow_fees(self):
        """
        Tests get_borrow_fees_reindexed_like. (get_borrow_fees_reindexed_like
        and get_shortable_shares_reindexed_like share a base function so for
        the most part testing one tests both.)
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_borrow_fees(f, *args, **kwargs):
            borrow_fees = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     ConId=[12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
                            23456],
                     FeeRate=[1.5,
                               1.65,
                               1.7,
                               0.35,
                               0.40,
                               0.44,
                               0.23
                               ]))
            borrow_fees.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_borrow_fees', new=mock_download_borrow_fees):

            borrow_fees = get_borrow_fees_reindexed_like(
                closes,
                time="09:30:00")

            borrow_fees = borrow_fees.reset_index()
            borrow_fees.loc[:, "Date"] = borrow_fees.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                borrow_fees.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00-0400', 12345: 1.5, 23456: 0.35},
                 {'Date': '2018-05-02T00:00:00-0400', 12345: 1.7, 23456: 0.40},
                 {'Date': '2018-05-03T00:00:00-0400', 12345: 1.7, 23456: 0.23}]
            )
