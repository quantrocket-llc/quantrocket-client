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
    get_reuters_estimates_reindexed_like,
    get_reuters_financials_reindexed_like,
    get_borrow_fees_reindexed_like,
    get_shortable_shares_reindexed_like,
    get_sharadar_fundamentals_reindexed_like,
    get_wsh_earnings_dates_reindexed_like
)
from quantrocket.exceptions import ParameterError, MissingData

class ReutersEstimatesReindexedLikeTestCase(unittest.TestCase):

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
            get_reuters_estimates_reindexed_like(closes, codes="BVPS")

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
            get_reuters_estimates_reindexed_like(closes, codes="BVPS")

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
            get_reuters_estimates_reindexed_like(closes, codes="BVPS")

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_reuters_estimates")
    @patch("quantrocket.fundamental.download_master_file")
    def test_pass_args_correctly(self,
                                 mock_download_master_file,
                                 mock_download_reuters_estimates):
        """
        Tests that conids, date ranges, and and other args are correctly
        passed to download_reuters_estimates.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def _mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-06-30",
                        "2018-06-30"
                        ],
                    UpdatedDate=[
                        "2018-04-06T10:00:00",
                        "2018-04-06T10:00:00",
                        "2018-04-23T13:00:00",
                        "2018-04-23T13:00:00",
                        "2018-07-23T13:00:00",
                        "2018-07-23T13:00:00",
                        ],
                     ConId=[
                         12345,
                         12345,
                         23456,
                         23456,
                         12345,
                         12345,
                         ],
                     Indicator=[
                         "BVPS",
                         "EPS",
                         "BVPS",
                         "EPS",
                         "BVPS",
                         "EPS"
                         ],
                     Actual=[
                         20,
                         9.56,
                         50,
                         63.22,
                         24.5,
                         11.35
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456],
                                               Timezone=["Japan","Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        mock_download_reuters_estimates.side_effect = _mock_download_reuters_estimates

        get_reuters_estimates_reindexed_like(
            closes, ["BVPS","EPS"], fields=["Actual", "FiscalPeriodEndDate"],
            period_types=["Q"], max_lag="500D")

        reuters_estimates_call = mock_download_reuters_estimates.mock_calls[0]
        _, args, kwargs = reuters_estimates_call
        self.assertListEqual(args[0], ["BVPS", "EPS"])
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2016-09-02") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")
        self.assertEqual(kwargs["fields"], ["Actual", "FiscalPeriodEndDate", "UpdatedDate"])
        self.assertEqual(kwargs["period_types"], ["Q"])

        master_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = master_call
        self.assertEqual(kwargs["conids"], [12345,23456])

        get_reuters_estimates_reindexed_like(
            closes, ["BVPS", "EPS", "ROA"], fields=["Actual", "Mean"],
            period_types=["A","S"], max_lag="500D")

        reuters_estimates_call = mock_download_reuters_estimates.mock_calls[1]
        _, args, kwargs = reuters_estimates_call
        self.assertListEqual(args[0], ["BVPS", "EPS", "ROA"])
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2016-09-02") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")
        self.assertEqual(kwargs["fields"], ["Actual", "Mean","UpdatedDate"])
        self.assertEqual(kwargs["period_types"], ["A","S"])

        master_call = mock_download_master_file.mock_calls[1]
        _, args, kwargs = master_call
        self.assertEqual(kwargs["conids"], [12345,23456])

    def test_dedupe_updated_date(self):
        """
        Tests that duplicate UpdatedDates (resulting from reporting several
        fiscal periods at once) are deduped by keeping the latest record.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-31",
                        "2018-06-30",
                        ],
                    UpdatedDate=[
                        "2018-07-23T10:00:00",
                        "2018-07-23T10:00:00",
                        ],
                     ConId=[
                         12345,
                         12345,
                         ],
                     Indicator=[
                         "EPS",
                         "EPS"
                         ],
                     Actual=[
                         9.56,
                         11.35
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345],
                                           Timezone=["Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                estimates = get_reuters_estimates_reindexed_like(
                    closes, "EPS", period_types="Q")

        self.assertSetEqual(set(estimates.index.get_level_values("Indicator")), {"EPS"})
        self.assertSetEqual(set(estimates.index.get_level_values("Field")), {"Actual"})

        eps = estimates.loc["EPS"].loc["Actual"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        self.assertEqual(eps[12345].loc["2018-08-01"], 11.35)

    def test_ffill_no_lookahead_bias(self):
        """
        Tests that indicators are ffilled and are shifted forward 1 period to
        avoid lookahead bias.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-07-20", periods=6, freq="D", name="Date"))

        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-30",
                        "2018-06-30"
                        ],
                    UpdatedDate=[
                        "2018-04-23T10:00:00",
                        "2018-07-23T10:00:00",
                        ],
                     ConId=[
                         12345,
                         12345,
                         ],
                     Indicator=[
                         "EPS",
                         "EPS",
                         ],
                     Actual=[
                         13.45,
                         16.34
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345],
                                           Timezone=["America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["EPS"])

        self.assertSetEqual(set(estimates.index.get_level_values("Indicator")), {"EPS"})
        self.assertSetEqual(set(estimates.index.get_level_values("Field")), {"Actual"})

        eps = estimates.loc["EPS"].loc["Actual"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        self.assertEqual(eps[12345].loc["2018-07-23"], 13.45)
        self.assertEqual(eps[12345].loc["2018-07-24"], 16.34)

    def test_no_shift(self):
        """
        Tests that indicators are not shifted forward 1 period if shift=False.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-07-20", periods=6, freq="D", name="Date"))

        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-30",
                        "2018-06-30"
                        ],
                    UpdatedDate=[
                        "2018-04-23T10:00:00",
                        "2018-07-23T10:00:00",
                        ],
                     ConId=[
                         12345,
                         12345,
                         ],
                     Indicator=[
                         "EPS",
                         "EPS",
                         ],
                     Actual=[
                         13.45,
                         16.34
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345],
                                           Timezone=["America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["EPS"], shift=False)

        self.assertSetEqual(set(estimates.index.get_level_values("Indicator")), {"EPS"})
        self.assertSetEqual(set(estimates.index.get_level_values("Field")), {"Actual"})

        eps = estimates.loc["EPS"].loc["Actual"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        self.assertEqual(eps[12345].loc["2018-07-22"], 13.45)
        self.assertEqual(eps[12345].loc["2018-07-23"], 16.34)

    def test_no_ffill(self):
        """
        Tests that indicators are not forward-filled if ffill=False.
        """
        closes = pd.DataFrame(
            np.random.rand(6,3),
            columns=[12345, 23456, 34567],
            index=pd.DatetimeIndex(["2018-07-22", "2018-07-23","2018-07-24",
                                    "2018-07-27","2018-07-28","2018-07-29"], name="Date"))

        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-30",
                        "2018-06-30",
                        "2018-03-30",
                        "2018-06-30",
                        "2018-06-30",
                        "2018-06-30",
                        "2018-06-30"
                        ],
                    UpdatedDate=[
                        "2018-04-23T10:00:00",
                        "2018-07-23T10:00:00",
                        "2018-04-25T10:00:00",
                        "2018-07-25T10:00:00",
                        "2018-07-26T10:00:00",
                        "2018-07-26T10:00:00",
                        "2018-07-28T10:00:00",
                        ],
                     ConId=[
                         12345,
                         12345,
                         23456,
                         23456,
                         34567,
                         12345,
                         23456
                         ],
                     Indicator=[
                         "EPS",
                         "EPS",
                         "EPS",
                         "EPS",
                         "EPS",
                         "BVPS",
                         "BVPS",
                         ],
                     Mean=[
                         13.50,
                         15.67,
                         None,
                         10.03,
                         1.00,
                         42.34,
                         24.56
                         ],
                     Actual=[
                         13.45,
                         16.34,
                         9.45,
                         10.04,
                         0.56,
                         45.34,
                         21.34
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456,34567],
                                           Timezone=["America/New_York","America/New_York",
                                                     "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["EPS", "BVPS"], fields=["Mean","Actual"],
                    ffill=False, shift=False)

        self.assertSetEqual(set(estimates.index.get_level_values("Indicator")), {"EPS", "BVPS"})
        self.assertSetEqual(set(estimates.index.get_level_values("Field")), {"Actual", "Mean"})

        eps_actuals = estimates.loc["EPS"].loc["Actual"]
        self.assertListEqual(list(eps_actuals.index), list(closes.index))
        self.assertListEqual(list(eps_actuals.columns), list(closes.columns))

        # replace Nan with "nan" to allow equality comparisons
        eps_actuals = eps_actuals.fillna("nan")

        self.maxDiff = None

        self.assertDictEqual(
            eps_actuals.to_dict(),
            {12345: {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): 16.34,
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): "nan",
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"
                },
             23456: {
                 pd.Timestamp('2018-07-22 00:00:00'): "nan",
                 pd.Timestamp('2018-07-23 00:00:00'): "nan",
                 pd.Timestamp('2018-07-24 00:00:00'): "nan",
                 pd.Timestamp('2018-07-27 00:00:00'): 10.04,
                 pd.Timestamp('2018-07-28 00:00:00'): "nan",
                 pd.Timestamp('2018-07-29 00:00:00'): "nan"
             },
            34567: {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): "nan",
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): 0.56,
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"}
            })

        eps_estimates = estimates.loc["EPS"].loc["Mean"]

        # replace Nan with "nan" to allow equality comparisons
        eps_estimates = eps_estimates.fillna("nan")

        self.assertDictEqual(
            eps_estimates.to_dict(),
            {12345: {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): 15.67,
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): "nan",
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"
                },
             23456: {
                 pd.Timestamp('2018-07-22 00:00:00'): "nan",
                 pd.Timestamp('2018-07-23 00:00:00'): "nan",
                 pd.Timestamp('2018-07-24 00:00:00'): "nan",
                 pd.Timestamp('2018-07-27 00:00:00'): 10.03,
                 pd.Timestamp('2018-07-28 00:00:00'): "nan",
                 pd.Timestamp('2018-07-29 00:00:00'): "nan"
             },
            34567: {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): "nan",
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): 1.00,
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"}
            })

        bvps_actuals = estimates.loc["BVPS"].loc["Actual"]

        # replace Nan with "nan" to allow equality comparisons
        bvps_actuals = bvps_actuals.fillna("nan")
        self.assertDictEqual(
            bvps_actuals.to_dict(),
            {12345: {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): "nan",
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): 45.34,
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"
                },
             23456: {
                 pd.Timestamp('2018-07-22 00:00:00'): "nan",
                 pd.Timestamp('2018-07-23 00:00:00'): "nan",
                 pd.Timestamp('2018-07-24 00:00:00'): "nan",
                 pd.Timestamp('2018-07-27 00:00:00'): "nan",
                 pd.Timestamp('2018-07-28 00:00:00'): 21.34,
                 pd.Timestamp('2018-07-29 00:00:00'): "nan"
             },
            34567: {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): "nan",
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): "nan",
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"}
            })

        bvps_estimates = estimates.loc["BVPS"].loc["Mean"]

        # replace Nan with "nan" to allow equality comparisons
        bvps_estimates = bvps_estimates.fillna("nan")

        self.assertDictEqual(
            bvps_estimates.to_dict(),
            {12345: {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): "nan",
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): 42.34,
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"
                },
             23456: {
                 pd.Timestamp('2018-07-22 00:00:00'): "nan",
                 pd.Timestamp('2018-07-23 00:00:00'): "nan",
                 pd.Timestamp('2018-07-24 00:00:00'): "nan",
                 pd.Timestamp('2018-07-27 00:00:00'): "nan",
                 pd.Timestamp('2018-07-28 00:00:00'): 24.56,
                 pd.Timestamp('2018-07-29 00:00:00'): "nan"
             },
            34567: {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): "nan",
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): "nan",
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"}
            })

    def test_max_lag(self):
        """
        Tests that max_lag works as expected.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-07-20", periods=6, freq="D", name="Date"))

        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-06-30"
                        ],
                    UpdatedDate=[
                        "2018-07-06T18:00:35",
                        ],
                     ConId=[
                         12345,
                         ],
                     Indicator=[
                         "BVPS",
                         ],
                     Actual=[
                         45
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345],
                                           Timezone=["America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                # request without max_lag
                estimates = get_reuters_estimates_reindexed_like(
                    closes, "BVPS")

        self.assertSetEqual(set(estimates.index.get_level_values("Indicator")), {"BVPS"})
        self.assertSetEqual(set(estimates.index.get_level_values("Field")), {"Actual"})

        bvps = estimates.loc["BVPS"].loc["Actual"]
        self.assertListEqual(list(bvps.index), list(closes.index))
        self.assertListEqual(list(bvps.columns), list(closes.columns))
        # Data is ffiled to end of frame
        self.assertTrue((bvps[12345] == 45).all())

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                # request with max_lag
                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["BVPS"], max_lag="23D")

        bvps = estimates.loc["BVPS"].loc["Actual"][12345]
        # Data is only ffiled to 2018-07-23 (2018-06-30 + 23D)
        self.assertTrue((bvps.loc[bvps.index <= "2018-07-23"] == 45).all())
        self.assertTrue((bvps.loc[bvps.index > "2018-07-23"].isnull()).all())

    def test_tz_aware_index(self):
        """
        Tests that reindex_like.index can be tz-naive or tz-aware.
        """
        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-31",
                        "2018-06-30"
                        ],
                    UpdatedDate=[
                        "2018-04-23T14:00:00",
                        "2018-07-06T17:34:00",
                        ],
                     ConId=[
                         12345,
                         12345
                         ],
                     Indicator=[
                         "ROA",
                         "ROA"
                         ],
                     Actual=[
                         35,
                         23
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345],
                                           Timezone=["America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                # request with tz_naive
                closes = pd.DataFrame(
                    np.random.rand(4,1),
                    columns=[12345],
                    index=pd.date_range(start="2018-07-05", periods=4, freq="D", name="Date"))

                estimates = get_reuters_estimates_reindexed_like(
                    closes, "ROA", fields="Actual")

        self.assertSetEqual(set(estimates.index.get_level_values("Indicator")), {"ROA"})
        self.assertSetEqual(set(estimates.index.get_level_values("Field")), {"Actual"})

        roas = estimates.loc["ROA"].loc["Actual"]
        self.assertListEqual(list(roas.index), list(closes.index))
        self.assertListEqual(list(roas.columns), list(closes.columns))
        roas = roas.reset_index()
        roas.loc[:, "Date"] = roas.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            roas.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00', 12345: 35.0},
             {'Date': '2018-07-06T00:00:00', 12345: 35.0},
             {'Date': '2018-07-07T00:00:00', 12345: 23.0},
             {'Date': '2018-07-08T00:00:00', 12345: 23.0}]
        )

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                # request with tz-aware
                closes = pd.DataFrame(
                    np.random.rand(4,1),
                    columns=[12345],
                    index=pd.date_range(start="2018-07-05", periods=4, freq="D",
                                        tz="America/New_York", name="Date"))

                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["ROA"])

        roas = estimates.loc["ROA"].loc["Actual"]
        roas = roas.reset_index()
        roas.loc[:, "Date"] = roas.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            roas.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00-0400', 12345: 35.0},
             {'Date': '2018-07-06T00:00:00-0400', 12345: 35.0},
             {'Date': '2018-07-07T00:00:00-0400', 12345: 23.0},
             {'Date': '2018-07-08T00:00:00-0400', 12345: 23.0}]
        )

    def test_complain_if_missing_securities(self):
        """
        Tests error handling when a security is missing from the securities
        master.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-06-30",
                        "2018-06-30"
                        ],
                    UpdatedDate=[
                        "2018-04-06T10:00:00",
                        "2018-04-06T10:00:00",
                        "2018-04-23T13:00:00",
                        "2018-04-23T13:00:00",
                        "2018-07-23T13:00:00",
                        "2018-07-23T13:00:00",
                        ],
                     ConId=[
                         12345,
                         12345,
                         23456,
                         23456,
                         12345,
                         12345,
                         ],
                     Indicator=[
                         "BVPS",
                         "EPS",
                         "BVPS",
                         "EPS",
                         "BVPS",
                         "EPS"
                         ],
                     Actual=[
                         20,
                         9.56,
                         50,
                         63.22,
                         24.5,
                         11.35
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345],
                                           Timezone=["Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):


                with self.assertRaises(MissingData) as cm:
                    get_reuters_estimates_reindexed_like(
                        closes, ["BVPS","EPS"])

        self.assertIn((
            "timezones are missing for some conids so cannot convert UTC "
            "estimates to timezone of security (conids missing timezone: 23456)"), str(cm.exception))

    def test_convert_utc_to_security_timezone(self):
        """
        Tests that estimate UpdatedDates are converted from UTC to the
        security timezone for the purpose of date alignment.
        """
        closes = pd.DataFrame(
            np.random.rand(4,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-07-22", periods=4, freq="D", name="Date"))

        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-31",
                        "2018-03-31",
                        "2018-06-30",
                        "2018-06-30",
                        ],
                    UpdatedDate=[
                        "2018-04-06T08:00:00",
                        "2018-04-07T09:35:00",
                        "2018-07-23T17:00:00", # = 2018-07-23 America/New_York
                        "2018-07-23T17:00:00", # = 2018-07-24 Japan
                        ],
                     ConId=[
                         12345,
                         23456,
                         12345,
                         23456
                         ],
                     Indicator=[
                         "EPS",
                         "EPS",
                         "EPS",
                         "EPS"
                         ],
                     Actual=[
                         24.5,
                         11.35,
                         26.7,
                         15.4
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456],
                                           Timezone=["America/New_York", "Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["EPS"])

        eps = estimates.loc["EPS"].loc["Actual"]
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(eps.to_dict(orient="records"),
            [{'Date': '2018-07-22T00:00:00', 12345: 24.5, 23456: 11.35},
             {'Date': '2018-07-23T00:00:00', 12345: 24.5, 23456: 11.35},
             {'Date': '2018-07-24T00:00:00', 12345: 26.7, 23456: 11.35},
             {'Date': '2018-07-25T00:00:00', 12345: 26.7, 23456: 15.4}]
        )

    def test_ignore_no_actuals(self):
        """
        Tests that estimates with no actuals are ignored.
        """
        closes = pd.DataFrame(
            np.random.rand(4,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-07-05", periods=4, freq="D", name="Date"))

        def mock_download_reuters_estimates(codes, f, *args, **kwargs):
            estimates = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-31",
                        "2018-06-30",
                        "2018-03-31",
                        "2018-06-30",
                        ],
                    UpdatedDate=[
                        "2018-04-23T14:00:00",
                        "2018-07-06T17:34:00",
                        "2018-04-23T14:00:00",
                        "2018-07-06T17:34:00",
                        ],
                     ConId=[
                         12345,
                         12345,
                         23456,
                         23456,
                         ],
                     Indicator=[
                         "ROA",
                         "ROA",
                         "ROA",
                         "ROA"
                         ],
                     Actual=[
                         35,
                         None,
                         None,
                         46.7
                     ]))
            estimates.to_csv(f, index=False)
            f.seek(0)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345, 23456],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["ROA"])

        self.assertSetEqual(set(estimates.index.get_level_values("Indicator")), {"ROA"})
        self.assertSetEqual(set(estimates.index.get_level_values("Field")), {"Actual"})

        roas = estimates.loc["ROA"].loc["Actual"]
        self.assertListEqual(list(roas.index), list(closes.index))
        self.assertListEqual(list(roas.columns), list(closes.columns))
        # replace nan with "nan" to allow equality comparisons
        roas = roas.where(roas.notnull(), "nan")
        roas = roas.reset_index()
        roas.loc[:, "Date"] = roas.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            roas.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00', 12345: 35.0, 23456: "nan"},
             {'Date': '2018-07-06T00:00:00', 12345: 35.0, 23456: "nan"},
             {'Date': '2018-07-07T00:00:00', 12345: 35.0, 23456: 46.7},
             {'Date': '2018-07-08T00:00:00', 12345: 35.0, 23456: 46.7}]
        )

class ReutersFinancialsReindexedLikeTestCase(unittest.TestCase):

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
            get_reuters_financials_reindexed_like(closes, "ATOT")

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
            get_reuters_financials_reindexed_like(closes, "ATOT")

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
            get_reuters_financials_reindexed_like(closes, "ATOT")

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_reuters_financials")
    def test_pass_args_correctly(self,
                                 mock_download_reuters_financials):
        """
        Tests that conids, date ranges, and and other args are correctly
        passed to download_reuters_financials.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def _mock_download_reuters_financials(coa_codes, f, *args, **kwargs):
            financials = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-06-30",
                        "2018-06-30"
                        ],
                    SourceDate=[
                        "2018-04-06",
                        "2018-04-06",
                        "2018-04-23",
                        "2018-04-23",
                        "2018-07-23",
                        "2018-07-23",
                        ],
                     ConId=[
                         12345,
                         12345,
                         23456,
                         23456,
                         12345,
                         12345,
                         ],
                     CoaCode=[
                         "ATOT",
                         "QTCO",
                         "ATOT",
                         "QTCO",
                         "ATOT",
                         "QTCO"
                         ],
                     Amount=[
                         565,
                         89,
                         235,
                         73,
                         580,
                         92
                     ]))
            financials.to_csv(f, index=False)
            f.seek(0)

        mock_download_reuters_financials.side_effect = _mock_download_reuters_financials

        get_reuters_financials_reindexed_like(
            closes, ["ATOT","QTCO"], fields=["Amount", "FiscalPeriodEndDate"],
            interim=True, exclude_restatements=False, max_lag="500D")

        reuters_financials_call = mock_download_reuters_financials.mock_calls[0]
        _, args, kwargs = reuters_financials_call
        self.assertListEqual(args[0], ["ATOT", "QTCO"])
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2016-09-02") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")
        self.assertEqual(kwargs["fields"], ["Amount", "FiscalPeriodEndDate"])
        self.assertTrue(kwargs["interim"])
        self.assertFalse(kwargs["exclude_restatements"])

        get_reuters_financials_reindexed_like(
            closes, ["ATOT", "QTCO", "LTLL"], fields=["Amount", "Source"],
            interim=False, exclude_restatements=True, max_lag="500D")

        reuters_financials_call = mock_download_reuters_financials.mock_calls[1]
        _, args, kwargs = reuters_financials_call
        self.assertListEqual(args[0], ["ATOT", "QTCO", "LTLL"])
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2016-09-02") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")
        self.assertEqual(kwargs["fields"], ["Amount", "Source"])
        self.assertFalse(kwargs["interim"])
        self.assertTrue(kwargs["exclude_restatements"])

    def test_dedupe_source_date(self):
        """
        Tests that duplicate SourceDates (resulting from reporting several
        fiscal periods at once) are deduped by keeping the latest record.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def mock_download_reuters_financials(coa_codes, f, *args, **kwargs):
            financials = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-30",
                        "2018-06-30"
                        ],
                    SourceDate=[
                        "2018-07-23",
                        "2018-07-23",
                        ],
                     ConId=[
                         12345,
                         12345,
                         ],
                     CoaCode=[
                         "ATOT",
                         "ATOT",
                         ],
                     Amount=[
                         565,
                         580
                     ]))
            financials.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_financials', new=mock_download_reuters_financials):

            financials = get_reuters_financials_reindexed_like(
                closes, "ATOT", interim=True)

        self.assertSetEqual(set(financials.index.get_level_values("CoaCode")), {"ATOT"})
        self.assertSetEqual(set(financials.index.get_level_values("Field")), {"Amount"})

        atots = financials.loc["ATOT"].loc["Amount"]
        self.assertListEqual(list(atots.index), list(closes.index))
        self.assertListEqual(list(atots.columns), list(closes.columns))
        self.assertEqual(atots[12345].loc["2018-08-01"], 580)

    def test_ffill_no_lookahead_bias(self):
        """
        Tests that financial statement metrics are ffilled and are shifted
        forward 1 period to avoid lookahead bias.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-07-20", periods=6, freq="D", name="Date"))

        def mock_download_reuters_financials(coa_codes, f, *args, **kwargs):
            financials = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-30",
                        "2018-06-30"
                        ],
                    SourceDate=[
                        "2018-04-23",
                        "2018-07-23",
                        ],
                     ConId=[
                         12345,
                         12345,
                         ],
                     CoaCode=[
                         "ATOT",
                         "ATOT",
                         ],
                     Amount=[
                         565,
                         580
                     ]))
            financials.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_financials', new=mock_download_reuters_financials):

            financials = get_reuters_financials_reindexed_like(
                closes, ["ATOT"], interim=True)

        self.assertSetEqual(set(financials.index.get_level_values("CoaCode")), {"ATOT"})
        self.assertSetEqual(set(financials.index.get_level_values("Field")), {"Amount"})

        atots = financials.loc["ATOT"].loc["Amount"]
        self.assertListEqual(list(atots.index), list(closes.index))
        self.assertListEqual(list(atots.columns), list(closes.columns))
        self.assertEqual(atots[12345].loc["2018-07-23"], 565)
        self.assertEqual(atots[12345].loc["2018-07-24"], 580)

    def test_max_lag(self):
        """
        Tests that max_lag works as expected.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-07-20", periods=6, freq="D", name="Date"))

        def mock_download_reuters_financials(coa_codes, f, *args, **kwargs):
            financials = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-06-30"
                        ],
                    SourceDate=[
                        "2018-07-06",
                        ],
                     ConId=[
                         12345,
                         ],
                     CoaCode=[
                         "ATOT",
                         ],
                     Amount=[
                         580
                     ]))
            financials.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_financials', new=mock_download_reuters_financials):

            # request without max_lag
            financials = get_reuters_financials_reindexed_like(
                closes, ["ATOT"], interim=True)

        self.assertSetEqual(set(financials.index.get_level_values("CoaCode")), {"ATOT"})
        self.assertSetEqual(set(financials.index.get_level_values("Field")), {"Amount"})

        atots = financials.loc["ATOT"].loc["Amount"]
        self.assertListEqual(list(atots.index), list(closes.index))
        self.assertListEqual(list(atots.columns), list(closes.columns))
        # Data is ffiled to end of frame
        self.assertTrue((atots[12345] == 580).all())

        with patch('quantrocket.fundamental.download_reuters_financials', new=mock_download_reuters_financials):

            # request with max_lag
            financials = get_reuters_financials_reindexed_like(
                closes, ["ATOT"], interim=True, max_lag="23D")

        atots = financials.loc["ATOT"].loc["Amount"][12345]
        # Data is only ffiled to 2018-07-23 (2018-06-30 + 23D)
        self.assertTrue((atots.loc[atots.index <= "2018-07-23"] == 580).all())
        self.assertTrue((atots.loc[atots.index > "2018-07-23"].isnull()).all())

    def test_tz_aware_index(self):
        """
        Tests that reindex_like.index can be tz-naive or tz-aware.
        """
        def mock_download_reuters_financials(coa_codes, f, *args, **kwargs):
            financials = pd.DataFrame(
                dict(
                    FiscalPeriodEndDate=[
                        "2018-03-31",
                        "2018-06-30"
                        ],
                    SourceDate=[
                        "2018-04-23",
                        "2018-07-06",
                        ],
                     ConId=[
                         12345,
                         12345
                         ],
                     CoaCode=[
                         "ATOT",
                         "ATOT"
                         ],
                     Amount=[
                         580,
                         542
                     ]))
            financials.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_financials', new=mock_download_reuters_financials):

            # request with tz_naive
            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=[12345],
                index=pd.date_range(start="2018-07-05", periods=4, freq="D", name="Date"))

            financials = get_reuters_financials_reindexed_like(
                closes, "ATOT", fields="Amount", interim=True)

        self.assertSetEqual(set(financials.index.get_level_values("CoaCode")), {"ATOT"})
        self.assertSetEqual(set(financials.index.get_level_values("Field")), {"Amount"})

        atots = financials.loc["ATOT"].loc["Amount"]
        self.assertListEqual(list(atots.index), list(closes.index))
        self.assertListEqual(list(atots.columns), list(closes.columns))
        atots = atots.reset_index()
        atots.loc[:, "Date"] = atots.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            atots.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00', 12345: 580.0},
             {'Date': '2018-07-06T00:00:00', 12345: 580.0},
             {'Date': '2018-07-07T00:00:00', 12345: 542.0},
             {'Date': '2018-07-08T00:00:00', 12345: 542.0}]
        )

        with patch('quantrocket.fundamental.download_reuters_financials', new=mock_download_reuters_financials):

            # request with tz-aware
            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=[12345],
                index=pd.date_range(start="2018-07-05", periods=4, freq="D", tz="America/New_York", name="Date"))

            financials = get_reuters_financials_reindexed_like(
                closes, ["ATOT"], interim=True)

        atots = financials.loc["ATOT"].loc["Amount"][12345]
        atots = atots.reset_index()
        atots.loc[:, "Date"] = atots.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            atots.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00-0400', 12345: 580.0},
             {'Date': '2018-07-06T00:00:00-0400', 12345: 580.0},
             {'Date': '2018-07-07T00:00:00-0400', 12345: 542.0},
             {'Date': '2018-07-08T00:00:00-0400', 12345: 542.0}]
        )

class WSHEarningsDatesReindexedLikeTestCase(unittest.TestCase):

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
            get_wsh_earnings_dates_reindexed_like(closes)

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
            get_wsh_earnings_dates_reindexed_like(closes)

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
            get_wsh_earnings_dates_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_wsh_earnings_dates")
    def test_pass_conids_and_dates_based_on_reindex_like(self,
                                                         mock_download_wsh_earnings_dates):
        """
        Tests that conids and date ranges are correctly passed to the
        download_wsh_earnings_dates function based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_wsh_earnings_dates(f, *args, **kwargs):
            announcements = pd.DataFrame(
                dict(Date=["2018-05-01",
                           "2018-05-02"],
                     ConId=[12345,
                            23456],
                     Time=["Before Market",
                            "After Market"],
                     Status=["Unconfirmed",
                             "Unconfirmed"],
                     LastUpdated=["2018-04-11T07:48:20",
                                  "2018-04-09T07:48:20"]
                     ))
            announcements.to_csv(f, index=False)
            f.seek(0)

        mock_download_wsh_earnings_dates.side_effect = _mock_download_wsh_earnings_dates

        get_wsh_earnings_dates_reindexed_like(closes, fields=["Time","Status"], statuses="Unconfirmed")

        wsh_call = mock_download_wsh_earnings_dates.mock_calls[0]
        _, args, kwargs = wsh_call
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2018-05-01")
        self.assertEqual(kwargs["end_date"], "2018-05-03")
        self.assertListEqual(kwargs["fields"], ["Time","Status","LastUpdated"])
        self.assertListEqual(kwargs["statuses"], ["Unconfirmed"])

    @patch("quantrocket.fundamental.download_wsh_earnings_dates")
    def test_dedupe(self, mock_download_wsh_earnings_dates):
        """
        Tests that the resulting DataFrame is correct when deduping on
        LastUpdated.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_wsh_earnings_dates(f, *args, **kwargs):
            announcements = pd.DataFrame(
                dict(Date=["2018-05-01",
                           "2018-05-01",
                           "2018-05-02",
                           "2018-05-02"],
                     ConId=[12345,
                            12345,
                            23456,
                            23456],
                     Time=["Before Market",
                           "After Market",
                            "After Market",
                            "Unspecified"],
                     Status=["Unconfirmed",
                             "Confirmed",
                             "Confirmed",
                             "Confirmed"],
                     LastUpdated=["2018-03-11T07:48:20",
                                  "2018-04-09T07:48:20",
                                  "2018-04-11T07:48:20",
                                  "2018-04-09T07:48:20"]))
            announcements.to_csv(f, index=False)
            f.seek(0)

        mock_download_wsh_earnings_dates.side_effect = _mock_download_wsh_earnings_dates

        announcements = get_wsh_earnings_dates_reindexed_like(closes,
                                                              statuses=["Confirmed","Unconfirmed"])

        wsh_call = mock_download_wsh_earnings_dates.mock_calls[0]
        _, args, kwargs = wsh_call
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2018-05-01")
        self.assertEqual(kwargs["end_date"], "2018-05-03")
        self.assertListEqual(kwargs["fields"], ["Time", "LastUpdated"])
        self.assertListEqual(kwargs["statuses"], ["Confirmed", "Unconfirmed"])

        # but only Time is returned, as requested
        self.assertSetEqual(set(announcements.index.get_level_values("Field").unique()), {"Time"})

        announce_times = announcements.loc["Time"]
        announce_times = announce_times.reset_index()
        announce_times.loc[:, "Date"] = announce_times.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        announce_times = announce_times.fillna("nan")

        self.assertListEqual(
            announce_times.to_dict(orient="records"),
            [
                {'Date': '2018-05-01T00:00:00',
                 12345: 'After Market',
                 23456: 'nan'},
                {'Date': '2018-05-02T00:00:00',
                 12345: 'nan',
                 23456: 'After Market'},
                {'Date': '2018-05-03T00:00:00',
                 12345: 'nan',
                 23456: 'nan'}]
        )

        # Repeat but request Status field so we can check the output of that too
        announcements = get_wsh_earnings_dates_reindexed_like(closes,
                                                              fields=["Time","Status"],
                                                              statuses=["Confirmed","Unconfirmed"])

        announce_statuses = announcements.loc["Status"]
        announce_statuses = announce_statuses.reset_index()
        announce_statuses.loc[:, "Date"] = announce_statuses.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        announce_statuses = announce_statuses.fillna("nan")

        self.assertListEqual(
            announce_statuses.to_dict(orient="records"),
            [
                {'Date': '2018-05-01T00:00:00',
                 12345: 'Confirmed',
                 23456: 'nan'},
                {'Date': '2018-05-02T00:00:00',
                 12345: 'nan',
                 23456: 'Confirmed'},
                {'Date': '2018-05-03T00:00:00',
                 12345: 'nan',
                 23456: 'nan'}]
        )

    def test_tz_aware_index(self):
        """
        Tests that a tz-aware index in the input DataFrame can be handled.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", tz="America/New_York",
                                name="Date"))

        def mock_download_wsh_earnings_dates(f, *args, **kwargs):
            announcements = pd.DataFrame(
                dict(Date=["2018-05-01",
                           "2018-05-02"],
                     ConId=[12345,
                            23456],
                     Time=["Before Market",
                           "After Market"],
                     LastUpdated=["2018-04-11T07:48:20",
                                  "2018-04-09T07:48:20"]))
            announcements.to_csv(f, index=False)
            f.seek(0)


        with patch('quantrocket.fundamental.download_wsh_earnings_dates', new=mock_download_wsh_earnings_dates):

            announcements = get_wsh_earnings_dates_reindexed_like(closes)

        self.assertSetEqual(set(announcements.index.get_level_values("Field").unique()), {"Time"})

        announce_times = announcements.loc["Time"]
        self.assertEqual(announce_times.index.tz.zone, "America/New_York")
        announce_times = announce_times.reset_index()
        announce_times.loc[:, "Date"] = announce_times.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        announce_times = announce_times.fillna("nan")

        self.assertListEqual(
            announce_times.to_dict(orient="records"),
            [
                {'Date': '2018-05-01T00:00:00-0400',
                 12345: 'Before Market',
                 23456: 'nan'},
                {'Date': '2018-05-02T00:00:00-0400',
                 12345: 'nan',
                 23456: 'After Market'},
                {'Date': '2018-05-03T00:00:00-0400',
                 12345: 'nan',
                 23456: 'nan'}]
        )

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

class SharadarFundamentalsReindexedLikeTestCase(unittest.TestCase):

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
            get_sharadar_fundamentals_reindexed_like(closes, "main")

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
            get_sharadar_fundamentals_reindexed_like(closes, "main")

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
            get_sharadar_fundamentals_reindexed_like(closes, "main")

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_sharadar_fundamentals")
    def test_pass_args_correctly(self,
                                 mock_download_sharadar_fundamentals):
        """
        Tests that conids, date ranges, and and other args are correctly
        passed to download_sharadar_fundamentals.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def _mock_download_sharadar_fundamentals(domain, filepath_or_buffer, *args, **kwargs):
            fundamentals = pd.DataFrame(
                dict(
                    DATEKEY=[
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-06-30",
                        "2018-06-30"
                        ],
                    REPORTPERIOD=[
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-03-31",
                        "2018-06-30",
                        "2018-06-30"
                        ],
                    ConId=[
                         12345,
                         12345,
                         23456,
                         23456,
                         12345,
                         12345,
                         ],
                     EPS=[
                         565,
                         89,
                         235,
                         73,
                         580,
                         92
                     ]))
            fundamentals.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        mock_download_sharadar_fundamentals.side_effect = _mock_download_sharadar_fundamentals

        get_sharadar_fundamentals_reindexed_like(
            closes, domain="main", fields=["EPS", "DATEKEY"], dimension="ARQ")

        sharadar_fundamentals_call = mock_download_sharadar_fundamentals.mock_calls[0]
        _, args, kwargs = sharadar_fundamentals_call
        self.assertEqual(kwargs["domain"], "main")
        self.assertEqual(kwargs["start_date"], "2016-09-02") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")
        self.assertEqual(kwargs["fields"], ["EPS", "DATEKEY"])
        self.assertEqual(kwargs["dimensions"], "ARQ")

    def test_dedupe_datekey(self):
        """
        Tests that duplicate DATEKEYS (resulting from reporting several
        fiscal periods at once) are deduped by keeping the latest record.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def mock_download_sharadar_fundamentals(domain, filepath_or_buffer, *args, **kwargs):
            fundamentals = pd.DataFrame(
                dict(
                    REPORTPERIOD=[
                        "2018-03-30",
                        "2018-06-30"
                        ],
                    DATEKEY=[
                        "2018-07-23",
                        "2018-07-23",
                        ],
                     ConId=[
                         12345,
                         12345,
                         ],
                     EPS=[
                         565,
                         580
                     ]))
            fundamentals.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, domain="main", fields="EPS")

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"EPS"})

        eps = fundamentals.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(eps.columns))
        self.assertEqual(eps[12345].loc["2018-08-01"], 580)

    def test_ffill_no_lookahead_bias(self):
        """
        Tests that financial statement metrics are ffilled and are shifted
        forward 1 period to avoid lookahead bias.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=[12345],
            index=pd.date_range(start="2018-07-20", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_fundamentals(domain, filepath_or_buffer, *args, **kwargs):
            fundamentals = pd.DataFrame(
                dict(
                    REPORTPERIOD=[
                        "2018-03-30",
                        "2018-06-30"
                        ],
                    DATEKEY=[
                        "2018-04-23",
                        "2018-07-23",
                        ],
                     ConId=[
                         12345,
                         12345,
                         ],
                     EPS=[
                         565,
                         580
                     ]))
            fundamentals.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, domain="main", fields=["EPS"])

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"EPS"})

        eps = fundamentals.loc["EPS"]
        self.assertListEqual(list(eps.index), list(eps.index))
        self.assertListEqual(list(eps.columns), list(eps.columns))
        self.assertEqual(eps[12345].loc["2018-07-23"], 565)
        self.assertEqual(eps[12345].loc["2018-07-24"], 580)

    def test_tz_aware_index(self):
        """
        Tests that reindex_like.index can be tz-naive or tz-aware.
        """
        def mock_download_sharadar_fundamentals(domain, filepath_or_buffer, *args, **kwargs):
            fundamentals = pd.DataFrame(
                dict(
                    REPORTPERIOD=[
                        "2018-03-31",
                        "2018-06-30"
                        ],
                    DATEKEY=[
                        "2018-04-23",
                        "2018-07-06",
                        ],
                     ConId=[
                         12345,
                         12345,
                         ],
                     REVENUE=[
                         580,
                         542
                     ]))
            fundamentals.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            # request with tz_naive
            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=[12345],
                index=pd.date_range(start="2018-07-05", periods=4, freq="D", name="Date"))

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, domain="sharadar", fields="REVENUE")

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"REVENUE"})

        revenues = fundamentals.loc["REVENUE"]
        self.assertListEqual(list(revenues.index), list(revenues.index))
        self.assertListEqual(list(revenues.columns), list(revenues.columns))
        revenues = revenues.reset_index()
        revenues.loc[:, "Date"] = revenues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            revenues.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00', 12345: 580.0},
             {'Date': '2018-07-06T00:00:00', 12345: 580.0},
             {'Date': '2018-07-07T00:00:00', 12345: 542.0},
             {'Date': '2018-07-08T00:00:00', 12345: 542.0}]
        )

        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            # request with tz-aware
            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=[12345],
                index=pd.date_range(start="2018-07-05", periods=4, freq="D", tz="America/New_York", name="Date"))

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, domain="sharadar", fields="REVENUE")

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"REVENUE"})

        revenues = fundamentals.loc["REVENUE"]
        self.assertListEqual(list(revenues.index), list(revenues.index))
        self.assertListEqual(list(revenues.columns), list(revenues.columns))
        revenues = revenues.reset_index()
        revenues.loc[:, "Date"] = revenues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            revenues.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00-0400', 12345: 580.0},
             {'Date': '2018-07-06T00:00:00-0400', 12345: 580.0},
             {'Date': '2018-07-07T00:00:00-0400', 12345: 542.0},
             {'Date': '2018-07-08T00:00:00-0400', 12345: 542.0}]
        )