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

# To run: pytest path/to/quantrocket/tests -v

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
    get_alpaca_etb_reindexed_like,
    get_ibkr_borrow_fees_reindexed_like,
    get_ibkr_shortable_shares_reindexed_like,
    get_ibkr_margin_requirements_reindexed_like,
    get_sharadar_fundamentals_reindexed_like,
    get_sharadar_institutions_reindexed_like,
    get_sharadar_sec8_reindexed_like,
    get_sharadar_sp500_reindexed_like,
    get_wsh_earnings_dates_reindexed_like,
    get_brain_bsi_reindexed_like,
    get_brain_blmcf_reindexed_like,
    get_brain_blmect_reindexed_like,
)
from quantrocket.exceptions import ParameterError, MissingData, NoFundamentalData

class ReutersEstimatesReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
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
            columns=["FI12345","FI23456"],
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
            columns=["FI12345","FI23456"],
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
        Tests that sids, date ranges, and and other args are correctly
        passed to download_reuters_estimates.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         "FI12345",
                         "FI12345",
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
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
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
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2016-09-02") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")
        self.assertEqual(kwargs["fields"], ["Actual", "FiscalPeriodEndDate", "UpdatedDate"])
        self.assertEqual(kwargs["period_types"], ["Q"])

        master_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = master_call
        self.assertEqual(kwargs["sids"], ["FI12345","FI23456"])

        get_reuters_estimates_reindexed_like(
            closes, ["BVPS", "EPS", "ROA"], fields=["Actual", "Mean"],
            period_types=["A","S"], max_lag="500D")

        reuters_estimates_call = mock_download_reuters_estimates.mock_calls[1]
        _, args, kwargs = reuters_estimates_call
        self.assertListEqual(args[0], ["BVPS", "EPS", "ROA"])
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2016-09-02") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")
        self.assertEqual(kwargs["fields"], ["Actual", "Mean","UpdatedDate"])
        self.assertEqual(kwargs["period_types"], ["A","S"])

        master_call = mock_download_master_file.mock_calls[1]
        _, args, kwargs = master_call
        self.assertEqual(kwargs["sids"], ["FI12345","FI23456"])

    def test_dedupe_announce_date(self):
        """
        Tests that duplicate UpdatedDates (resulting from reporting several
        fiscal periods at once) are deduped by keeping the latest record.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=["FI12345"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
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
            securities = pd.DataFrame(dict(Sid=["FI12345"],
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
        self.assertEqual(eps["FI12345"].loc["2018-08-01"], 11.35)

    def test_ffill_no_lookahead_bias(self):
        """
        Tests that indicators are ffilled and are shifted forward 1 period to
        avoid lookahead bias.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=["FI12345"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
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
            securities = pd.DataFrame(dict(Sid=["FI12345"],
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
        self.assertEqual(eps["FI12345"].loc["2018-07-23"], 13.45)
        self.assertEqual(eps["FI12345"].loc["2018-07-24"], 16.34)

    def test_no_shift(self):
        """
        Tests that indicators are not shifted forward 1 period if shift=False.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=["FI12345"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
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
            securities = pd.DataFrame(dict(Sid=["FI12345"],
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
        self.assertEqual(eps["FI12345"].loc["2018-07-22"], 13.45)
        self.assertEqual(eps["FI12345"].loc["2018-07-23"], 16.34)

    def test_no_ffill(self):
        """
        Tests that indicators are not forward-filled if ffill=False.
        """
        closes = pd.DataFrame(
            np.random.rand(6,3),
            columns=["FI12345", "FI23456", "FI34567"],
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
                        "2018-07-25T10:00:00", # in the unlikely event of an announcement on the weekend, it will be dropped if no ffill
                        "2018-07-27T10:00:00",
                        "2018-07-27T10:00:00",
                        "2018-07-28T10:00:00",
                        ],
                     Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         "FI34567",
                         "FI12345",
                         "FI23456"
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
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456","FI34567"],
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
            {"FI12345": {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): 16.34,
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): "nan",
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"
                },
             "FI23456": {
                 pd.Timestamp('2018-07-22 00:00:00'): "nan",
                 pd.Timestamp('2018-07-23 00:00:00'): "nan",
                 pd.Timestamp('2018-07-24 00:00:00'): "nan",
                 pd.Timestamp('2018-07-27 00:00:00'): "nan",
                 pd.Timestamp('2018-07-28 00:00:00'): "nan",
                 pd.Timestamp('2018-07-29 00:00:00'): "nan"
             },
            "FI34567": {
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
            {"FI12345": {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): 15.67,
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): "nan",
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"
                },
             "FI23456": {
                 pd.Timestamp('2018-07-22 00:00:00'): "nan",
                 pd.Timestamp('2018-07-23 00:00:00'): "nan",
                 pd.Timestamp('2018-07-24 00:00:00'): "nan",
                 pd.Timestamp('2018-07-27 00:00:00'): "nan",
                 pd.Timestamp('2018-07-28 00:00:00'): "nan",
                 pd.Timestamp('2018-07-29 00:00:00'): "nan"
             },
            "FI34567": {
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
            {"FI12345": {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): "nan",
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): 45.34,
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"
                },
             "FI23456": {
                 pd.Timestamp('2018-07-22 00:00:00'): "nan",
                 pd.Timestamp('2018-07-23 00:00:00'): "nan",
                 pd.Timestamp('2018-07-24 00:00:00'): "nan",
                 pd.Timestamp('2018-07-27 00:00:00'): "nan",
                 pd.Timestamp('2018-07-28 00:00:00'): 21.34,
                 pd.Timestamp('2018-07-29 00:00:00'): "nan"
             },
            "FI34567": {
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
            {"FI12345": {
                pd.Timestamp('2018-07-22 00:00:00'): "nan",
                pd.Timestamp('2018-07-23 00:00:00'): "nan",
                pd.Timestamp('2018-07-24 00:00:00'): "nan",
                pd.Timestamp('2018-07-27 00:00:00'): 42.34,
                pd.Timestamp('2018-07-28 00:00:00'): "nan",
                pd.Timestamp('2018-07-29 00:00:00'): "nan"
                },
             "FI23456": {
                 pd.Timestamp('2018-07-22 00:00:00'): "nan",
                 pd.Timestamp('2018-07-23 00:00:00'): "nan",
                 pd.Timestamp('2018-07-24 00:00:00'): "nan",
                 pd.Timestamp('2018-07-27 00:00:00'): "nan",
                 pd.Timestamp('2018-07-28 00:00:00'): 24.56,
                 pd.Timestamp('2018-07-29 00:00:00'): "nan"
             },
            "FI34567": {
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
            columns=["FI12345"],
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
                     Sid=[
                         "FI12345",
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
            securities = pd.DataFrame(dict(Sid=["FI12345"],
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
        self.assertTrue((bvps["FI12345"] == 45).all())

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                # request with max_lag
                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["BVPS"], max_lag="23D")

        bvps = estimates.loc["BVPS"].loc["Actual"]["FI12345"]
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
                     Sid=[
                         "FI12345",
                         "FI12345"
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
            securities = pd.DataFrame(dict(Sid=["FI12345"],
                                           Timezone=["America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                # request with tz_naive
                closes = pd.DataFrame(
                    np.random.rand(4,1),
                    columns=["FI12345"],
                    index=pd.date_range(start="2018-07-05", periods=4, freq="D", name="Date"))

                estimates = get_reuters_estimates_reindexed_like(
                    closes, "ROA", fields="Actual")

        self.assertSetEqual(set(estimates.index.get_level_values("Indicator")), {"ROA"})
        self.assertSetEqual(set(estimates.index.get_level_values("Field")), {"Actual"})

        roas = estimates.loc["ROA"].loc["Actual"]
        self.assertListEqual(list(roas.index), list(closes.index))
        self.assertListEqual(list(roas.columns), list(closes.columns))
        roas = roas.reset_index()
        roas["Date"] = roas.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            roas.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00', "FI12345": 35.0},
             {'Date': '2018-07-06T00:00:00', "FI12345": 35.0},
             {'Date': '2018-07-07T00:00:00', "FI12345": 23.0},
             {'Date': '2018-07-08T00:00:00', "FI12345": 23.0}]
        )

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                # request with tz-aware
                closes = pd.DataFrame(
                    np.random.rand(4,1),
                    columns=["FI12345"],
                    index=pd.date_range(start="2018-07-05", periods=4, freq="D",
                                        tz="America/New_York", name="Date"))

                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["ROA"])

        roas = estimates.loc["ROA"].loc["Actual"]
        roas = roas.reset_index()
        roas["Date"] = roas.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            roas.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00-0400', "FI12345": 35.0},
             {'Date': '2018-07-06T00:00:00-0400', "FI12345": 35.0},
             {'Date': '2018-07-07T00:00:00-0400', "FI12345": 23.0},
             {'Date': '2018-07-08T00:00:00-0400', "FI12345": 23.0}]
        )

    def test_complain_if_missing_securities(self):
        """
        Tests error handling when a security is missing from the securities
        master.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         "FI12345",
                         "FI12345",
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
            securities = pd.DataFrame(dict(Sid=["FI12345"],
                                           Timezone=["Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):


                with self.assertRaises(MissingData) as cm:
                    get_reuters_estimates_reindexed_like(
                        closes, ["BVPS","EPS"])

        self.assertIn((
            "timezones are missing for some sids so cannot convert UTC "
            "estimates to timezone of security (sids missing timezone: FI23456)"), str(cm.exception))

    def test_convert_utc_to_security_timezone(self):
        """
        Tests that estimate UpdatedDates are converted from UTC to the
        security timezone for the purpose of date alignment.
        """
        closes = pd.DataFrame(
            np.random.rand(4,2),
            columns=["FI12345","FI23456"],
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
                     Sid=[
                         "FI12345",
                         "FI23456",
                         "FI12345",
                         "FI23456"
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
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_reuters_estimates', new=mock_download_reuters_estimates):
            with patch('quantrocket.fundamental.download_master_file', new=mock_download_master_file):

                estimates = get_reuters_estimates_reindexed_like(
                    closes, ["EPS"])

        eps = estimates.loc["EPS"].loc["Actual"]
        eps = eps.reset_index()
        eps["Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(eps.to_dict(orient="records"),
            [{'Date': '2018-07-22T00:00:00', "FI12345": 24.5, "FI23456": 11.35},
             {'Date': '2018-07-23T00:00:00', "FI12345": 24.5, "FI23456": 11.35},
             {'Date': '2018-07-24T00:00:00', "FI12345": 26.7, "FI23456": 11.35},
             {'Date': '2018-07-25T00:00:00', "FI12345": 26.7, "FI23456": 15.4}]
        )

    def test_ignore_no_actuals(self):
        """
        Tests that estimates with no actuals are ignored.
        """
        closes = pd.DataFrame(
            np.random.rand(4,2),
            columns=["FI12345","FI23456"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
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
            securities = pd.DataFrame(dict(Sid=["FI12345", "FI23456"],
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
        roas["Date"] = roas.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            roas.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00', "FI12345": 35.0, "FI23456": "nan"},
             {'Date': '2018-07-06T00:00:00', "FI12345": 35.0, "FI23456": "nan"},
             {'Date': '2018-07-07T00:00:00', "FI12345": 35.0, "FI23456": 46.7},
             {'Date': '2018-07-08T00:00:00', "FI12345": 35.0, "FI23456": 46.7}]
        )

class ReutersFinancialsReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
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
            columns=["FI12345","FI23456"],
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
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_reuters_financials_reindexed_like(closes, "ATOT")

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_reuters_financials")
    def test_pass_args_correctly(self,
                                 mock_download_reuters_financials):
        """
        Tests that sids, date ranges, and and other args are correctly
        passed to download_reuters_financials.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         "FI12345",
                         "FI12345",
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
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
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
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
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
            columns=["FI12345"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
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
        self.assertEqual(atots["FI12345"].loc["2018-08-01"], 580)

    def test_ffill_no_lookahead_bias(self):
        """
        Tests that financial statement metrics are ffilled and are shifted
        forward 1 period to avoid lookahead bias.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=["FI12345"],
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
                     Sid=[
                         "FI12345",
                         "FI12345",
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
        self.assertEqual(atots["FI12345"].loc["2018-07-23"], 565)
        self.assertEqual(atots["FI12345"].loc["2018-07-24"], 580)

    def test_max_lag(self):
        """
        Tests that max_lag works as expected.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=["FI12345"],
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
                     Sid=[
                         "FI12345",
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
        self.assertTrue((atots["FI12345"] == 580).all())

        with patch('quantrocket.fundamental.download_reuters_financials', new=mock_download_reuters_financials):

            # request with max_lag
            financials = get_reuters_financials_reindexed_like(
                closes, ["ATOT"], interim=True, max_lag="23D")

        atots = financials.loc["ATOT"].loc["Amount"]["FI12345"]
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
                     Sid=[
                         "FI12345",
                         "FI12345"
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
                columns=["FI12345"],
                index=pd.date_range(start="2018-07-05", periods=4, freq="D", name="Date"))

            financials = get_reuters_financials_reindexed_like(
                closes, "ATOT", fields="Amount", interim=True)

        self.assertSetEqual(set(financials.index.get_level_values("CoaCode")), {"ATOT"})
        self.assertSetEqual(set(financials.index.get_level_values("Field")), {"Amount"})

        atots = financials.loc["ATOT"].loc["Amount"]
        self.assertListEqual(list(atots.index), list(closes.index))
        self.assertListEqual(list(atots.columns), list(closes.columns))
        atots = atots.reset_index()
        atots["Date"] = atots.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            atots.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00', "FI12345": 580.0},
             {'Date': '2018-07-06T00:00:00', "FI12345": 580.0},
             {'Date': '2018-07-07T00:00:00', "FI12345": 542.0},
             {'Date': '2018-07-08T00:00:00', "FI12345": 542.0}]
        )

        with patch('quantrocket.fundamental.download_reuters_financials', new=mock_download_reuters_financials):

            # request with tz-aware
            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-07-05", periods=4, freq="D", tz="America/New_York", name="Date"))

            financials = get_reuters_financials_reindexed_like(
                closes, ["ATOT"], interim=True)

        atots = financials.loc["ATOT"].loc["Amount"]["FI12345"]
        atots = atots.reset_index()
        atots["Date"] = atots.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            atots.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00-0400', "FI12345": 580.0},
             {'Date': '2018-07-06T00:00:00-0400', "FI12345": 580.0},
             {'Date': '2018-07-07T00:00:00-0400', "FI12345": 542.0},
             {'Date': '2018-07-08T00:00:00-0400', "FI12345": 542.0}]
        )

class WSHEarningsDatesReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
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
            columns=["FI12345","FI23456"],
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
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_wsh_earnings_dates_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_wsh_earnings_dates")
    def test_pass_sids_and_dates_based_on_reindex_like(self,
                                                         mock_download_wsh_earnings_dates):
        """
        Tests that sids and date ranges are correctly passed to the
        download_wsh_earnings_dates function based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_wsh_earnings_dates(f, *args, **kwargs):
            announcements = pd.DataFrame(
                dict(Date=["2018-05-01",
                           "2018-05-02"],
                     Sid=["FI12345",
                            "FI23456"],
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
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
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
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_wsh_earnings_dates(f, *args, **kwargs):
            announcements = pd.DataFrame(
                dict(Date=["2018-05-01",
                           "2018-05-01",
                           "2018-05-02",
                           "2018-05-02"],
                     Sid=["FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456"],
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
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2018-05-01")
        self.assertEqual(kwargs["end_date"], "2018-05-03")
        self.assertListEqual(kwargs["fields"], ["Time", "LastUpdated"])
        self.assertListEqual(kwargs["statuses"], ["Confirmed", "Unconfirmed"])

        # but only Time is returned, as requested
        self.assertSetEqual(set(announcements.index.get_level_values("Field").unique()), {"Time"})

        announce_times = announcements.loc["Time"]
        announce_times = announce_times.reset_index()
        announce_times["Date"] = announce_times.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        announce_times = announce_times.fillna("nan")

        self.assertListEqual(
            announce_times.to_dict(orient="records"),
            [
                {'Date': '2018-05-01T00:00:00',
                 "FI12345": 'After Market',
                 "FI23456": 'nan'},
                {'Date': '2018-05-02T00:00:00',
                 "FI12345": 'nan',
                 "FI23456": 'After Market'},
                {'Date': '2018-05-03T00:00:00',
                 "FI12345": 'nan',
                 "FI23456": 'nan'}]
        )

        # Repeat but request Status field so we can check the output of that too
        announcements = get_wsh_earnings_dates_reindexed_like(closes,
                                                              fields=["Time","Status"],
                                                              statuses=["Confirmed","Unconfirmed"])

        announce_statuses = announcements.loc["Status"]
        announce_statuses = announce_statuses.reset_index()
        announce_statuses["Date"] = announce_statuses.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        announce_statuses = announce_statuses.fillna("nan")

        self.assertListEqual(
            announce_statuses.to_dict(orient="records"),
            [
                {'Date': '2018-05-01T00:00:00',
                 "FI12345": 'Confirmed',
                 "FI23456": 'nan'},
                {'Date': '2018-05-02T00:00:00',
                 "FI12345": 'nan',
                 "FI23456": 'Confirmed'},
                {'Date': '2018-05-03T00:00:00',
                 "FI12345": 'nan',
                 "FI23456": 'nan'}]
        )

    def test_tz_aware_index(self):
        """
        Tests that a tz-aware index in the input DataFrame can be handled.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", tz="America/New_York",
                                name="Date"))

        def mock_download_wsh_earnings_dates(f, *args, **kwargs):
            announcements = pd.DataFrame(
                dict(Date=["2018-05-01",
                           "2018-05-02"],
                     Sid=["FI12345",
                            "FI23456"],
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
        announce_times["Date"] = announce_times.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        announce_times = announce_times.fillna("nan")

        self.assertListEqual(
            announce_times.to_dict(orient="records"),
            [
                {'Date': '2018-05-01T00:00:00-0400',
                 "FI12345": 'Before Market',
                 "FI23456": 'nan'},
                {'Date': '2018-05-02T00:00:00-0400',
                 "FI12345": 'nan',
                 "FI23456": 'After Market'},
                {'Date': '2018-05-03T00:00:00-0400',
                 "FI12345": 'nan',
                 "FI23456": 'nan'}]
        )

class StockloanDataReindexedLikeTestCase(unittest.TestCase):
    """
    Contains tests which are common to get_ibkr_shortable_shares_reindexed_like
    and get_ibkr_margin_requirements_reindexed_like.
    """

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.MultiIndex.from_product((
                pd.date_range(start="2018-01-01", periods=3, freq="D"),
                ["15:00:00","15:15:00"]), names=["Date", "Time"]))

        with self.assertRaises(ParameterError) as cm:
            get_ibkr_shortable_shares_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

    def test_complain_if_date_level_not_in_index(self):
        """
        Tests error handling when reindex_like doesn't have an index named
        Date.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-01-01", periods=3, freq="D"))

        with self.assertRaises(ParameterError) as cm:
            get_ibkr_shortable_shares_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

    def test_complain_if_not_datetime_index(self):
        """
        Tests error handling when the reindex_like index is named Date but is
        not a DatetimeIndex.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_ibkr_shortable_shares_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_ibkr_borrow_fees")
    @patch("quantrocket.fundamental.download_ibkr_shortable_shares")
    @patch("quantrocket.fundamental.download_ibkr_margin_requirements")
    def test_pass_sids_and_dates_based_on_reindex_like(self,
                                                         mock_download_ibkr_margin_requirements,
                                                         mock_download_ibkr_shortable_shares,
                                                         mock_download_ibkr_borrow_fees):
        """
        Tests that sids and date ranges and corrected passed to the
        download_* functions based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     Sid=["FI12345",
                            "FI12345",
                            "FI23456"],
                     Quantity=[10000,
                               9000,
                               80000]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        mock_download_ibkr_shortable_shares.side_effect = _mock_download_ibkr_shortable_shares

        get_ibkr_shortable_shares_reindexed_like(closes, time="00:00:00 America/New_York")

        shortable_shares_call = mock_download_ibkr_shortable_shares.mock_calls[0]
        _, args, kwargs = shortable_shares_call
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2018-03-17") # 45 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-05-03")
        self.assertNotIn("aggregate", kwargs)

        # aggregate shortable shares
        get_ibkr_shortable_shares_reindexed_like(closes, aggregate=True)

        shortable_shares_call = mock_download_ibkr_shortable_shares.mock_calls[1]
        _, args, kwargs = shortable_shares_call
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2018-03-17") # 45 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-05-03")
        self.assertTrue(kwargs["aggregate"])

        def _mock_download_ibkr_borrow_fees(f, *args, **kwargs):
            borrow_fees = pd.DataFrame(
                dict(Date=["2018-05-01",
                           "2018-05-02",
                           "2018-05-03"],
                     Sid=["FI12345",
                            "FI12345",
                            "FI23456"],
                     FeeRate=[1.75,
                              1.79,
                              0.35]))
            borrow_fees.to_csv(f, index=False)
            f.seek(0)

        mock_download_ibkr_borrow_fees.side_effect = _mock_download_ibkr_borrow_fees

        get_ibkr_borrow_fees_reindexed_like(closes)

        borrow_fees_call = mock_download_ibkr_borrow_fees.mock_calls[0]
        _, args, kwargs = borrow_fees_call
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2018-03-17") # 45 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-05-03")

        def _mock_download_ibkr_margin_requirements(f, *args, **kwargs):
            margin_requirements = pd.DataFrame(
                dict(Date=["2018-05-01",
                           "2018-05-02",
                           "2018-05-03"],
                     Sid=["FI12345",
                            "FI12345",
                            "FI23456"],
                     ShortInitialMargin=[100,
                                        100,
                                        50],
                     ShortMaintenanceMargin=[100,
                                        100,
                                        50],
                     LongInitialMargin=[100,
                                        100,
                                        50],
                     LongMaintenanceMargin=[100,
                                        100,
                                        50]))
            margin_requirements.to_csv(f, index=False)
            f.seek(0)

        mock_download_ibkr_margin_requirements.side_effect = _mock_download_ibkr_margin_requirements

        get_ibkr_margin_requirements_reindexed_like(closes, time="00:00:00 America/Toronto")

        margin_requirements_call = mock_download_ibkr_margin_requirements.mock_calls[0]
        _, args, kwargs = margin_requirements_call
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2018-03-17") # 45 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-05-03")

    def test_complain_if_passed_timezone_not_match_reindex_like_timezone(self):
        """
        Tests error handling when a timezone is passed and reindex_like
        timezone is set and they do not match.
        """

        closes = pd.DataFrame(
                    np.random.rand(3,2),
                    columns=["FI12345","FI23456"],
                    index=pd.date_range(start="2018-05-01",
                                        periods=3,
                                        freq="D",
                                        tz="America/New_York",
                                        name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T23:15:02",
                           "2018-05-03T00:30:03",
                           "2018-05-01T21:45:02",
                           "2018-05-02T23:15:02",
                           "2018-05-03T00:30:03",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
                     Quantity=[10000,
                               9000,
                               80000,
                               3500,
                               3600,
                               3800
                               ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            with self.assertRaises(ParameterError) as cm:
                get_ibkr_shortable_shares_reindexed_like(closes, time="09:30:00 Europe/London")

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
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
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

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(
                closes,
                time="09:30:00 America/New_York")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', "FI12345": 10000.0, "FI23456": 3500.0},
                 {'Date': '2018-05-02T00:00:00', "FI12345": 80000.0, "FI23456": 3600.0},
                 {'Date': '2018-05-03T00:00:00', "FI12345": 80000.0, "FI23456": 3100.0}]
            )

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(
                closes,
                time="09:30:00 Europe/London")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', "FI12345": 10000.0, "FI23456": 3500.0},
                 {'Date': '2018-05-02T00:00:00', "FI12345": 9000.0, "FI23456": 3600.0},
                 {'Date': '2018-05-03T00:00:00', "FI12345": 80000.0, "FI23456": 3100.0}]
            )

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(
                closes,
                time="09:30:00 Japan")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', "FI12345": 10000.0, "FI23456": 3500.0},
                 {'Date': '2018-05-02T00:00:00', "FI12345": 9000.0, "FI23456": 3600.0},
                 {'Date': '2018-05-03T00:00:00', "FI12345": 80000.0, "FI23456": 3800.0}]
            )

    def test_use_reindex_like_timezone(self):
        """
        Tests that, when a timezone is not passed but reindex_like timezone
        is set, the latter is used.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
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

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(
                closes,
                time="09:30:00")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00-0400', "FI12345": 10000.0, "FI23456": 3500.0},
                 {'Date': '2018-05-02T00:00:00-0400', "FI12345": 80000.0, "FI23456": 3600.0},
                 {'Date': '2018-05-03T00:00:00-0400', "FI12345": 80000.0, "FI23456": 3100.0}]
            )

    @patch("quantrocket.fundamental.download_master_file")
    def test_infer_timezone_from_securities(self, mock_download_master_file):
        """
        Tests that, when timezone is not passed and reindex_like timezone is
        not set, the timezone is inferred from the component securities.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["Japan","Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
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

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(
                closes,
                time="09:30:00")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', "FI12345": 10000.0, "FI23456": 3500.0},
                 {'Date': '2018-05-02T00:00:00', "FI12345": 9000.0, "FI23456": 3600.0},
                 {'Date': '2018-05-03T00:00:00', "FI12345": 80000.0, "FI23456": 3800.0}]
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
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York","Japan"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     Sid=["FI12345",
                            "FI12345",
                            "FI23456"],
                     Quantity=[10000,
                               9000,
                               80000]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):
            with patch("quantrocket.fundamental.download_master_file", new=mock_download_master_file):

                with self.assertRaises(ParameterError) as cm:
                    get_ibkr_shortable_shares_reindexed_like(closes)

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
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     Sid=["FI12345",
                            "FI12345",
                            "FI23456"],
                     Quantity=[10000,
                               9000,
                               80000]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            with self.assertRaises(pytz.exceptions.UnknownTimeZoneError) as cm:
                get_ibkr_shortable_shares_reindexed_like(closes, time="09:30:00 Mars")

                self.assertIn("pytz.exceptions.UnknownTimeZoneError: 'Mars'", repr(cm.exception))

    def test_invalid_time(self):
        """
        Tests error handling when an invalid time is passed.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", tz="America/New_York",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-05-01T21:45:02",
                           "2018-05-01T22:00:03",
                           "2018-05-01T21:45:02"],
                     Sid=["FI12345",
                            "FI12345",
                            "FI23456"],
                     Quantity=[10000,
                               9000,
                               80000]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            with self.assertRaises(ParameterError) as cm:
                get_ibkr_shortable_shares_reindexed_like(closes, time="foo")

                self.assertIn("could not parse time 'foo': Unknown datetime string format, unable to parse", str(cm.exception))


    def test_pass_time(self):
        """
        Tests that, when a time arg is passed, it is used.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
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

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(
                closes,
                time="09:30:00")

            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00-0400', "FI12345": 10000.0, "FI23456": 3500.0},
                 {'Date': '2018-05-02T00:00:00-0400', "FI12345": 80000.0, "FI23456": 3600.0},
                 {'Date': '2018-05-03T00:00:00-0400', "FI12345": 80000.0, "FI23456": 3100.0}]
            )

    def test_no_pass_time(self):
        """
        Tests that, when no time arg is passed, the reindex_like times are
        used, which for a date index are 00:00:00.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            shortable_shares = pd.DataFrame(
                dict(Date=["2018-04-20T21:45:02",
                           "2018-05-01T13:45:02",
                           "2018-05-02T12:30:03",
                           "2018-04-20T21:45:02",
                           "2018-05-01T14:15:02",
                           "2018-05-02T14:30:03",
                           "2018-05-03T08:30:00",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
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

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(closes)
            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00-0400', "FI12345": 10000.0, "FI23456": 3500.0},
                 {'Date': '2018-05-02T00:00:00-0400', "FI12345": 9000.0, "FI23456": 3600.0},
                 {'Date': '2018-05-03T00:00:00-0400', "FI12345": 80000.0, "FI23456": 3800.0}]
            )

    def test_fillna_0_after_start_date(self):
        """
        Tests that NaN data after 2018-04-15 is converted to 0 but NaN data
        before is not. Applies to shortable shares and margin requirements.
        """
        closes = pd.DataFrame(
            np.random.rand(5,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-04-13",
                                periods=5,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            if kwargs.get("aggregate"):
                shortable_shares = pd.DataFrame(
                    dict(Date=[
                        "2018-04-15",
                        "2018-04-16",
                        "2018-04-17",
                        ],
                        Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            ],
                        MinQuantity=[
                            10,
                            900,
                            800],
                        MaxQuantity=[
                            12000,
                            19000,
                            84000],
                        MeanQuantity=[
                            5300,
                            4334,
                            42344],
                        LastQuantity=[
                            10000,
                            9000,
                            80000],
                        ))
            else:
                shortable_shares = pd.DataFrame(
                    dict(Date=["2018-04-15T21:45:02",
                            "2018-04-16T13:45:02",
                            "2018-04-17T12:30:03",
                            ],
                        Sid=["FI12345",
                                "FI12345",
                                "FI12345",
                            ],
                        Quantity=[10000,
                                9000,
                                80000,
                                ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(closes)
            # replace nan with "nan" to allow equality comparisons
            shortable_shares = shortable_shares.where(shortable_shares.notnull(), "nan")
            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2018-04-13T00:00:00-0400', "FI12345": "nan", "FI23456": "nan"},
                 {'Date': '2018-04-14T00:00:00-0400', "FI12345": "nan", "FI23456": "nan"},
                 {'Date': '2018-04-15T00:00:00-0400', "FI12345": "nan", "FI23456": "nan"},
                 {'Date': '2018-04-16T00:00:00-0400', "FI12345": 10000.0, "FI23456": 0.0},
                 {'Date': '2018-04-17T00:00:00-0400', "FI12345": 9000.0, "FI23456": 0.0}]
            )

            # aggregate shortable shares
            shortable_shares = get_ibkr_shortable_shares_reindexed_like(closes, aggregate=True)
            # replace nan with "nan" to allow equality comparisons
            shortable_shares = shortable_shares.where(shortable_shares.notnull(), "nan")
            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%d")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Field': 'MinQuantity', 'Date': '2018-04-13', 'FI12345': 'nan', 'FI23456': 'nan'},
                {'Field': 'MinQuantity', 'Date': '2018-04-14', 'FI12345': 'nan', 'FI23456': 'nan'},
                {'Field': 'MinQuantity', 'Date': '2018-04-15', 'FI12345': 10.0, 'FI23456': 'nan'},
                {'Field': 'MinQuantity', 'Date': '2018-04-16', 'FI12345': 900.0, 'FI23456': 0.0},
                {'Field': 'MinQuantity', 'Date': '2018-04-17', 'FI12345': 800.0, 'FI23456': 0.0},
                {'Field': 'MaxQuantity', 'Date': '2018-04-13', 'FI12345': 'nan', 'FI23456': 'nan'},
                {'Field': 'MaxQuantity', 'Date': '2018-04-14', 'FI12345': 'nan', 'FI23456': 'nan'},
                {'Field': 'MaxQuantity', 'Date': '2018-04-15', 'FI12345': 12000.0, 'FI23456': 'nan'},
                {'Field': 'MaxQuantity', 'Date': '2018-04-16', 'FI12345': 19000.0, 'FI23456': 0.0},
                {'Field': 'MaxQuantity', 'Date': '2018-04-17', 'FI12345': 84000.0, 'FI23456': 0.0},
                {'Field': 'MeanQuantity', 'Date': '2018-04-13', 'FI12345': 'nan', 'FI23456': 'nan'},
                {'Field': 'MeanQuantity', 'Date': '2018-04-14', 'FI12345': 'nan', 'FI23456': 'nan'},
                {'Field': 'MeanQuantity', 'Date': '2018-04-15', 'FI12345': 5300.0, 'FI23456': 'nan'},
                {'Field': 'MeanQuantity', 'Date': '2018-04-16', 'FI12345': 4334.0, 'FI23456': 0.0},
                {'Field': 'MeanQuantity', 'Date': '2018-04-17', 'FI12345': 42344.0, 'FI23456': 0.0},
                {'Field': 'LastQuantity', 'Date': '2018-04-13', 'FI12345': 'nan', 'FI23456': 'nan'},
                {'Field': 'LastQuantity', 'Date': '2018-04-14', 'FI12345': 'nan', 'FI23456': 'nan'},
                {'Field': 'LastQuantity', 'Date': '2018-04-15', 'FI12345': 10000.0, 'FI23456': 'nan'},
                {'Field': 'LastQuantity', 'Date': '2018-04-16', 'FI12345': 9000.0, 'FI23456': 0.0},
                {'Field': 'LastQuantity', 'Date': '2018-04-17', 'FI12345': 80000.0, 'FI23456': 0.0}]
            )

        def mock_download_ibkr_margin_requirements(f, *args, **kwargs):
            margin_requirements = pd.DataFrame(
                dict(Date=["2018-04-15T21:45:02",
                           "2018-04-16T13:45:02",
                           "2018-04-17T12:30:03",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                           ],
                     ShortInitialMargin=[100,
                                        50,
                                        75,
                                        ],
                     ShortMaintenanceMargin=[100,
                                        50,
                                        75,
                                        ],
                     LongInitialMargin=[100,
                                        50,
                                        75,
                                        ],
                     LongMaintenanceMargin=[100,
                                        50,
                                        75,
                                        ]))
            margin_requirements.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_margin_requirements', new=mock_download_ibkr_margin_requirements):

            margin_requirements = get_ibkr_margin_requirements_reindexed_like(closes)
            # replace nan with "nan" to allow equality comparisons
            margin_requirements = margin_requirements.where(margin_requirements.notnull(), "nan")
            margin_requirements = margin_requirements.reset_index()
            margin_requirements["Date"] = margin_requirements.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                margin_requirements.to_dict(orient="records"),
                [{'Date': '2018-04-13T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-14T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-15T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 100.0,
                'FI23456': 0.0,
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-17T00:00:00-0400',
                'FI12345': 50.0,
                'FI23456': 0.0,
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-13T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'ShortMaintenanceMargin'},
                {'Date': '2018-04-14T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'ShortMaintenanceMargin'},
                {'Date': '2018-04-15T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'ShortMaintenanceMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 100.0,
                'FI23456': 0.0,
                'Field': 'ShortMaintenanceMargin'},
                {'Date': '2018-04-17T00:00:00-0400',
                'FI12345': 50.0,
                'FI23456': 0.0,
                'Field': 'ShortMaintenanceMargin'},
                {'Date': '2018-04-13T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-14T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-15T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 100.0,
                'FI23456': 0.0,
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-17T00:00:00-0400',
                'FI12345': 50.0,
                'FI23456': 0.0,
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-13T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'LongMaintenanceMargin'},
                {'Date': '2018-04-14T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'LongMaintenanceMargin'},
                {'Date': '2018-04-15T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'LongMaintenanceMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 100.0,
                'FI23456': 0.0,
                'Field': 'LongMaintenanceMargin'},
                {'Date': '2018-04-17T00:00:00-0400',
                'FI12345': 50.0,
                'FI23456': 0.0,
                'Field': 'LongMaintenanceMargin'}]
            )

    def test_shift(self):
        """
        Tests the shift parameter.
        """
        closes = pd.DataFrame(
            np.random.rand(2,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2019-04-16",
                                periods=2,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            if kwargs.get("aggregate"):
                shortable_shares = pd.DataFrame(
                    dict(Date=[
                        "2019-04-15",
                        "2019-04-16",
                        "2019-04-17",
                        "2019-04-15",
                        "2019-04-16",
                        ],
                        Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456"
                            ],
                        MinQuantity=[
                            10,
                            900,
                            800,
                            50,
                            60],
                        MaxQuantity=[
                            12000,
                            19000,
                            84000,
                            500,
                            900],
                        MeanQuantity=[
                            5300,
                            4334,
                            42344,
                            151,
                            251],
                        LastQuantity=[
                            10000,
                            9000,
                            80000,
                            200,
                            300],
                        ))
            else:
                shortable_shares = pd.DataFrame(
                    dict(Date=["2019-04-15T00:45:02",
                            "2019-04-16T00:45:02",
                            "2019-04-17T00:30:03",
                            "2019-04-15T00:00:00",
                            "2019-04-16T00:00:00",
                            ],
                        Sid=["FI12345",
                                "FI12345",
                                "FI12345",
                                "FI23456",
                                "FI23456"
                            ],
                        Quantity=[10000,
                                9000,
                                80000,
                                200,
                                300
                                ]))
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            # aggregate shortable shares

            # without shift
            shortable_shares = get_ibkr_shortable_shares_reindexed_like(closes, aggregate=True, fields="MeanQuantity")
            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%d")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Field': 'MeanQuantity', 'Date': '2019-04-16', 'FI12345': 4334.0, 'FI23456': 251.0},
                {'Field': 'MeanQuantity', 'Date': '2019-04-17', 'FI12345': 42344.0, 'FI23456': 251.0}]
            )
            # with shift
            shortable_shares = get_ibkr_shortable_shares_reindexed_like(closes, aggregate=True, fields="MeanQuantity", shift=1)
            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%d")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Field': 'MeanQuantity', 'Date': '2019-04-16', 'FI12345': 5300.0, 'FI23456': 151.0},
                {'Field': 'MeanQuantity', 'Date': '2019-04-17', 'FI12345': 4334.0, 'FI23456': 251.0}]
            )

            # without shift
            shortable_shares = get_ibkr_shortable_shares_reindexed_like(closes, time="09:30:00 America/New_York")
            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Date': '2019-04-16T00:00:00-0400', 'FI12345': 9000.0, 'FI23456': 300.0},
                {'Date': '2019-04-17T00:00:00-0400', 'FI12345': 80000.0, 'FI23456': 300.0}]
            )

            # with shift
            shortable_shares = get_ibkr_shortable_shares_reindexed_like(closes, time="09:30:00 America/New_York", shift=1)
            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                # currently, first row is Nan (becomes 0) for intraday dataframes
                [{'Date': '2019-04-16T00:00:00-0400', 'FI12345': 0.0, 'FI23456': 0.0},
                {'Date': '2019-04-17T00:00:00-0400', 'FI12345': 9000.0, 'FI23456': 300.0}]
            )

        def mock_download_ibkr_margin_requirements(f, *args, **kwargs):
            margin_requirements = pd.DataFrame(
                dict(Date=["2019-04-15T00:45:02",
                           "2019-04-16T00:45:02",
                           "2019-04-17T00:30:03",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                           ],
                     ShortInitialMargin=[100,
                                        50,
                                        75,
                                        ],
                     ShortMaintenanceMargin=[100,
                                        50,
                                        75,
                                        ],
                     LongInitialMargin=[100,
                                        50,
                                        75,
                                        ],
                     LongMaintenanceMargin=[100,
                                        50,
                                        75,
                                        ]))
            margin_requirements.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_margin_requirements', new=mock_download_ibkr_margin_requirements):

            # without shift
            margin_requirements = get_ibkr_margin_requirements_reindexed_like(closes)
            margin_requirements = margin_requirements.loc["ShortInitialMargin"]
            margin_requirements = margin_requirements.reset_index()
            margin_requirements["Date"] = margin_requirements.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                margin_requirements.to_dict(orient="records"),
                [{'Date': '2019-04-16T00:00:00-0400', 'FI12345': 50.0, 'FI23456': 0.0},
                {'Date': '2019-04-17T00:00:00-0400', 'FI12345': 75.0, 'FI23456': 0.0}]
            )

            # with shift
            margin_requirements = get_ibkr_margin_requirements_reindexed_like(closes, shift=1)
            margin_requirements = margin_requirements.loc["ShortInitialMargin"]
            margin_requirements = margin_requirements.reset_index()
            margin_requirements["Date"] = margin_requirements.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                margin_requirements.to_dict(orient="records"),
                # currently, first row is Nan (becomes 0) for intraday dataframes
                [{'Date': '2019-04-16T00:00:00-0400', 'FI12345': 0.0, 'FI23456': 0.0},
                {'Date': '2019-04-17T00:00:00-0400', 'FI12345': 50.0, 'FI23456': 0.0}]
            )

class IBKRShortableSharesReindexedLikeTestCase(unittest.TestCase):
    """
    See also StockloanDataReindexedLikeTestCase.
    """

    def test_complain_if_aggregate_and_time(self):
        """
        Tests error handling when you request aggregate=True and specify a time.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_ibkr_shortable_shares_reindexed_like(closes, aggregate=True, time="14:00:00")

        self.assertIn("the time argument is only supported if aggregate=False", str(cm.exception))

    def test_complain_if_intraday_and_fields(self):
        """
        Tests error handling when you request aggregate=False and specify fields.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_ibkr_shortable_shares_reindexed_like(closes, fields="MinQuantity")

        self.assertIn("the fields parameter is only supported if aggregate=True", str(cm.exception))

    def test_aggregate_shortable_shares_limit_fields(self):
        """
        Tests limiting fields for aggregate shortable shares.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-15",
                                periods=3,
                                freq="D",
                                name="Date"))

        def mock_download_ibkr_shortable_shares(f, *args, **kwargs):
            if kwargs.get("aggregate"):
                shortable_shares = pd.DataFrame(
                    dict(Date=[
                        "2018-05-15",
                        "2018-05-16",
                        "2018-05-17",
                        "2018-05-16",
                        ],
                        Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            ],
                        MinQuantity=[
                            10,
                            900,
                            800,
                            600],
                        MaxQuantity=[
                            12000,
                            19000,
                            84000,
                            6000],
                        MeanQuantity=[
                            5300,
                            4334,
                            42344,
                            678],
                        LastQuantity=[
                            10000,
                            9000,
                            80000,
                            5600],
                        ))
            else:
                raise NotImplementedError()
            shortable_shares.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_shortable_shares', new=mock_download_ibkr_shortable_shares):

            shortable_shares = get_ibkr_shortable_shares_reindexed_like(
                closes,
                aggregate=True,
                fields=["MeanQuantity", "LastQuantity"])

            # replace nan with "nan" to allow equality comparisons
            shortable_shares = shortable_shares.where(shortable_shares.notnull(), "nan")
            shortable_shares = shortable_shares.reset_index()
            shortable_shares["Date"] = shortable_shares.Date.dt.strftime("%Y-%m-%d")
            self.assertListEqual(
                shortable_shares.to_dict(orient="records"),
                [{'Field': 'MeanQuantity', 'Date': '2018-05-15', 'FI12345': 5300.0, 'FI23456': 0.0},
                {'Field': 'MeanQuantity', 'Date': '2018-05-16', 'FI12345': 4334.0, 'FI23456': 678.0},
                {'Field': 'MeanQuantity', 'Date': '2018-05-17', 'FI12345': 42344.0, 'FI23456': 678.0},
                {'Field': 'LastQuantity', 'Date': '2018-05-15', 'FI12345': 10000.0, 'FI23456': 0.0},
                {'Field': 'LastQuantity', 'Date': '2018-05-16', 'FI12345': 9000.0, 'FI23456': 5600.0},
                {'Field': 'LastQuantity', 'Date': '2018-05-17', 'FI12345': 80000.0, 'FI23456': 5600.0}]
            )

class IBKRBorrowFeesReindexedLikeTestCase(unittest.TestCase):

    def test_borrow_fees(self):
        """
        Tests get_ibkr_borrow_fees_reindexed_like. (get_ibkr_borrow_fees_reindexed_like
        and get_ibkr_shortable_shares_reindexed_like share a base function so for
        the most part testing one tests both.)
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                name="Date"))

        def mock_download_ibkr_borrow_fees(f, *args, **kwargs):
            borrow_fees = pd.DataFrame(
                dict(Date=["2018-04-20",
                           "2018-05-01",
                           "2018-05-02",
                           "2018-04-20",
                           "2018-05-01",
                           "2018-05-02",
                           "2018-05-03",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
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

        with patch('quantrocket.fundamental.download_ibkr_borrow_fees', new=mock_download_ibkr_borrow_fees):

            borrow_fees = get_ibkr_borrow_fees_reindexed_like(closes)

            borrow_fees = borrow_fees.reset_index()
            borrow_fees["Date"] = borrow_fees.Date.dt.strftime("%Y-%m-%d")
            self.assertListEqual(
                borrow_fees.to_dict(orient="records"),
                [{'Date': '2018-05-01', "FI12345": 1.65, "FI23456": 0.40},
                 {'Date': '2018-05-02', "FI12345": 1.7, "FI23456": 0.44},
                 {'Date': '2018-05-03', "FI12345": 1.7, "FI23456": 0.23}]
            )

    def test_shift(self):
        """
        Tests the shift parameter for borrow fees.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                name="Date"))

        def mock_download_ibkr_borrow_fees(f, *args, **kwargs):
            borrow_fees = pd.DataFrame(
                dict(Date=["2018-04-20",
                           "2018-05-01",
                           "2018-05-02",
                           "2018-04-20",
                           "2018-05-01",
                           "2018-05-02",
                           "2018-05-03",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
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

        with patch('quantrocket.fundamental.download_ibkr_borrow_fees', new=mock_download_ibkr_borrow_fees):

            # without shift
            borrow_fees = get_ibkr_borrow_fees_reindexed_like(closes)

            borrow_fees = borrow_fees.reset_index()
            borrow_fees["Date"] = borrow_fees.Date.dt.strftime("%Y-%m-%d")
            self.assertListEqual(
                borrow_fees.to_dict(orient="records"),
                [{'Date': '2018-05-01', "FI12345": 1.65, "FI23456": 0.40},
                 {'Date': '2018-05-02', "FI12345": 1.7, "FI23456": 0.44},
                 {'Date': '2018-05-03', "FI12345": 1.7, "FI23456": 0.23}]
            )

            # with shift
            borrow_fees = get_ibkr_borrow_fees_reindexed_like(closes, shift=1)

            borrow_fees = borrow_fees.reset_index()
            borrow_fees["Date"] = borrow_fees.Date.dt.strftime("%Y-%m-%d")
            self.assertListEqual(
                borrow_fees.to_dict(orient="records"),
                [{'Date': '2018-05-01', 'FI12345': 1.5, 'FI23456': 0.35},
                {'Date': '2018-05-02', 'FI12345': 1.65, 'FI23456': 0.4},
                {'Date': '2018-05-03', 'FI12345': 1.7, 'FI23456': 0.44}]
            )

class IBKRMarginRequirementsReindexedLikeTestCase(unittest.TestCase):

    def test_get_ibkr_margin_requirements_reindexed_like(self):
        """
        Tests get_ibkr_margin_requirements_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(2,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-04-16",
                                periods=2,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_ibkr_margin_requirements(f, *args, **kwargs):
            margin_requirements = pd.DataFrame(
                dict(Date=["2018-04-15T21:45:02",
                           "2018-04-16T13:45:02",
                            "2018-04-15T21:45:02",
                           "2018-04-16T13:45:02"
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456"
                           ],
                     ShortInitialMargin=[100,
                                        50,
                                        75,
                                        0
                                        ],
                     ShortMaintenanceMargin=[100,
                                        50,
                                        75,
                                        0,
                                        ],
                     LongInitialMargin=[100,
                                        50,
                                        75,
                                        200
                                        ],
                     LongMaintenanceMargin=[100,
                                        50,
                                        75,
                                        300
                                        ]))
            margin_requirements.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_ibkr_margin_requirements', new=mock_download_ibkr_margin_requirements):

            margin_requirements = get_ibkr_margin_requirements_reindexed_like(closes)
            # replace nan with "nan" to allow equality comparisons
            margin_requirements = margin_requirements.where(margin_requirements.notnull(), "nan")
            margin_requirements = margin_requirements.reset_index()
            margin_requirements["Date"] = margin_requirements.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                margin_requirements.to_dict(orient="records"),
                [{'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 100.0,
                'FI23456': 75.0,
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-17T00:00:00-0400',
                'FI12345': 50.0,
                'FI23456': 0.0,
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 100.0,
                'FI23456': 75.0,
                'Field': 'ShortMaintenanceMargin'},
                {'Date': '2018-04-17T00:00:00-0400',
                'FI12345': 50.0,
                'FI23456': 0.0,
                'Field': 'ShortMaintenanceMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 100.0,
                'FI23456': 75.0,
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-17T00:00:00-0400',
                'FI12345': 50.0,
                'FI23456': 200.0,
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 100.0,
                'FI23456': 75.0,
                'Field': 'LongMaintenanceMargin'},
                {'Date': '2018-04-17T00:00:00-0400',
                'FI12345': 50.0,
                'FI23456': 300.0,
                'Field': 'LongMaintenanceMargin'}]
            )

    def test_get_ibkr_margin_requirements_reindexed_like_with_no_data(self):
        """
        Tests that get_ibkr_margin_requirements_reindexed_like returns 0s (or
        nans, pre data start date) when no margin requirements data is available
        for the requested sids.
        """
        closes = pd.DataFrame(
            np.random.rand(2,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-04-15",
                                periods=2,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_ibkr_margin_requirements(f, *args, **kwargs):
            raise NoFundamentalData("no data")

        with patch('quantrocket.fundamental.download_ibkr_margin_requirements', new=mock_download_ibkr_margin_requirements):

            margin_requirements = get_ibkr_margin_requirements_reindexed_like(closes)
            # replace nan with "nan" to allow equality comparisons
            margin_requirements = margin_requirements.where(margin_requirements.notnull(), "nan")
            margin_requirements = margin_requirements.reset_index()
            margin_requirements["Date"] = margin_requirements.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                margin_requirements.to_dict(orient="records"),
                [{'Date': '2018-04-15T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 0.0,
                'FI23456': 0.0,
                'Field': 'LongInitialMargin'},
                {'Date': '2018-04-15T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'LongMaintenanceMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 0.0,
                'FI23456': 0.0,
                'Field': 'LongMaintenanceMargin'},
                {'Date': '2018-04-15T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 0.0,
                'FI23456': 0.0,
                'Field': 'ShortInitialMargin'},
                {'Date': '2018-04-15T00:00:00-0400',
                'FI12345': 'nan',
                'FI23456': 'nan',
                'Field': 'ShortMaintenanceMargin'},
                {'Date': '2018-04-16T00:00:00-0400',
                'FI12345': 0.0,
                'FI23456': 0.0,
                'Field': 'ShortMaintenanceMargin'}]
            )

class AlpacaETBReindexedLikeTestCase(unittest.TestCase):

    def test_alpaca_etb(self):
        """
        Tests get_alpaca_etb_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2019-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_alpaca_etb(f, *args, **kwargs):
            etb = pd.DataFrame(
                dict(Date=["2019-05-01",
                           "2019-05-02",
                           "2019-05-03",
                           "2019-05-01",
                           "2019-05-02",
                           "2019-05-03",
                           ],
                     Sid=["FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456"],
                     EasyToBorrow=[1,
                               0,
                               1,
                               0,
                               0,
                               1,
                               ]))
            etb.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.fundamental.download_alpaca_etb', new=mock_download_alpaca_etb):

            etb = get_alpaca_etb_reindexed_like(closes)

            etb = etb.reset_index()
            etb["Date"] = etb.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                etb.to_dict(orient="records"),
                [{'Date': '2019-05-01T00:00:00-0400', "FI12345": True, "FI23456": False},
                 {'Date': '2019-05-02T00:00:00-0400', "FI12345": False, "FI23456": False},
                 {'Date': '2019-05-03T00:00:00-0400', "FI12345": True, "FI23456": True}]
            )

class SharadarFundamentalsReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.MultiIndex.from_product((
                pd.date_range(start="2018-01-01", periods=3, freq="D"),
                ["15:00:00","15:15:00"]), names=["Date", "Time"]))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_fundamentals_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

    def test_complain_if_date_level_not_in_index(self):
        """
        Tests error handling when reindex_like doesn't have an index named
        Date.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-01-01", periods=3, freq="D"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_fundamentals_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

    def test_complain_if_not_datetime_index(self):
        """
        Tests error handling when the reindex_like index is named Date but is
        not a DatetimeIndex.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_fundamentals_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    def test_complain_if_period_offset_positive(self):
        """
        Tests error handling when period_offset is positive.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-01-01", periods=3, freq="D", name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_fundamentals_reindexed_like(closes, period_offset=1)

        self.assertIn("period_offset must be a negative integer or 0", str(cm.exception))

    @patch("quantrocket.fundamental.download_sharadar_fundamentals")
    def test_pass_args_correctly(self,
                                 mock_download_sharadar_fundamentals):
        """
        Tests that sids, date ranges, and and other args are correctly
        passed to download_sharadar_fundamentals.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def _mock_download_sharadar_fundamentals(filepath_or_buffer, *args, **kwargs):
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
                    Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         "FI12345",
                         "FI12345",
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
            closes, fields=["EPS", "DATEKEY"], dimension="ARQ")

        sharadar_fundamentals_call = mock_download_sharadar_fundamentals.mock_calls[0]
        _, args, kwargs = sharadar_fundamentals_call
        self.assertEqual(kwargs["start_date"], "2016-09-02") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")
        self.assertEqual(kwargs["fields"], ["EPS", "DATEKEY"])
        self.assertEqual(kwargs["dimensions"], "ARQ")

    @patch("quantrocket.fundamental.download_sharadar_fundamentals")
    def test_pass_args_correctly_period_offset(self,
                                 mock_download_sharadar_fundamentals):
        """
        Tests that the start_date passed to download_sharadar_fundamentals
        is appropriately modified by period_offset.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def _mock_download_sharadar_fundamentals(filepath_or_buffer, *args, **kwargs):
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
                    Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         "FI12345",
                         "FI12345",
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
            closes, fields=["EPS", "DATEKEY"], dimension="ARQ", period_offset=-2)

        sharadar_fundamentals_call = mock_download_sharadar_fundamentals.mock_calls[0]
        _, args, kwargs = sharadar_fundamentals_call
        self.assertEqual(kwargs["start_date"], "2016-03-02") # (92*2+365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")

        get_sharadar_fundamentals_reindexed_like(
            closes, fields=["EPS", "DATEKEY"], dimension="ART", period_offset=-1)

        sharadar_fundamentals_call = mock_download_sharadar_fundamentals.mock_calls[1]
        _, args, kwargs = sharadar_fundamentals_call
        self.assertEqual(kwargs["start_date"], "2016-06-02") # (92*1+365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")

        get_sharadar_fundamentals_reindexed_like(
            closes, fields=["EPS", "DATEKEY"], dimension="ARY", period_offset=-3)

        sharadar_fundamentals_call = mock_download_sharadar_fundamentals.mock_calls[2]
        _, args, kwargs = sharadar_fundamentals_call
        self.assertEqual(kwargs["start_date"], "2013-09-03") # (365*3+365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-01")

    def test_dedupe_datekey(self):
        """
        Tests that duplicate DATEKEYS (resulting from reporting several
        fiscal periods at once) are deduped by keeping the latest record.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=["FI12345"],
            index=pd.date_range(start="2018-03-01", periods=6, freq="MS", name="Date"))

        def mock_download_sharadar_fundamentals(filepath_or_buffer, *args, **kwargs):
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
                     Sid=[
                         "FI12345",
                         "FI12345",
                         ],
                     EPS=[
                         565,
                         580
                     ]))
            fundamentals.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, fields="EPS")

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"EPS"})

        eps = fundamentals.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(eps.columns))
        self.assertEqual(eps["FI12345"].loc["2018-08-01"], 580)

    def test_ffill_no_lookahead_bias(self):
        """
        Tests that financial statement metrics are ffilled and are shifted
        forward 1 period to avoid lookahead bias.
        """
        closes = pd.DataFrame(
            np.random.rand(6,1),
            columns=["FI12345"],
            index=pd.date_range(start="2018-07-20", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_fundamentals(filepath_or_buffer, *args, **kwargs):
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
                     Sid=[
                         "FI12345",
                         "FI12345",
                         ],
                     EPS=[
                         565,
                         580
                     ]))
            fundamentals.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, fields=["EPS"])

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"EPS"})

        eps = fundamentals.loc["EPS"]
        self.assertListEqual(list(eps.index), list(eps.index))
        self.assertListEqual(list(eps.columns), list(eps.columns))
        self.assertEqual(eps["FI12345"].loc["2018-07-23"], 565)
        self.assertEqual(eps["FI12345"].loc["2018-07-24"], 580)

    def test_period_offset(self):
        """
        Tests that period_offset can be used to return financial statement metrics
        for a prior fiscal period.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345", "FI23456"],
            index=pd.date_range(start="2018-07-20", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_fundamentals(filepath_or_buffer, *args, **kwargs):
            fundamentals = pd.DataFrame(
                dict(
                    REPORTPERIOD=[
                        "2017-06-30",
                        "2017-09-30",
                        "2017-12-31",
                        "2018-03-30",
                        "2018-06-30",
                        "2017-06-30",
                        "2017-09-30",
                        "2017-12-31",
                        "2018-03-30",
                        "2018-06-30"
                        ],
                    DATEKEY=[
                        "2017-07-23",
                        "2017-10-23",
                        "2018-01-23",
                        "2018-04-23",
                        "2018-07-23",
                        "2017-09-22",
                        "2017-12-22",
                        "2018-03-22",
                        "2018-06-22",
                        "2018-09-22",
                        ],
                     Sid=[
                         "FI12345",
                         "FI12345",
                         "FI12345",
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         "FI23456",
                         "FI23456",
                         "FI23456",
                         ],
                     EPS=[
                         400,
                         450,
                         500,
                         565,
                         580,
                         40,
                         45,
                         50,
                         56,
                         58
                     ]))
            fundamentals.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        # no period offset
        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, fields=["EPS"], dimension="ART")

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"EPS"})

        eps = fundamentals.loc["EPS"]
        self.assertListEqual(list(eps.index), list(eps.index))
        self.assertListEqual(list(eps.columns), list(eps.columns))
        self.assertEqual(eps["FI12345"].loc["2018-07-23"], 565)
        self.assertEqual(eps["FI12345"].loc["2018-07-24"], 580)
        self.assertEqual(eps["FI23456"].loc["2018-07-23"], 56)
        self.assertEqual(eps["FI23456"].loc["2018-07-24"], 56)

        # period_offset = -1
        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, fields=["EPS"], dimension="ART", period_offset=-1)

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"EPS"})

        eps = fundamentals.loc["EPS"]
        self.assertListEqual(list(eps.index), list(eps.index))
        self.assertListEqual(list(eps.columns), list(eps.columns))
        self.assertEqual(eps["FI12345"].loc["2018-07-23"], 500)
        self.assertEqual(eps["FI12345"].loc["2018-07-24"], 565)
        self.assertEqual(eps["FI23456"].loc["2018-07-23"], 50)
        self.assertEqual(eps["FI23456"].loc["2018-07-24"], 50)

        # period_offset = -2
        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, fields=["EPS"], dimension="ART", period_offset=-2)

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"EPS"})

        eps = fundamentals.loc["EPS"]
        self.assertListEqual(list(eps.index), list(eps.index))
        self.assertListEqual(list(eps.columns), list(eps.columns))
        self.assertEqual(eps["FI12345"].loc["2018-07-23"], 450)
        self.assertEqual(eps["FI12345"].loc["2018-07-24"], 500)
        self.assertEqual(eps["FI23456"].loc["2018-07-23"], 45)
        self.assertEqual(eps["FI23456"].loc["2018-07-24"], 45)

        # period_offset = -3
        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, fields=["EPS"], dimension="ART", period_offset=-3)

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"EPS"})

        eps = fundamentals.loc["EPS"]
        self.assertListEqual(list(eps.index), list(eps.index))
        self.assertListEqual(list(eps.columns), list(eps.columns))
        self.assertEqual(eps["FI12345"].loc["2018-07-23"], 400)
        self.assertEqual(eps["FI12345"].loc["2018-07-24"], 450)
        self.assertEqual(eps["FI23456"].loc["2018-07-23"], 40)
        self.assertEqual(eps["FI23456"].loc["2018-07-24"], 40)

    def test_tz_aware_index(self):
        """
        Tests that reindex_like.index can be tz-naive or tz-aware.
        """
        def mock_download_sharadar_fundamentals(filepath_or_buffer, *args, **kwargs):
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
                     Sid=[
                         "FI12345",
                         "FI12345",
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
                columns=["FI12345"],
                index=pd.date_range(start="2018-07-05", periods=4, freq="D", name="Date"))

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, fields="REVENUE")

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"REVENUE"})

        revenues = fundamentals.loc["REVENUE"]
        self.assertListEqual(list(revenues.index), list(revenues.index))
        self.assertListEqual(list(revenues.columns), list(revenues.columns))
        revenues = revenues.reset_index()
        revenues["Date"] = revenues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            revenues.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00', "FI12345": 580.0},
             {'Date': '2018-07-06T00:00:00', "FI12345": 580.0},
             {'Date': '2018-07-07T00:00:00', "FI12345": 542.0},
             {'Date': '2018-07-08T00:00:00', "FI12345": 542.0}]
        )

        with patch('quantrocket.fundamental.download_sharadar_fundamentals', new=mock_download_sharadar_fundamentals):

            # request with tz-aware
            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-07-05", periods=4, freq="D", tz="America/New_York", name="Date"))

            fundamentals = get_sharadar_fundamentals_reindexed_like(
                closes, fields="REVENUE")

        self.assertSetEqual(set(fundamentals.index.get_level_values("Field")), {"REVENUE"})

        revenues = fundamentals.loc["REVENUE"]
        self.assertListEqual(list(revenues.index), list(revenues.index))
        self.assertListEqual(list(revenues.columns), list(revenues.columns))
        revenues = revenues.reset_index()
        revenues["Date"] = revenues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            revenues.to_dict(orient="records"),
            [{'Date': '2018-07-05T00:00:00-0400', "FI12345": 580.0},
             {'Date': '2018-07-06T00:00:00-0400', "FI12345": 580.0},
             {'Date': '2018-07-07T00:00:00-0400', "FI12345": 542.0},
             {'Date': '2018-07-08T00:00:00-0400', "FI12345": 542.0}]
        )

class SharadarInstitutionsReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.MultiIndex.from_product((
                pd.date_range(start="2018-01-01", periods=3, freq="D"),
                ["15:00:00","15:15:00"]), names=["Date", "Time"]))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_institutions_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

    def test_complain_if_date_level_not_in_index(self):
        """
        Tests error handling when reindex_like doesn't have an index named
        Date.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-01-01", periods=3, freq="D"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_institutions_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

    def test_complain_if_not_datetime_index(self):
        """
        Tests error handling when the reindex_like index is named Date but is
        not a DatetimeIndex.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_institutions_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_sharadar_institutions")
    def test_pass_args_correctly(self,
                                 mock_download_sharadar_institutions):
        """
        Tests that sids, date ranges, and and other args are correctly
        passed to download_sharadar_institutions.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def _mock_download_sharadar_institutions(filepath_or_buffer, *args, **kwargs):
            institutions = pd.DataFrame(
                dict(
                    CALENDARDATE=[
                        "2018-03-31",
                        "2018-06-30",
                        "2018-03-31",
                        "2018-06-30"
                        ],
                    Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         ],
                     SHRVALUE=[
                         500000,
                         600000,
                         700000,
                         800000,
                     ],
                     TOTALVALUE=[
                         1500000,
                         1600000,
                         1700000,
                         1800000,
                     ]))
            institutions.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        mock_download_sharadar_institutions.side_effect = _mock_download_sharadar_institutions

        get_sharadar_institutions_reindexed_like(
            closes, fields=["SHRVALUE", "TOTALVALUE"])

        sharadar_institutions_call = mock_download_sharadar_institutions.mock_calls[0]
        _, args, kwargs = sharadar_institutions_call
        self.assertEqual(kwargs["start_date"], "2017-02-14") # 365+180 days before reindex_like min date
        self.assertEqual(kwargs["end_date"], "2018-08-18")
        self.assertEqual(kwargs["fields"], ["SHRVALUE", "TOTALVALUE"])

    def test_ffill_and_shift(self):
        """
        Tests that  metrics are ffilled and are shifted forward to avoid
        lookahead bias.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-11", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_institutions(filepath_or_buffer, *args, **kwargs):
            institutions = pd.DataFrame(
                dict(
                    CALENDARDATE=[
                        "2018-03-31",
                        "2018-06-30",
                        "2018-03-31",
                        "2018-06-30"
                        ],
                    Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         ],
                     SHRVALUE=[
                         500000,
                         600000,
                         700000,
                         800000,
                     ],
                     TOTALVALUE=[
                         1500000,
                         1600000,
                         1700000,
                         1800000,
                     ]))
            institutions.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch("quantrocket.fundamental.download_sharadar_institutions", new=mock_download_sharadar_institutions):
            institutions = get_sharadar_institutions_reindexed_like(
                closes, fields=["SHRVALUE", "TOTALVALUE"])

        self.assertSetEqual(set(institutions.index.get_level_values("Field")), {"SHRVALUE", "TOTALVALUE"})

        sharevalues = institutions.loc["SHRVALUE"]
        sharevalues = sharevalues.reset_index()
        sharevalues["Date"] = sharevalues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            sharevalues.to_dict(orient="records"),
            [{'Date': '2018-08-11T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-12T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-13T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-14T00:00:00', 'FI12345': 600000.0, 'FI23456': 800000.0},
            {'Date': '2018-08-15T00:00:00', 'FI12345': 600000.0, 'FI23456': 800000.0},
            {'Date': '2018-08-16T00:00:00', 'FI12345': 600000.0, 'FI23456': 800000.0}]
        )

        totalvalues = institutions.loc["TOTALVALUE"]
        totalvalues = totalvalues.reset_index()
        totalvalues["Date"] = totalvalues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            totalvalues.to_dict(orient="records"),
            [{'Date': '2018-08-11T00:00:00', 'FI12345': 1500000.0, 'FI23456': 1700000.0},
            {'Date': '2018-08-12T00:00:00', 'FI12345': 1500000.0, 'FI23456': 1700000.0},
            {'Date': '2018-08-13T00:00:00', 'FI12345': 1500000.0, 'FI23456': 1700000.0},
            {'Date': '2018-08-14T00:00:00', 'FI12345': 1600000.0, 'FI23456': 1800000.0},
            {'Date': '2018-08-15T00:00:00', 'FI12345': 1600000.0, 'FI23456': 1800000.0},
            {'Date': '2018-08-16T00:00:00', 'FI12345': 1600000.0, 'FI23456': 1800000.0}]
        )

        # Repeat with a custom shift
        with patch("quantrocket.fundamental.download_sharadar_institutions", new=mock_download_sharadar_institutions):
            institutions = get_sharadar_institutions_reindexed_like(
                closes, fields=["SHRVALUE", "TOTALVALUE"], shift=47)

        self.assertSetEqual(set(institutions.index.get_level_values("Field")), {"SHRVALUE", "TOTALVALUE"})

        sharevalues = institutions.loc["SHRVALUE"]
        sharevalues = sharevalues.reset_index()
        sharevalues["Date"] = sharevalues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            sharevalues.to_dict(orient="records"),
            [{'Date': '2018-08-11T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-12T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-13T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-14T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-15T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-16T00:00:00', 'FI12345': 600000.0, 'FI23456': 800000.0}]
        )

    def test_tz_aware_index(self):
        """
        Tests that reindex_like.index can be tz-naive or tz-aware.
        """
        def mock_download_sharadar_institutions(filepath_or_buffer, *args, **kwargs):
            institutions = pd.DataFrame(
                dict(
                    CALENDARDATE=[
                        "2018-03-31",
                        "2018-06-30",
                        "2018-03-31",
                        "2018-06-30"
                        ],
                    Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         "FI23456",
                         ],
                     SHRVALUE=[
                         500000,
                         600000,
                         700000,
                         800000,
                     ],
                     TOTALVALUE=[
                         1500000,
                         1600000,
                         1700000,
                         1800000,
                     ]))
            institutions.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        # request with tz_naive
        with patch("quantrocket.fundamental.download_sharadar_institutions", new=mock_download_sharadar_institutions):

            closes = pd.DataFrame(
                np.random.rand(6,2),
                columns=["FI12345","FI23456"],
                index=pd.date_range(start="2018-08-11", periods=6, freq="D", name="Date"))

            institutions = get_sharadar_institutions_reindexed_like(
                closes, fields=["SHRVALUE", "TOTALVALUE"])

        self.assertSetEqual(set(institutions.index.get_level_values("Field")), {"SHRVALUE", "TOTALVALUE"})

        sharevalues = institutions.loc["SHRVALUE"]
        sharevalues = sharevalues.reset_index()
        sharevalues["Date"] = sharevalues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            sharevalues.to_dict(orient="records"),
            [{'Date': '2018-08-11T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-12T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-13T00:00:00', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-14T00:00:00', 'FI12345': 600000.0, 'FI23456': 800000.0},
            {'Date': '2018-08-15T00:00:00', 'FI12345': 600000.0, 'FI23456': 800000.0},
            {'Date': '2018-08-16T00:00:00', 'FI12345': 600000.0, 'FI23456': 800000.0}]
        )

        # request with tz aware
        with patch("quantrocket.fundamental.download_sharadar_institutions", new=mock_download_sharadar_institutions):

            closes = pd.DataFrame(
                np.random.rand(6,2),
                columns=["FI12345","FI23456"],
                index=pd.date_range(start="2018-08-11", periods=6, tz='America/New_York', freq="D", name="Date"))

            institutions = get_sharadar_institutions_reindexed_like(
                closes, fields=["SHRVALUE", "TOTALVALUE"])

        self.assertSetEqual(set(institutions.index.get_level_values("Field")), {"SHRVALUE", "TOTALVALUE"})

        sharevalues = institutions.loc["SHRVALUE"]
        sharevalues = sharevalues.reset_index()
        sharevalues["Date"] = sharevalues.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            sharevalues.to_dict(orient="records"),
            [{'Date': '2018-08-11T00:00:00-0400', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-12T00:00:00-0400', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-13T00:00:00-0400', 'FI12345': 500000.0, 'FI23456': 700000.0},
            {'Date': '2018-08-14T00:00:00-0400', 'FI12345': 600000.0, 'FI23456': 800000.0},
            {'Date': '2018-08-15T00:00:00-0400', 'FI12345': 600000.0, 'FI23456': 800000.0},
            {'Date': '2018-08-16T00:00:00-0400', 'FI12345': 600000.0, 'FI23456': 800000.0}]
        )

class SharadarSEC8ReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.MultiIndex.from_product((
                pd.date_range(start="2018-01-01", periods=3, freq="D"),
                ["15:00:00","15:15:00"]), names=["Date", "Time"]))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_sec8_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

    def test_complain_if_date_level_not_in_index(self):
        """
        Tests error handling when reindex_like doesn't have an index named
        Date.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-01-01", periods=3, freq="D"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_sec8_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

    def test_complain_if_not_datetime_index(self):
        """
        Tests error handling when the reindex_like index is named Date but is
        not a DatetimeIndex.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_sec8_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_sharadar_sec8")
    def test_pass_args_correctly(self,
                                 mock_download_sharadar_sec8):
        """
        Tests that sids, date ranges, and event codes are correctly
        passed to download_sharadar_sec8.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def _mock_download_sharadar_sec8(filepath_or_buffer, *args, **kwargs):
            sec8 = pd.DataFrame(
                dict(
                    DATE=[
                        "2018-08-15",
                        "2018-08-16"
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         ],
                     EVENTCODE=[
                         13,
                         13
                     ],
                     ))
            sec8.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        mock_download_sharadar_sec8.side_effect = _mock_download_sharadar_sec8

        get_sharadar_sec8_reindexed_like(
            closes, event_codes=[13])

        sharadar_sec8_call = mock_download_sharadar_sec8.mock_calls[0]
        _, args, kwargs = sharadar_sec8_call
        self.assertEqual(kwargs["start_date"], "2018-08-13")
        self.assertEqual(kwargs["end_date"], "2018-08-18")
        self.assertEqual(kwargs["event_codes"], [13])
        self.assertEqual(kwargs["fields"], ["Sid","DATE","EVENTCODE"])

    def test_single_code(self):
        """
        Tests requesting a single event code.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_sec8(filepath_or_buffer, *args, **kwargs):
            sec8 = pd.DataFrame(
                dict(
                    DATE=[
                        "2018-08-15",
                        "2018-08-16"
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         ],
                     EVENTCODE=[
                         13,
                         13
                     ],
                     ))
            sec8.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch("quantrocket.fundamental.download_sharadar_sec8", new=mock_download_sharadar_sec8):
            have_events = get_sharadar_sec8_reindexed_like(closes, event_codes=[13])

        have_events = have_events.reset_index()
        have_events["Date"] = have_events.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            have_events.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-15T00:00:00', 'FI12345': True, 'FI23456': False},
            {'Date': '2018-08-16T00:00:00', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-17T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-18T00:00:00', 'FI12345': False, 'FI23456': False}]
        )

    def test_multiple_codes(self):
        """
        Tests requesting multiple event codes.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_sec8(filepath_or_buffer, *args, **kwargs):
            sec8 = pd.DataFrame(
                dict(
                    DATE=[
                        "2018-08-15",
                        "2018-08-17"
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         ],
                     EVENTCODE=[
                         13,
                         14
                     ],
                     ))
            sec8.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch("quantrocket.fundamental.download_sharadar_sec8", new=mock_download_sharadar_sec8):
            have_events = get_sharadar_sec8_reindexed_like(closes, event_codes=[13, 14])

        have_events = have_events.reset_index()
        have_events["Date"] = have_events.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            have_events.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-15T00:00:00', 'FI12345': True, 'FI23456': False},
            {'Date': '2018-08-16T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-17T00:00:00', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-18T00:00:00', 'FI12345': False, 'FI23456': False}]
        )

    def test_no_matching_events(self):
        """
        Tests that False is return (not an exception) when there are no matching events.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_sec8(filepath_or_buffer, *args, **kwargs):
            raise NoFundamentalData("no sec8 data matches the query parameters")

        with patch("quantrocket.fundamental.download_sharadar_sec8", new=mock_download_sharadar_sec8):
            have_events = get_sharadar_sec8_reindexed_like(closes, event_codes=[13])

        have_events = have_events.reset_index()
        have_events["Date"] = have_events.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            have_events.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-15T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-16T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-17T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-18T00:00:00', 'FI12345': False, 'FI23456': False}]
        )

    def test_tz_aware_index(self):
        """
        Tests that reindex_like.index can be tz-naive or tz-aware.
        """
        def mock_download_sharadar_sec8(filepath_or_buffer, *args, **kwargs):
            sec8 = pd.DataFrame(
                dict(
                    DATE=[
                        "2018-08-15",
                        "2018-08-16"
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         ],
                     EVENTCODE=[
                         13,
                         13
                     ],
                     ))
            sec8.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        # request with tz_naive
        with patch("quantrocket.fundamental.download_sharadar_sec8", new=mock_download_sharadar_sec8):

            closes = pd.DataFrame(
                np.random.rand(6,2),
                columns=["FI12345","FI23456"],
                index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

            have_events = get_sharadar_sec8_reindexed_like(closes, event_codes=[13])

        have_events = have_events.reset_index()
        have_events["Date"] = have_events.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            have_events.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-15T00:00:00', 'FI12345': True, 'FI23456': False},
            {'Date': '2018-08-16T00:00:00', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-17T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-18T00:00:00', 'FI12345': False, 'FI23456': False}]
        )

        # request with tz aware
        with patch("quantrocket.fundamental.download_sharadar_sec8", new=mock_download_sharadar_sec8):

            closes = pd.DataFrame(
                np.random.rand(6,2),
                columns=["FI12345","FI23456"],
                index=pd.date_range(start="2018-08-13", periods=6, tz='America/New_York', freq="D", name="Date"))

            have_events = get_sharadar_sec8_reindexed_like(closes, event_codes=[13])

        have_events = have_events.reset_index()
        have_events["Date"] = have_events.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            have_events.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00-0400', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00-0400', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-15T00:00:00-0400', 'FI12345': True, 'FI23456': False},
            {'Date': '2018-08-16T00:00:00-0400', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-17T00:00:00-0400', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-18T00:00:00-0400', 'FI12345': False, 'FI23456': False}]
        )

class SharadarSP500ReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.MultiIndex.from_product((
                pd.date_range(start="2018-01-01", periods=3, freq="D"),
                ["15:00:00","15:15:00"]), names=["Date", "Time"]))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_sp500_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

    def test_complain_if_date_level_not_in_index(self):
        """
        Tests error handling when reindex_like doesn't have an index named
        Date.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-01-01", periods=3, freq="D"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_sp500_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

    def test_complain_if_not_datetime_index(self):
        """
        Tests error handling when the reindex_like index is named Date but is
        not a DatetimeIndex.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_sharadar_sp500_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_sharadar_sp500")
    def test_pass_args_correctly(self,
                                 mock_download_sharadar_sp500):
        """
        Tests that sids, date ranges, and fields are correctly
        passed to download_sharadar_sp500.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def _mock_download_sharadar_sp500(filepath_or_buffer, *args, **kwargs):
            sp500 = pd.DataFrame(
                dict(
                    DATE=[
                        "1970-08-15",
                        "2018-08-16",
                        "2018-08-14"
                        ],
                    Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         ],
                     ACTION=[
                         "added",
                         "removed",
                         "added"
                     ],
                     ))
            sp500.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        mock_download_sharadar_sp500.side_effect = _mock_download_sharadar_sp500

        get_sharadar_sp500_reindexed_like(closes)

        sharadar_sp500_call = mock_download_sharadar_sp500.mock_calls[0]
        _, args, kwargs = sharadar_sp500_call
        self.assertNotIn("start_date", kwargs) # not called with start_date
        self.assertEqual(kwargs["end_date"], "2018-08-18")
        self.assertEqual(kwargs["sids"], ["FI12345", "FI23456"])
        self.assertEqual(kwargs["fields"], ["Sid","DATE","ACTION"])

    def test_in_sp500(self):
        """
        Tests requesting securities that were in the S&P500
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_sp500(filepath_or_buffer, *args, **kwargs):
            sp500 = pd.DataFrame(
                dict(
                    DATE=[
                        "1970-08-15",
                        "2018-08-16",
                        "2018-08-14"
                        ],
                    Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         ],
                     ACTION=[
                         "added",
                         "removed",
                         "added"
                     ],
                     ))
            sp500.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch("quantrocket.fundamental.download_sharadar_sp500", new=mock_download_sharadar_sp500):
            in_sp500 = get_sharadar_sp500_reindexed_like(closes)

        in_sp500 = in_sp500.reset_index()
        in_sp500["Date"] = in_sp500.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            in_sp500.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': True, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00', 'FI12345': True, 'FI23456': True},
            {'Date': '2018-08-15T00:00:00', 'FI12345': True, 'FI23456': True},
            {'Date': '2018-08-16T00:00:00', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-17T00:00:00', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-18T00:00:00', 'FI12345': False, 'FI23456': True}]
        )

    def test_no_matching_events(self):
        """
        Tests that False is returned (not an exception) when the securities were never
        in the S&P 500.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def mock_download_sharadar_sp500(filepath_or_buffer, *args, **kwargs):
            raise NoFundamentalData("no sp500 data matches the query parameters")

        with patch("quantrocket.fundamental.download_sharadar_sp500", new=mock_download_sharadar_sp500):
            in_sp500 = get_sharadar_sp500_reindexed_like(closes)

        in_sp500 = in_sp500.reset_index()
        in_sp500["Date"] = in_sp500.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            in_sp500.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-15T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-16T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-17T00:00:00', 'FI12345': False, 'FI23456': False},
            {'Date': '2018-08-18T00:00:00', 'FI12345': False, 'FI23456': False}]
        )

    def test_tz_aware_index(self):
        """
        Tests that reindex_like.index can be tz-naive or tz-aware.
        """
        def mock_download_sharadar_sp500(filepath_or_buffer, *args, **kwargs):
            sp500 = pd.DataFrame(
                dict(
                    DATE=[
                        "1970-08-15",
                        "2018-08-16",
                        "2018-08-14"
                        ],
                    Sid=[
                         "FI12345",
                         "FI12345",
                         "FI23456",
                         ],
                     ACTION=[
                         "added",
                         "removed",
                         "added"
                     ],
                     ))
            sp500.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        # request with tz_naive
        with patch("quantrocket.fundamental.download_sharadar_sp500", new=mock_download_sharadar_sp500):

            closes = pd.DataFrame(
                np.random.rand(6,2),
                columns=["FI12345","FI23456"],
                index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

            in_sp500 = get_sharadar_sp500_reindexed_like(closes)

        in_sp500 = in_sp500.reset_index()
        in_sp500["Date"] = in_sp500.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            in_sp500.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': True, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00', 'FI12345': True, 'FI23456': True},
            {'Date': '2018-08-15T00:00:00', 'FI12345': True, 'FI23456': True},
            {'Date': '2018-08-16T00:00:00', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-17T00:00:00', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-18T00:00:00', 'FI12345': False, 'FI23456': True}]
        )

        # request with tz aware
        with patch("quantrocket.fundamental.download_sharadar_sp500", new=mock_download_sharadar_sp500):

            closes = pd.DataFrame(
                np.random.rand(6,2),
                columns=["FI12345","FI23456"],
                index=pd.date_range(start="2018-08-13", periods=6, tz='America/New_York', freq="D", name="Date"))

            in_sp500 = get_sharadar_sp500_reindexed_like(closes)

        in_sp500 = in_sp500.reset_index()
        in_sp500["Date"] = in_sp500.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            in_sp500.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00-0400', 'FI12345': True, 'FI23456': False},
            {'Date': '2018-08-14T00:00:00-0400', 'FI12345': True, 'FI23456': True},
            {'Date': '2018-08-15T00:00:00-0400', 'FI12345': True, 'FI23456': True},
            {'Date': '2018-08-16T00:00:00-0400', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-17T00:00:00-0400', 'FI12345': False, 'FI23456': True},
            {'Date': '2018-08-18T00:00:00-0400', 'FI12345': False, 'FI23456': True}]
        )

class BrainReindexedLikeTestCase(unittest.TestCase):

    def test_complain_if_time_level_in_index(self):
        """
        Tests error handling when reindex_like has a Time level in the index.
        """

        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.MultiIndex.from_product((
                pd.date_range(start="2018-01-01", periods=3, freq="D"),
                ["15:00:00","15:15:00"]), names=["Date", "Time"]))

        with self.assertRaises(ParameterError) as cm:
            get_brain_bsi_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

        with self.assertRaises(ParameterError) as cm:
            get_brain_blmcf_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

        with self.assertRaises(ParameterError) as cm:
            get_brain_blmect_reindexed_like(closes)

        self.assertIn("reindex_like should not have 'Time' in index", str(cm.exception))

    def test_complain_if_date_level_not_in_index(self):
        """
        Tests error handling when reindex_like doesn't have an index named
        Date.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-01-01", periods=3, freq="D"))

        with self.assertRaises(ParameterError) as cm:
            get_brain_bsi_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

        with self.assertRaises(ParameterError) as cm:
            get_brain_blmcf_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

        with self.assertRaises(ParameterError) as cm:
            get_brain_blmect_reindexed_like(closes)

        self.assertIn("reindex_like must have index called 'Date'", str(cm.exception))

    def test_complain_if_not_datetime_index(self):
        """
        Tests error handling when the reindex_like index is named Date but is
        not a DatetimeIndex.
        """

        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.Index(["foo","bar","bat"], name="Date"))

        with self.assertRaises(ParameterError) as cm:
            get_brain_bsi_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

        with self.assertRaises(ParameterError) as cm:
            get_brain_blmcf_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

        with self.assertRaises(ParameterError) as cm:
            get_brain_blmect_reindexed_like(closes)

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.fundamental.download_brain_bsi")
    def test_bsi_pass_args_correctly(self,
                                 mock_download_brain_bsi):
        """
        Tests that sids, date ranges, and N are correctly
        passed to download_brain_bsi.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def _mock_download_brain_bsi(filepath_or_buffer, *args, **kwargs):
            bsi = pd.DataFrame(
                dict(
                    Date=[
                        "2018-08-15",
                        "2018-08-16"
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         ],
                     SENTIMENT_SCORE=[
                         0.5,
                         0.4
                     ],
                     VOLUME=[
                            1000000,
                            2000000
                     ],
                    ))
            bsi.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        mock_download_brain_bsi.side_effect = _mock_download_brain_bsi

        get_brain_bsi_reindexed_like(
            closes, N=1, fields=["SENTIMENT_SCORE", "VOLUME"])

        brain_bsi_call = mock_download_brain_bsi.mock_calls[0]
        _, args, kwargs = brain_bsi_call
        self.assertEqual(kwargs["start_date"], "2018-08-03")
        self.assertEqual(kwargs["end_date"], "2018-08-18")
        self.assertEqual(kwargs["N"], 1)
        self.assertEqual(kwargs["fields"], ["SENTIMENT_SCORE", "VOLUME"])

    def test_bsi(self):
        """
        Tests get_brain_bsi_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def mock_download_brain_bsi(filepath_or_buffer, *args, **kwargs):
            bsi = pd.DataFrame(
                dict(
                    Date=[
                        "2018-08-15",
                        "2018-08-15",
                        "2018-08-16",
                        '2018-08-17'
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         "FI12345",
                         "FI23456",
                         ],
                    SENTIMENT_SCORE=[
                        0.5,
                        0.4,
                        0.55,
                        0.45
                    ],
                    VOLUME=[
                        100,
                        200,
                        150,
                        250
                    ],
                )
            )
            bsi.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch("quantrocket.fundamental.download_brain_bsi", new=mock_download_brain_bsi):
            bsi = get_brain_bsi_reindexed_like(closes, N=7, fields=["SENTIMENT_SCORE", "VOLUME"])

        bsi.fillna(-999, inplace=True)
        bsi = bsi.loc["SENTIMENT_SCORE"].reset_index()
        bsi["Date"] = bsi.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            bsi.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': -999.0, 'FI23456': -999.0},
            {'Date': '2018-08-14T00:00:00', 'FI12345': -999.0, 'FI23456': -999.0},
            {'Date': '2018-08-15T00:00:00', 'FI12345': 0.5, 'FI23456': 0.4},
            {'Date': '2018-08-16T00:00:00', 'FI12345': 0.55, 'FI23456': -999.0},
            {'Date': '2018-08-17T00:00:00', 'FI12345': -999.0, 'FI23456': 0.45},
            {'Date': '2018-08-18T00:00:00', 'FI12345': -999.0, 'FI23456': -999.0}]
        )

        # repeat with tz-aware index
        with patch("quantrocket.fundamental.download_brain_bsi", new=mock_download_brain_bsi):
            closes.index = closes.index.tz_localize("America/New_York")
            bsi = get_brain_bsi_reindexed_like(closes, N=7, fields=["SENTIMENT_SCORE", "VOLUME"])

        bsi.fillna(-999, inplace=True)
        bsi = bsi.loc["SENTIMENT_SCORE"].reset_index()
        bsi["Date"] = bsi.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            bsi.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00-0400', 'FI12345': -999.0, 'FI23456': -999.0},
            {'Date': '2018-08-14T00:00:00-0400', 'FI12345': -999.0, 'FI23456': -999.0},
            {'Date': '2018-08-15T00:00:00-0400', 'FI12345': 0.5, 'FI23456': 0.4},
            {'Date': '2018-08-16T00:00:00-0400', 'FI12345': 0.55, 'FI23456': -999.0},
            {'Date': '2018-08-17T00:00:00-0400', 'FI12345': -999.0, 'FI23456': 0.45},
            {'Date': '2018-08-18T00:00:00-0400', 'FI12345': -999.0, 'FI23456': -999.0}]
        )

    @patch("quantrocket.fundamental.download_brain_blmcf")
    def test_blmcf_pass_args_correctly(self,
                                 mock_download_brain_blmcf):
        """
        Tests that sids, date ranges, and report_category are correctly
        passed to download_brain_blmcf.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def _mock_download_brain_blmcf(filepath_or_buffer, *args, **kwargs):
            metrics = pd.DataFrame(
                dict(
                    Date=[
                        "2018-05-15",
                        "2018-6-01",
                        "2018-08-15",
                        "2018-08-16",
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         "FI12345",
                         "FI23456",
                         ],
                     SENTIMENT=[
                         0.5,
                         0.4,
                         0.55,
                         0.45
                     ],
                    ))
            metrics.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        mock_download_brain_blmcf.side_effect = _mock_download_brain_blmcf

        get_brain_blmcf_reindexed_like(
            closes, fields="SENTIMENT")

        brain_call = mock_download_brain_blmcf.mock_calls[0]
        _, args, kwargs = brain_call
        self.assertEqual(kwargs["start_date"], "2017-02-14")
        self.assertEqual(kwargs["end_date"], "2018-08-18")
        self.assertEqual(kwargs["report_category"], None)
        self.assertEqual(kwargs["fields"], ["SENTIMENT"])

    def test_blmcf(self):
        """
        Tests get_brain_blmcf_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def mock_download_brain_blmcf(filepath_or_buffer, *args, **kwargs):
            metrics = pd.DataFrame(
                dict(
                    Date=[
                        "2018-05-15",
                        "2018-6-01",
                        "2018-08-15",
                        "2018-08-16",
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         "FI12345",
                         "FI23456",
                         ],
                     SENTIMENT=[
                         0.5,
                         0.4,
                         0.55,
                         0.45
                     ],
                    ))
            metrics.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch("quantrocket.fundamental.download_brain_blmcf", new=mock_download_brain_blmcf):
            metrics = get_brain_blmcf_reindexed_like(
                closes, fields=["SENTIMENT"])

        metrics = metrics.loc["SENTIMENT"].reset_index()
        metrics["Date"] = metrics.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            metrics.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': 0.5, 'FI23456': 0.4},
             {'Date': '2018-08-14T00:00:00', 'FI12345': 0.5, 'FI23456': 0.4},
             {'Date': '2018-08-15T00:00:00', 'FI12345': 0.55, 'FI23456': 0.4},
             {'Date': '2018-08-16T00:00:00', 'FI12345': 0.55, 'FI23456': 0.45},
             {'Date': '2018-08-17T00:00:00', 'FI12345': 0.55, 'FI23456': 0.45},
             {'Date': '2018-08-18T00:00:00', 'FI12345': 0.55, 'FI23456': 0.45}]
        )

        # repeat with tz-aware index
        with patch("quantrocket.fundamental.download_brain_blmcf", new=mock_download_brain_blmcf):
            closes.index = closes.index.tz_localize("America/New_York")
            metrics = get_brain_blmcf_reindexed_like(
                closes, fields=["SENTIMENT"])

        metrics = metrics.loc["SENTIMENT"].reset_index()
        metrics["Date"] = metrics.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            metrics.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00-0400', 'FI12345': 0.5, 'FI23456': 0.4},
             {'Date': '2018-08-14T00:00:00-0400', 'FI12345': 0.5, 'FI23456': 0.4},
             {'Date': '2018-08-15T00:00:00-0400', 'FI12345': 0.55, 'FI23456': 0.4},
             {'Date': '2018-08-16T00:00:00-0400', 'FI12345': 0.55, 'FI23456': 0.45},
             {'Date': '2018-08-17T00:00:00-0400', 'FI12345': 0.55, 'FI23456': 0.45},
             {'Date': '2018-08-18T00:00:00-0400', 'FI12345': 0.55, 'FI23456': 0.45}]
        )

    def test_blmect(self):
        """
        Tests get_brain_blmect_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-08-13", periods=6, freq="D", name="Date"))

        def mock_download_brain_blmect(filepath_or_buffer, *args, **kwargs):
            metrics = pd.DataFrame(
                dict(
                    Date=[
                        "2018-05-15",
                        "2018-6-01",
                        "2018-08-15",
                        "2018-08-16",
                        ],
                    Sid=[
                         "FI12345",
                         "FI23456",
                         "FI12345",
                         "FI23456",
                         ],
                     MD_SENTIMENT=[
                         0.5,
                         0.4,
                         0.55,
                         0.45
                     ],
                    ))
            metrics.to_csv(filepath_or_buffer, index=False)
            filepath_or_buffer.seek(0)

        with patch("quantrocket.fundamental.download_brain_blmect", new=mock_download_brain_blmect):
            metrics = get_brain_blmect_reindexed_like(
                closes, fields=["MD_SENTIMENT"])

        metrics = metrics.loc["MD_SENTIMENT"].reset_index()
        metrics["Date"] = metrics.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            metrics.to_dict(orient="records"),
            [{'Date': '2018-08-13T00:00:00', 'FI12345': 0.5, 'FI23456': 0.4},
             {'Date': '2018-08-14T00:00:00', 'FI12345': 0.5, 'FI23456': 0.4},
             {'Date': '2018-08-15T00:00:00', 'FI12345': 0.55, 'FI23456': 0.4},
             {'Date': '2018-08-16T00:00:00', 'FI12345': 0.55, 'FI23456': 0.45},
             {'Date': '2018-08-17T00:00:00', 'FI12345': 0.55, 'FI23456': 0.45},
             {'Date': '2018-08-18T00:00:00', 'FI12345': 0.55, 'FI23456': 0.45}]
        )
