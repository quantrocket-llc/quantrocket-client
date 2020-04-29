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
from quantrocket.master import get_securities_reindexed_like, get_contract_nums_reindexed_like
from quantrocket.exceptions import ParameterError

class SecuritiesReindexedLikeTestCase(unittest.TestCase):

    @patch("quantrocket.master.download_master_file")
    def test_pass_sids_based_on_reindex_like(self, mock_download_master_file):
        """
        Tests that sids are correctly passed to the download_master_file
        function based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                          "FI23456"],
                     Symbol=["ABC",
                             "DEF"],
                     Etf=[1,
                          0],
                     Delisted=[0,
                               1],
                     Currency=["USD",
                               "USD"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        get_securities_reindexed_like(closes, fields=["Symbol", "Etf", "Delisted", "Currency"])

        download_master_file_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = download_master_file_call
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertListEqual(kwargs["fields"], ["Symbol", "Etf", "Delisted", "Currency"])

    @patch("quantrocket.master.download_master_file")
    def test_cast_boolean_fields(self, mock_download_master_file):
        """
        Tests that master fields Etf and Delisted are cast to boolean.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Symbol=["ABC","DEF"],
                                           Delisted=[0, 1],
                                           Etf=[1, 0],
                                           edi_Delisted=[0,1],
                                           ibkr_Etf=[1, 0]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

            securities = get_securities_reindexed_like(
                closes,
                fields=["Symbol", "Etf", "Delisted", "edi_Delisted", "ibkr_Etf"])

        symbols = securities.loc["Symbol"]
        symbols = symbols.reset_index()
        symbols.loc[:, "Date"] = symbols.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            symbols.to_dict(orient="records"),
            [{'Date': '2018-05-01T00:00:00', "FI12345": "ABC", "FI23456": "DEF"},
            {'Date': '2018-05-02T00:00:00', "FI12345": "ABC", "FI23456": "DEF"},
            {'Date': '2018-05-03T00:00:00', "FI12345": "ABC", "FI23456": "DEF"}]
        )

        for field in ("Delisted", "edi_Delisted"):
            delisted = securities.loc[field]
            delisted = delisted.reset_index()
            delisted.loc[:, "Date"] = delisted.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                delisted.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', "FI12345": False, "FI23456": True},
                {'Date': '2018-05-02T00:00:00', "FI12345": False, "FI23456": True},
                {'Date': '2018-05-03T00:00:00', "FI12345": False, "FI23456": True}]
            )

        for field in ("Etf", "ibkr_Etf"):
            etfs = securities.loc[field]
            etfs = etfs.reset_index()
            etfs.loc[:, "Date"] = etfs.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                etfs.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', "FI12345": True, "FI23456": False},
                {'Date': '2018-05-02T00:00:00', "FI12345": True, "FI23456": False},
                {'Date': '2018-05-03T00:00:00', "FI12345": True, "FI23456": False},
                ]
            )

    def test_securities_reindexed_like(self):
        """
        Tests get_securities_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456"],
                     Symbol=["ABC",
                             "DEF"],
                     Etf=[1,
                          0],
                     Delisted=[0,
                               1],
                     Currency=["USD",
                               "EUR"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            securities = get_securities_reindexed_like(
                closes,
                fields=["Symbol", "Etf", "Delisted", "Currency"])

            securities = securities.reset_index()
            securities.loc[:, "Date"] = securities.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                securities.to_dict(orient="records"),
                [{'Field': 'Currency',
                  'Date': '2018-05-01T00:00:00-0400',
                  "FI12345": 'USD',
                  "FI23456": 'EUR'},
                 {'Field': 'Currency',
                  'Date': '2018-05-02T00:00:00-0400',
                  "FI12345": 'USD',
                  "FI23456": 'EUR'},
                 {'Field': 'Currency',
                  'Date': '2018-05-03T00:00:00-0400',
                  "FI12345": 'USD',
                  "FI23456": 'EUR'},
                 {'Field': 'Delisted',
                  'Date': '2018-05-01T00:00:00-0400',
                  "FI12345": False,
                  "FI23456": True},
                 {'Field': 'Delisted',
                  'Date': '2018-05-02T00:00:00-0400',
                  "FI12345": False,
                  "FI23456": True},
                 {'Field': 'Delisted',
                  'Date': '2018-05-03T00:00:00-0400',
                  "FI12345": False,
                  "FI23456": True},
                 {'Field': 'Etf',
                  'Date': '2018-05-01T00:00:00-0400',
                  "FI12345": True,
                  "FI23456": False},
                 {'Field': 'Etf',
                  'Date': '2018-05-02T00:00:00-0400',
                  "FI12345": True,
                  "FI23456": False},
                 {'Field': 'Etf',
                  'Date': '2018-05-03T00:00:00-0400',
                  "FI12345": True,
                  "FI23456": False},
                 {'Field': 'Symbol',
                  'Date': '2018-05-01T00:00:00-0400',
                  "FI12345": 'ABC',
                  "FI23456": 'DEF'},
                 {'Field': 'Symbol',
                  'Date': '2018-05-02T00:00:00-0400',
                  "FI12345": 'ABC',
                  "FI23456": 'DEF'},
                 {'Field': 'Symbol',
                  'Date': '2018-05-03T00:00:00-0400',
                  "FI12345": 'ABC',
                  "FI23456": 'DEF'}]
            )

    def test_securities_reindexed_like_intraday(self):
        """
        Tests get_securities_reindexed_like with Date and Time in the index.
        """
        closes = pd.DataFrame(
            np.random.rand(4,2),
            columns=["FI12345","FI23456"],
            index=pd.MultiIndex.from_product([
                pd.date_range(start="2018-05-01",
                              periods=2,
                              freq="D",
                              tz="America/New_York",
                              name="Date"),
                ["09:30:00", "09:31:00"],
                ], names=["Date", "Time"]))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456"],
                     Symbol=["ABC",
                             "DEF"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            securities = get_securities_reindexed_like(
                closes,
                fields="Symbol")

            securities = securities.reset_index()
            securities.loc[:, "Date"] = securities.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

            self.assertListEqual(
                securities.to_dict(orient="records"),
                [{"FI12345": 'ABC',
                  "FI23456": 'DEF',
                  'Date': '2018-05-01T00:00:00-0400',
                  'Field': 'Symbol',
                  'Time': '09:30:00'},
                 {"FI12345": 'ABC',
                  "FI23456": 'DEF',
                  'Date': '2018-05-01T00:00:00-0400',
                  'Field': 'Symbol',
                  'Time': '09:31:00'},
                 {"FI12345": 'ABC',
                  "FI23456": 'DEF',
                  'Date': '2018-05-02T00:00:00-0400',
                  'Field': 'Symbol',
                  'Time': '09:30:00'},
                 {"FI12345": 'ABC',
                  "FI23456": 'DEF',
                  'Date': '2018-05-02T00:00:00-0400',
                  'Field': 'Symbol',
                  'Time': '09:31:00'}]
            )
class ContractNumsReindexedLikeTestCase(unittest.TestCase):

    @patch("quantrocket.master.download_master_file")
    def test_pass_sids_based_on_reindex_like(self, mock_download_master_file):
        """
        Tests that sids are correctly passed to the download_master_file
        function based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456"],
                     ibkr_UnderConId=[1,
                                 1],
                     SecType=["FUT",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-07-04"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        get_contract_nums_reindexed_like(closes)

        download_master_file_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = download_master_file_call
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertListEqual(kwargs["fields"], ["RolloverDate", "ibkr_UnderConId", "SecType"])

    def test_complain_if_no_fut(self):
        """
        Tests error handling when you pass a DataFrame with no FUTs.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                    dict(Sid=["FI12345",
                                "FI23456"],
                         ibkr_UnderConId=[1,
                                     1],
                         SecType=["STK",
                                  "STK"],
                         RolloverDate=[
                             None,
                             None
                         ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            with self.assertRaises(ParameterError) as cm:
                get_contract_nums_reindexed_like(closes)

        self.assertIn("input DataFrame does not appear to contain any futures contracts",
                      repr(cm.exception))

    def test_contract_nums_reindexed_like(self):
        """
        Tests get_contract_nums_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(4,6),
            columns=["FI12345","FI23456", "FI34567", "FI45678", "FI56789", "FI67890"],
            index=pd.date_range(start="2018-05-01",
                                periods=4,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456",
                            "FI34567",
                            "FI45678",
                            "FI56789",
                            "FI67890"],
                     ibkr_UnderConId=[1,
                                 2,
                                 1,
                                 2,
                                 None,
                                 2],
                     SecType=["FUT",
                              "FUT",
                              "FUT",
                              "FUT",
                              "STK",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-06-02",
                         "2018-06-03",
                         "2018-05-03",
                         None,
                         "2018-07-03"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            contract_nums = get_contract_nums_reindexed_like(closes)

        contract_nums.index = contract_nums.index.strftime("%Y-%m-%d")
        contract_nums = contract_nums.fillna("nan")
        self.assertDictEqual(
            contract_nums.to_dict(orient="index"),
            {'2018-05-01': {
                "FI12345": 1.0,
                "FI23456": 2.0,
                "FI34567": 2.0,
                "FI45678": 1.0,
                "FI56789": 'nan',
                "FI67890": 3.0},
             '2018-05-02': {
                 "FI12345": 1.0,
                 "FI23456": 2.0,
                 "FI34567": 2.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 3.0
                },
             '2018-05-03': {
                 "FI12345": 'nan',
                 "FI23456": 2.0,
                 "FI34567": 1.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 3.0},
             '2018-05-04': {
                 "FI12345": 'nan',
                 "FI23456": 1.0,
                 "FI34567": 1.0,
                 "FI45678": 'nan',
                 "FI56789": 'nan',
                 "FI67890": 2.0}}
        )

    def test_contract_nums_reindexed_like_tz_naive(self):
        """
        Tests get_contract_nums_reindexed_like with a tz-naive reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(4,6),
            columns=["FI12345","FI23456", "FI34567", "FI45678", "FI56789", "FI67890"],
            index=pd.date_range(start="2018-05-01",
                                periods=4,
                                freq="D",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456",
                            "FI34567",
                            "FI45678",
                            "FI56789",
                            "FI67890"],
                     ibkr_UnderConId=[1,
                                 2,
                                 1,
                                 2,
                                 None,
                                 2],
                     SecType=["FUT",
                              "FUT",
                              "FUT",
                              "FUT",
                              "STK",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-06-02",
                         "2018-06-03",
                         "2018-05-03",
                         None,
                         "2018-07-03"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            contract_nums = get_contract_nums_reindexed_like(closes)

        contract_nums.index = contract_nums.index.strftime("%Y-%m-%d")
        contract_nums = contract_nums.fillna("nan")
        self.assertDictEqual(
            contract_nums.to_dict(orient="index"),
            {'2018-05-01': {
                "FI12345": 1.0,
                "FI23456": 2.0,
                "FI34567": 2.0,
                "FI45678": 1.0,
                "FI56789": 'nan',
                "FI67890": 3.0},
             '2018-05-02': {
                 "FI12345": 1.0,
                 "FI23456": 2.0,
                 "FI34567": 2.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 3.0
                },
             '2018-05-03': {
                 "FI12345": 'nan',
                 "FI23456": 2.0,
                 "FI34567": 1.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 3.0},
             '2018-05-04': {
                 "FI12345": 'nan',
                 "FI23456": 1.0,
                 "FI34567": 1.0,
                 "FI45678": 'nan',
                 "FI56789": 'nan',
                 "FI67890": 2.0}}
        )

    def test_limit_sequence_num(self):
        """
        Tests get_contract_nums_reindexed_like with the limit option.
        """
        closes = pd.DataFrame(
            np.random.rand(4,6),
            columns=["FI12345","FI23456", "FI34567", "FI45678", "FI56789", "FI67890"],
            index=pd.date_range(start="2018-05-01",
                                periods=4,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456",
                            "FI34567",
                            "FI45678",
                            "FI56789",
                            "FI67890"],
                     ibkr_UnderConId=[1,
                                 2,
                                 1,
                                 2,
                                 None,
                                 2],
                     SecType=["FUT",
                              "FUT",
                              "FUT",
                              "FUT",
                              "STK",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-06-02",
                         "2018-06-03",
                         "2018-05-03",
                         None,
                         "2018-07-03"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            contract_nums = get_contract_nums_reindexed_like(closes, limit=2)

        contract_nums.index = contract_nums.index.strftime("%Y-%m-%d")
        contract_nums = contract_nums.fillna("nan")
        self.assertDictEqual(
            contract_nums.to_dict(orient="index"),
            {'2018-05-01': {
                "FI12345": 1.0,
                "FI23456": 2.0,
                "FI34567": 2.0,
                "FI45678": 1.0,
                "FI56789": 'nan',
                "FI67890": 'nan'},
             '2018-05-02': {
                 "FI12345": 1.0,
                 "FI23456": 2.0,
                 "FI34567": 2.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 'nan'
                },
             '2018-05-03': {
                 "FI12345": 'nan',
                 "FI23456": 2.0,
                 "FI34567": 1.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 'nan'},
             '2018-05-04': {
                 "FI12345": 'nan',
                 "FI23456": 1.0,
                 "FI34567": 1.0,
                 "FI45678": 'nan',
                 "FI56789": 'nan',
                 "FI67890": 2.0}}
        )

    def test_contract_nums_reindexed_like_intraday(self):
        """
        Tests get_contract_nums_reindexed_like when the input DataFrame includes
        Dates and Times.
        """
        closes = pd.DataFrame(
            np.random.rand(8,6),
            columns=["FI12345","FI23456", "FI34567", "FI45678", "FI56789", "FI67890"],
            index=pd.MultiIndex.from_product([
                pd.date_range(start="2018-05-01",
                              periods=4,
                              freq="D",
                              tz="America/New_York",
                              name="Date"),
                ["09:30:00","09:31:00"],
            ], names=["Date","Time"]))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456",
                            "FI34567",
                            "FI45678",
                            "FI56789",
                            "FI67890"],
                     ibkr_UnderConId=[1,
                                 2,
                                 1,
                                 2,
                                 None,
                                 2],
                     SecType=["FUT",
                              "FUT",
                              "FUT",
                              "FUT",
                              "STK",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-06-02",
                         "2018-06-03",
                         "2018-05-03",
                         None,
                         "2018-07-03"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            contract_nums = get_contract_nums_reindexed_like(closes)

        self.assertEqual(list(contract_nums.index.names), ["Date","Time"])

        contract_nums = contract_nums.reset_index().fillna("nan")
        contract_nums.loc[:, "Date"] = contract_nums.Date.dt.strftime("%Y-%m-%d")

        self.assertListEqual(
            contract_nums.to_dict(orient="records"),
            [{"FI12345": 1.0,
              "FI23456": 2.0,
              "FI34567": 2.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-01',
              'Time': '09:30:00'},
             {"FI12345": 1.0,
              "FI23456": 2.0,
              "FI34567": 2.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-01',
              'Time': '09:31:00'},
             {"FI12345": 1.0,
              "FI23456": 2.0,
              "FI34567": 2.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-02',
              'Time': '09:30:00'},
             {"FI12345": 1.0,
              "FI23456": 2.0,
              "FI34567": 2.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-02',
              'Time': '09:31:00'},
             {"FI12345": 'nan',
              "FI23456": 2.0,
              "FI34567": 1.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-03',
              'Time': '09:30:00'},
             {"FI12345": 'nan',
              "FI23456": 2.0,
              "FI34567": 1.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-03',
              'Time': '09:31:00'},
             {"FI12345": 'nan',
              "FI23456": 1.0,
              "FI34567": 1.0,
              "FI45678": 'nan',
              "FI56789": 'nan',
              "FI67890": 2.0,
              'Date': '2018-05-04',
              'Time': '09:30:00'},
             {"FI12345": 'nan',
              "FI23456": 1.0,
              "FI34567": 1.0,
              "FI45678": 'nan',
              "FI56789": 'nan',
              "FI67890": 2.0,
              'Date': '2018-05-04',
              'Time': '09:31:00'}]
        )
