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
import requests
from quantrocket import get_prices, get_prices_reindexed_like
from quantrocket.exceptions import ParameterError, MissingData, NoHistoricalData

class GetPricesTestCase(unittest.TestCase):
    """
    Test cases for `quantrocket.get_prices`.
    """

    def test_complain_if_invalid_timezone(self):
        """
        Tests error handling when the timezone is invalid.
        """

        with self.assertRaises(ParameterError) as cm:
            get_prices("my-db", timezone="Timbuktu")

        self.assertIn("invalid timezone: Timbuktu (see `pytz.all_timezones` for choices)", str(cm.exception))

    @patch("quantrocket.price.list_realtime_databases")
    @patch("quantrocket.price.list_history_databases")
    @patch("quantrocket.price.list_bundles")
    @patch("quantrocket.price.get_history_db_config")
    @patch("quantrocket.price.download_history_file")
    @patch("quantrocket.price.download_master_file")
    def test_pass_args_correctly_single_db(self,
                                            mock_download_master_file,
                                            mock_download_history_file,
                                            mock_get_history_db_config,
                                            mock_list_bundles,
                                            mock_list_history_databases,
                                            mock_list_realtime_databases):
        """
        Tests that args are correctly passed to download_history_file,
        download_master_file, and get_db_config, when there is a single db.
        """
        def _mock_get_history_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def _mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456"
                        ],
                    Date=[
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03",
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03"
                        ],
                    Close=[
                        20.10,
                        20.50,
                        19.40,
                        50.5,
                        52.5,
                        51.59,
                    ]))
            prices.to_csv(f, index=False)

        def _mock_list_history_databases():
            return [
                "usa-stk-1d",
                "demo-stk-1min"
            ]

        def _mock_list_realtime_databases():
            return {}

        def _mock_list_bundles():
            return {}

        mock_list_history_databases.side_effect = _mock_list_history_databases
        mock_list_realtime_databases.side_effect = _mock_list_realtime_databases
        mock_list_bundles.side_effect = _mock_list_bundles

        mock_download_history_file.side_effect = _mock_download_history_file
        mock_get_history_db_config.side_effect = _mock_get_history_db_config

        get_prices(
            "usa-stk-1d", start_date="2018-04-01", end_date="2018-04-03",
            universes="usa-stk", sids=["FI12345","FI23456"], fields=["Close"],
            exclude_universes=["usa-stk-pharm"],
            exclude_sids=[99999], cont_fut=False, timezone="America/New_York")

        mock_download_master_file.assert_not_called()

        self.assertEqual(len(mock_get_history_db_config.mock_calls), 1)
        db_config_call = mock_get_history_db_config.mock_calls[0]
        _, args, kwargs = db_config_call
        self.assertEqual(args[0], "usa-stk-1d")

        mock_list_history_databases.assert_called_once_with()
        mock_list_realtime_databases.assert_called_once_with()
        mock_list_bundles.assert_called_once_with()

        history_call = mock_download_history_file.mock_calls[0]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "usa-stk-1d")
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        self.assertListEqual(kwargs["fields"], ["Close"])
        self.assertEqual(kwargs["universes"], "usa-stk")
        self.assertListEqual(kwargs["exclude_sids"], [99999])
        self.assertListEqual(kwargs["exclude_universes"], ["usa-stk-pharm"])
        self.assertFalse(kwargs["cont_fut"])

    @patch("quantrocket.price.list_realtime_databases")
    @patch("quantrocket.price.list_history_databases")
    @patch("quantrocket.price.list_bundles")
    @patch("quantrocket.price.get_realtime_db_config")
    @patch("quantrocket.price.get_history_db_config")
    @patch("quantrocket.price.get_bundle_config")
    @patch("quantrocket.price.download_market_data_file")
    @patch("quantrocket.price.download_history_file")
    @patch("quantrocket.price.download_bundle_file")
    def test_pass_args_correctly_multi_db(
                                        self,
                                        mock_download_bundle_file,
                                        mock_download_history_file,
                                        mock_download_market_data_file,
                                        mock_get_bundle_config,
                                        mock_get_history_db_config,
                                        mock_get_realtime_db_config,
                                        mock_list_bundles,
                                        mock_list_history_databases,
                                        mock_list_realtime_databases):
        """
        Tests that args are correctly passed to download_history_file,
        download_master_file, and get_db_config, when there are multiple dbs,
        including 2 history dbs, 1 realtime db, and 1 zipline bundle.
        """
        def _mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["japan-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def _mock_get_realtime_db_config(db):
            return {
                "bar_size": "1 day",
                "fields": ["LastClose", "LastOpen", "VolumeClose"],
                "tick_db_code": "demo-stk-taq"
            }

        def _mock_get_bundle_config(db):
            return {
                "ingest_type": "usstock",
                "sids": None,
                "universes": None,
                "free": False,
                "data_frequency": "minute",
                "calendar": "XNYS",
                "start_date": "2007-01-03"
                }

        def _mock_download_history_file(code, f, *args, **kwargs):
            if code == "usa-stk-1d":
                prices = pd.DataFrame(
                    dict(
                        Sid=[
                            "FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456"
                            ],
                        Date=[
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03",
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03"
                            ],
                        Close=[
                            20.10,
                            20.50,
                            19.40,
                            50.5,
                            52.5,
                            51.59,
                        ]))
            else:
                prices = pd.DataFrame(
                    dict(
                        Sid=[
                            "FI56789",
                            "FI56789",
                            "FI56789",
                            ],
                        Date=[
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03",
                            ],
                        Close=[
                            5900,
                            5920,
                            5950
                        ]))
            prices.to_csv(f, index=False)

        def _mock_download_market_data_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456"
                        ],
                    Date=[
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03",
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03"
                        ],
                    LastClose=[
                        20.10,
                        20.50,
                        19.40,
                        50.5,
                        52.5,
                        51.59,
                    ]))
            prices.to_csv(f, index=False)

        def _mock_download_bundle_file(code, f, *args, **kwargs):
            raise NoHistoricalData("No minute data matches the query parameters")

        def _mock_list_history_databases():
            return [
                "usa-stk-1d",
                "demo-stk-1min",
                "japan-stk-1d"
            ]

        def _mock_list_realtime_databases():
            return {
                "demo-stk-taq": ["demo-stk-taq-1d"]
            }

        def _mock_list_bundles():
            return {
                "usstock-1min": True
            }

        mock_download_history_file.side_effect = _mock_download_history_file
        mock_download_bundle_file.side_effect = _mock_download_bundle_file
        mock_download_market_data_file.side_effect = _mock_download_market_data_file

        mock_get_history_db_config.side_effect = _mock_get_history_db_config
        mock_get_realtime_db_config.side_effect = _mock_get_realtime_db_config
        mock_get_bundle_config.side_effect = _mock_get_bundle_config

        mock_list_bundles.side_effect = _mock_list_bundles
        mock_list_history_databases.side_effect = _mock_list_history_databases
        mock_list_realtime_databases.side_effect = _mock_list_realtime_databases

        get_prices(
            ["usa-stk-1d", "japan-stk-1d", "demo-stk-taq-1d", "usstock-1min"],
            start_date="2018-04-01", end_date="2018-04-03",
            sids=["FI12345","FI23456","FI56789"], fields=["Close", "LastClose"],
            data_frequency="daily")

        mock_list_history_databases.assert_called_once_with()
        mock_list_realtime_databases.assert_called_once_with()
        mock_list_bundles.assert_called_once_with()

        self.assertEqual(len(mock_get_history_db_config.mock_calls), 2)
        db_config_call = mock_get_history_db_config.mock_calls[0]
        _, args, kwargs = db_config_call
        db_config_call_arg1 = args[0]
        db_config_call = mock_get_history_db_config.mock_calls[1]
        _, args, kwargs = db_config_call
        db_config_call_arg2 = args[0]
        self.assertSetEqual({db_config_call_arg1, db_config_call_arg2},
                            {"usa-stk-1d", "japan-stk-1d"})

        self.assertEqual(len(mock_download_history_file.mock_calls), 2)
        history_call = mock_download_history_file.mock_calls[0]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "usa-stk-1d")
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456","FI56789"])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        # only supported subset of fields is requested
        self.assertListEqual(kwargs["fields"], ["Close"])

        history_call = mock_download_history_file.mock_calls[1]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "japan-stk-1d")
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456","FI56789"])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        self.assertListEqual(kwargs["fields"], ["Close"])
        self.assertNotIn("data_frequency", kwargs)

        self.assertEqual(len(mock_get_realtime_db_config.mock_calls), 1)
        db_config_call = mock_get_realtime_db_config.mock_calls[0]
        _, args, kwargs = db_config_call
        self.assertEqual(args[0], "demo-stk-taq-1d")

        self.assertEqual(len(mock_download_market_data_file.mock_calls), 1)
        realtime_call = mock_download_market_data_file.mock_calls[0]
        _, args, kwargs = realtime_call
        self.assertEqual(args[0], "demo-stk-taq-1d")
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456","FI56789"])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        # only supported subset of fields is requested
        self.assertListEqual(kwargs["fields"], ["LastClose"])
        self.assertNotIn("data_frequency", kwargs)

        # since we passed data_frequency, we didn't need to query the bundle config
        self.assertEqual(len(mock_get_bundle_config.mock_calls), 0)

        self.assertEqual(len(mock_download_bundle_file.mock_calls), 1)
        zipline_call = mock_download_bundle_file.mock_calls[0]
        _, args, kwargs = zipline_call
        self.assertEqual(args[0], "usstock-1min")
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456","FI56789"])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        # only supported subset of fields is requested
        self.assertListEqual(kwargs["fields"], ["Close"])
        # data_frequency arg is passed
        self.assertEqual(kwargs["data_frequency"], "daily")

    def test_query_eod_history_db(self):
        """
        Tests maniuplation of a single EOD db.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03",
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03"
                        ],
                    Close=[
                        20.10,
                        20.50,
                        19.40,
                        50.5,
                        52.5,
                        51.59,
                        ],
                    Volume=[
                        15000,
                        7800,
                        12400,
                        98000,
                        179000,
                        142500
                    ]
                ))
            prices.to_csv(f, index=False)

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            prices = get_prices("usa-stk-1d", fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        self.assertListEqual(
            list(prices.index.get_level_values("Field")),
            ["Close", "Close", "Close", "Volume", "Volume", "Volume"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.5},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 52.5},
             {'Date': '2018-04-03T00:00:00', "FI12345": 19.4, "FI23456": 51.59}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 15000, "FI23456": 98000},
             {'Date': '2018-04-02T00:00:00', "FI12345": 7800, "FI23456": 179000},
             {'Date': '2018-04-03T00:00:00', "FI12345": 12400, "FI23456": 142500}]
        )

    def test_query_multiple_eod_history_dbs(self):
        """
        Tests maniuplation of multiple EOD dbs.
        """
        def mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["japan-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_download_history_file(code, f, *args, **kwargs):
            if code == "usa-stk-1d":
                prices = pd.DataFrame(
                    dict(
                        Sid=[
                            "FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            ],
                        Date=[
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03",
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03"
                            ],
                        Close=[
                            20.10,
                            20.50,
                            19.40,
                            50.5,
                            52.5,
                            51.59,
                            ],
                        Volume=[
                            15000,
                            7800,
                            12400,
                            98000,
                            179000,
                            142500
                        ]
                    ))
            else:
                prices = pd.DataFrame(
                    dict(
                        Sid=[
                            "FI56789",
                            "FI56789",
                            "FI56789",
                            ],
                        Date=[
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03",
                            ],
                        Close=[
                            5900,
                            5920,
                            5950],
                        Volume=[
                            18000,
                            17600,
                            5600
                        ]
                    ))
            prices.to_csv(f, index=False)

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            prices = get_prices(["usa-stk-1d", "japan-stk-1d"], fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456","FI56789"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.5, "FI56789": 5900.0},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 52.5, "FI56789": 5920.0},
             {'Date': '2018-04-03T00:00:00', "FI12345": 19.4, "FI23456": 51.59, "FI56789": 5950.0}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 15000, "FI23456": 98000, "FI56789": 18000},
             {'Date': '2018-04-02T00:00:00', "FI12345": 7800, "FI23456": 179000, "FI56789": 17600},
             {'Date': '2018-04-03T00:00:00', "FI12345": 12400, "FI23456": 142500, "FI56789": 5600}]
        )

    def test_query_eod_history_and_realtime_db(self):
        """
        Tests querying of an EOD history db and EOD realtime aggregate db. This test
        is to make sure we can handle combining a history db with dates like "2020-04-05"
        and a 1-d realtime aggregate db with dates like "2020-04-05T00:00:00+00". It also
        tests that dates are made uniform across all fields.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):

            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03",
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03"
                        ],
                    Close=[
                        20.10,
                        20.50,
                        19.40,
                        50.5,
                        52.5,
                        51.59,
                        ],
                    Volume=[
                        15000,
                        7800,
                        12400,
                        98000,
                        179000,
                        142500
                    ]
                )
            )
            prices.to_csv(f, index=False)

        def mock_download_market_data_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        "2018-04-01T00:00:00+00",
                        "2018-04-02T00:00:00+00",
                        "2018-04-01T00:00:00+00",
                        "2018-04-02T00:00:00+00",
                        ],
                    LastClose=[
                        30.50,
                        39.40,
                        79.5,
                        79.59,
                        ],
                    LastCount=[
                        305,
                        940,
                        795,
                        959,
                        ]
                ))
            prices.to_csv(f, index=False)

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"usa-stk-tick": ["usa-stk-tick-1d"]}

        def mock_get_realtime_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["japan-stk"],
                "vendor": "ibkr",
                "fields": ["LastClose","LastOpen","LastHigh","LastLow", "VolumeClose"]
            }

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.get_realtime_db_config', new=mock_get_realtime_db_config):
                            with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                                with patch('quantrocket.price.download_market_data_file', new=mock_download_market_data_file):

                                    prices = get_prices(["usa-stk-1d", "usa-stk-tick-1d"], fields=["Close", "LastClose"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.5},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 52.5},
             {'Date': '2018-04-03T00:00:00', "FI12345": 19.4, "FI23456": 51.59}]
        )
        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 30.50, "FI23456": 79.50},
             {'Date': '2018-04-02T00:00:00', "FI12345": 39.40, "FI23456": 79.59},
             {'Date': '2018-04-03T00:00:00', "FI12345": "nan", "FI23456": "nan"}]
        )

    def test_consolidate_intraday_history_and_realtime_distinct_fields(self):
        """
        Tests that when querying a history and real-time database with
        distinct fields and overlapping dates/sids, both fields are
        preserved.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "15 mins",
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_get_realtime_db_config(db):
            return {
                "bar_size": "15 min",
                "fields": ["LastClose","LastOpen","LastHigh","LastLow", "VolumeClose"]
            }
        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T15:30:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T15:30:00-04:00",
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T15:30:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T15:30:00-04:00",
                        ],
                    Close=[
                        20.10,
                        20.50,
                        19.40,
                        18.56,
                        50.5,
                        52.5,
                        51.59,
                        54.23
                        ],
                    Volume=[
                        15000,
                        7800,
                        12400,
                        14500,
                        98000,
                        179000,
                        142500,
                        124000,
                    ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_market_data_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        # Data is UTC but will become NY
                        "2018-04-01T19:30:00+00",
                        "2018-04-02T19:30:00+00",
                        "2018-04-01T19:30:00+00",
                        "2018-04-02T19:30:00+00",
                        ],
                    LastClose=[
                        30.50,
                        39.40,
                        79.5,
                        79.59,
                        ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-15min",
                "japan-stk-1d",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"usa-stk-snapshot": ["usa-stk-snapshot-15min"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):

            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.get_realtime_db_config', new=mock_get_realtime_db_config):
                            with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                                with patch('quantrocket.price.download_market_data_file', new=mock_download_market_data_file):
                                    with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                                        prices = get_prices(
                                            ["usa-stk-15min", "usa-stk-snapshot-15min"],
                                            fields=["Close", "LastClose", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": 20.1, "FI23456": 50.5},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 20.5, "FI23456": 52.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": 19.4, "FI23456": 51.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 18.56, "FI23456": 54.23}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": 15000.0, "FI23456": 98000.0},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 7800.0, "FI23456": 179000.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": 12400.0, "FI23456": 142500.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 14500.0, "FI23456": 124000.0}]
        )

        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            # Data was UTC but now NY
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": "nan", "FI23456": "nan"},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 30.5, "FI23456": 79.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": "nan", "FI23456": "nan"},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 39.4, "FI23456": 79.59}]
        )

    def test_query_single_realtime_db(self):
        """
        Tests querying a single real-time aggregate database, with no history
        db in the query.
        """
        def mock_get_realtime_db_config(db):
            return {
                "bar_size": "15 min",
                "fields": ["LastClose","LastOpen","LastHigh","LastLow", "VolumeClose"]
            }

        def mock_download_market_data_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        # Data is UTC but will become Mexico_City
                        "2018-04-01T19:30:00+00",
                        "2018-04-02T19:30:00+00",
                        "2018-04-01T19:30:00+00",
                        "2018-04-02T19:30:00+00",
                        ],
                    LastClose=[
                        30.50,
                        39.40,
                        79.5,
                        79.59,
                        ],
                    LastCount=[
                        305,
                        940,
                        795,
                        959,
                        ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/Mexico_City", "America/Mexico_City"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-15min",
                "japan-stk-1d",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"mexi-stk-tick": ["mexi-stk-tick-15min"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_realtime_db_config', new=mock_get_realtime_db_config):
                        with patch('quantrocket.price.download_market_data_file', new=mock_download_market_data_file):
                            with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                                prices = get_prices("mexi-stk-tick-15min")

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])

        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            # Data was UTC but now Mexico_City
            [{'Date': '2018-04-01T00:00:00', 'Time': '14:30:00', "FI12345": 30.5, "FI23456": 79.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '14:30:00', "FI12345": 39.4, "FI23456": 79.59}]
        )

        last_counts = prices.loc["LastCount"]
        last_counts = last_counts.reset_index()
        last_counts.loc[:, "Date"] = last_counts.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_counts.fillna("nan").to_dict(orient="records"),
            # Data was UTC but now Mexico_City
            [{'Date': '2018-04-01T00:00:00', 'Time': '14:30:00', "FI12345": 305.0, "FI23456": 795.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '14:30:00', "FI12345": 940.0, "FI23456": 959.0}]
        )

    def test_query_single_zipline_bundle(self):
        """
        Tests querying a single Zipline bundle, with no history
        db or realtime db in the query.
        """
        def mock_download_bundle_file(code, f, *args, **kwargs):
            prices = pd.concat(
                dict(
                    Close=pd.DataFrame(
                        dict(
                            FI12345=[
                                48.90,
                                49.40,
                                55.49,
                                56.78
                            ],
                            FI23456=[
                                59.5,
                                59.59,
                                59.34,
                                51.56,
                            ]
                        ),
                        index=[
                            "2018-04-01T09:30:00-04:00",
                            "2018-04-01T15:30:00-04:00",
                            "2018-04-02T09:30:00-04:00",
                            "2018-04-02T15:30:00-04:00",
                            ]),
                    Volume=pd.DataFrame(
                        dict(
                            FI12345=[
                                100,
                                200,
                                300,
                                400
                            ],
                            FI23456=[
                                500,
                                600,
                                700,
                                800,
                            ]
                        ),
                        index=[
                            "2018-04-01T09:30:00-04:00",
                            "2018-04-01T15:30:00-04:00",
                            "2018-04-02T09:30:00-04:00",
                            "2018-04-02T15:30:00-04:00",
                            ])
                    ), names=["Field","Date"]
            )
            prices.to_csv(f)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-15min",
                "japan-stk-1d",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"mexi-stk-tick": ["mexi-stk-tick-15min"]}

        def mock_get_bundle_config(db):
            return {
                "ingest_type": "usstock",
                "sids": None,
                "universes": None,
                "free": False,
                "data_frequency": "minute",
                "calendar": "XNYS",
                "start_date": "2007-01-03"
                }

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.download_bundle_file', new=mock_download_bundle_file):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):
                            with patch("quantrocket.price.get_bundle_config", new=mock_get_bundle_config):
                                prices = get_prices("usstock-1min", fields=["Close","Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])

        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.fillna("nan").to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 'FI12345': 48.9, 'FI23456': 59.5},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 'FI12345': 49.4, 'FI23456': 59.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 'FI12345': 55.49, 'FI23456': 59.34},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 'FI12345': 56.78, 'FI23456': 51.56}]
        )

        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.fillna("nan").to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 'FI12345': 100, 'FI23456': 500},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 'FI12345': 200, 'FI23456': 600},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 'FI12345': 300, 'FI23456': 700},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 'FI12345': 400, 'FI23456': 800}]
        )

    @patch("quantrocket.price.download_bundle_file")
    def test_pass_data_frequency_based_on_bundle_config(self, mock_download_bundle_file):
        """
        Tests that when querying a Zipline bundle and not providing a data_frequency
        arg, the data frequency is determined from the bundle config.
        """
        def _mock_download_bundle_file(code, f, *args, **kwargs):
            prices = pd.concat(
                dict(
                    Close=pd.DataFrame(
                        dict(
                            FI12345=[
                                48.90,
                                49.40,
                                55.49,
                                56.78
                            ],
                            FI23456=[
                                59.5,
                                59.59,
                                59.34,
                                51.56,
                            ]
                        ),
                        index=[
                            "2018-04-01T09:30:00-04:00",
                            "2018-04-01T15:30:00-04:00",
                            "2018-04-02T09:30:00-04:00",
                            "2018-04-02T15:30:00-04:00",
                            ]),
                    Volume=pd.DataFrame(
                        dict(
                            FI12345=[
                                100,
                                200,
                                300,
                                400
                            ],
                            FI23456=[
                                500,
                                600,
                                700,
                                800,
                            ]
                        ),
                        index=[
                            "2018-04-01T09:30:00-04:00",
                            "2018-04-01T15:30:00-04:00",
                            "2018-04-02T09:30:00-04:00",
                            "2018-04-02T15:30:00-04:00",
                            ])
                    ), names=["Field","Date"]
            )
            prices.to_csv(f)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-15min",
                "japan-stk-1d",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"mexi-stk-tick": ["mexi-stk-tick-15min"]}

        def mock_get_bundle_config(db):
            return {
                "ingest_type": "usstock",
                "sids": None,
                "universes": None,
                "free": False,
                "data_frequency": "minute",
                "calendar": "XNYS",
                "start_date": "2007-01-03"
                }

        def mock_list_bundles():
            return {"usstock-1min": True}

        mock_download_bundle_file.side_effect = _mock_download_bundle_file

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch("quantrocket.price.download_master_file", new=mock_download_master_file):
                        with patch("quantrocket.price.get_bundle_config", new=mock_get_bundle_config):

                            # query with no data_frequency arg
                            get_prices("usstock-1min")

                            self.assertEqual(len(mock_download_bundle_file.mock_calls), 1)
                            zipline_call = mock_download_bundle_file.mock_calls[0]
                            _, args, kwargs = zipline_call
                            self.assertEqual(args[0], "usstock-1min")
                            self.assertEqual(kwargs["data_frequency"], "minute")

                            # query with data_frequency arg
                            get_prices("usstock-1min", data_frequency='daily')

                            self.assertEqual(len(mock_download_bundle_file.mock_calls), 2)
                            zipline_call = mock_download_bundle_file.mock_calls[1]
                            _, args, kwargs = zipline_call
                            self.assertEqual(args[0], "usstock-1min")
                            self.assertEqual(kwargs["data_frequency"], "daily")

    @patch("quantrocket.price.download_market_data_file")
    @patch("quantrocket.price.download_history_file")
    @patch("quantrocket.price.download_bundle_file")
    def test_apply_times_filter_to_history_vs_realtime_database_vs_zipline_bundle(
        self,
        mock_download_bundle_file,
        mock_download_history_file,
        mock_download_market_data_file):

        """
        Tests that the times filter is applied to a history database via the
        history query but is applied to the realtime database after
        converting to the exchange timezone.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "1 mins",
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Wap","Open","High","Low", "Volume"]
            }

        def mock_get_realtime_db_config(db):
            return {
                "bar_size": "1 min",
                "fields": ["LastClose","LastOpen","LastHigh","LastLow", "VolumeClose"]
            }

        def mock_get_bundle_config(db):
            return {
                "ingest_type": "usstock",
                "sids": None,
                "universes": None,
                "free": False,
                "data_frequency": "minute",
                "calendar": "XNYS",
                "start_date": "2007-01-03"
                }

        def _mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T15:30:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T15:30:00-04:00",
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T15:30:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T15:30:00-04:00",
                        ],
                    Wap=[
                        20.10,
                        20.50,
                        19.40,
                        18.56,
                        50.5,
                        52.5,
                        51.59,
                        54.23
                        ],
                    Volume=[
                        15000,
                        7800,
                        12400,
                        14500,
                        98000,
                        179000,
                        142500,
                        124000,
                    ]
                ))
            prices.to_csv(f, index=False)

        def _mock_download_market_data_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        # 19:00:00 UTC data is returned (15:00:00 NY) but will be filtered out
                        "2018-04-01T19:00:00+00",
                        "2018-04-01T19:30:00+00",
                        "2018-04-02T19:00:00+00",
                        "2018-04-02T19:30:00+00",
                        "2018-04-01T19:00:00+00",
                        "2018-04-01T19:30:00+00",
                        "2018-04-02T19:00:00+00",
                        "2018-04-02T19:30:00+00",
                        ],
                    LastClose=[
                        30.50,
                        39.40,
                        45.49,
                        46.78,
                        79.5,
                        79.59,
                        89.34,
                        81.56,
                        ]
                ))
            prices.to_csv(f, index=False)

        def _mock_download_bundle_file(code, f, *args, **kwargs):
            prices = pd.concat(
                dict(
                    Close=pd.DataFrame(
                        dict(
                            FI12345=[
                                48.90,
                                49.40,
                                55.49,
                                56.78
                            ],
                            FI23456=[
                                59.5,
                                59.59,
                                59.34,
                                51.56,
                            ]
                        ),
                        index=[
                            "2018-04-01T09:30:00-04:00",
                            "2018-04-01T15:30:00-04:00",
                            "2018-04-02T09:30:00-04:00",
                            "2018-04-02T15:30:00-04:00",
                            ])
                    ), names=["Field","Date"]
            )
            prices.to_csv(f)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-1min",
                "japan-stk-1d",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"usa-stk-snapshot": ["usa-stk-snapshot-1min"]}

        def mock_list_bundles():
            return {"usstock-1min": True, "usstock-free-1min": True}

        mock_download_history_file.side_effect = _mock_download_history_file
        mock_download_market_data_file.side_effect = _mock_download_market_data_file
        mock_download_bundle_file.side_effect = _mock_download_bundle_file

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.get_realtime_db_config', new=mock_get_realtime_db_config):
                            with patch("quantrocket.price.download_master_file", new=mock_download_master_file):
                                with patch("quantrocket.price.get_bundle_config", new=mock_get_bundle_config):
                                    prices = get_prices(
                                        ["usa-stk-1min", "usa-stk-snapshot-1min", "usstock-1min"],
                                        fields=["Close", "LastClose", "Volume", "Wap"],
                                        times=["09:30:00", "15:30:00"],
                                    )

        self.assertEqual(len(mock_download_history_file.mock_calls), 1)
        history_call = mock_download_history_file.mock_calls[0]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "usa-stk-1min")
        # only supported subset of fields is requested
        self.assertSetEqual(set(kwargs["fields"]), {"Wap", "Volume"})
        # times filter was passed
        self.assertListEqual(kwargs["times"], ["09:30:00", "15:30:00"])

        self.assertEqual(len(mock_download_market_data_file.mock_calls), 1)
        realtime_call = mock_download_market_data_file.mock_calls[0]
        _, args, kwargs = realtime_call
        self.assertEqual(args[0], "usa-stk-snapshot-1min")
        # only supported subset of fields is requested
        self.assertListEqual(kwargs["fields"], ["LastClose"])
        # times filter not passed
        self.assertNotIn("times", list(kwargs.keys()))

        self.assertEqual(len(mock_download_bundle_file.mock_calls), 1)
        minute_call = mock_download_bundle_file.mock_calls[0]
        _, args, kwargs = minute_call
        self.assertEqual(args[0], "usstock-1min")
        # only supported subset of fields is requested
        self.assertSetEqual(set(kwargs["fields"]), {"Close", "Volume"})
        # times filter was passed
        self.assertListEqual(kwargs["times"], ["09:30:00", "15:30:00"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        waps = prices.loc["Wap"]
        waps = waps.reset_index()
        waps.loc[:, "Date"] = waps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            waps.fillna("nan").to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": 20.1, "FI23456": 50.5},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 20.5, "FI23456": 52.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": 19.4, "FI23456": 51.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 18.56, "FI23456": 54.23}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": 15000.0, "FI23456": 98000.0},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 7800.0, "FI23456": 179000.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": 12400.0, "FI23456": 142500.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 14500.0, "FI23456": 124000.0}]
        )

        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            # Data was UTC but now NY
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": 'nan', "FI23456": 'nan'},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 39.4, "FI23456": 79.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": 'nan', "FI23456": 'nan'},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 46.78, "FI23456": 81.56}]
        )

        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.fillna("nan").to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 'FI12345': 48.9, 'FI23456': 59.5},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 'FI12345': 49.4, 'FI23456': 59.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 'FI12345': 55.49, 'FI23456': 59.34},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 'FI12345': 56.78, 'FI23456': 51.56}]
        )

    def test_consolidate_overlapping_fields_and_respect_priority(self):
        """
        Tests that when querying two databases with overlapping
        dates/sids/fields, the value is taken from the db which was passed
        first as an argument.
        """
        def mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["nyse-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_download_history_file(code, f, *args, **kwargs):
            if code == "usa-stk-1d":
                prices = pd.DataFrame(
                    dict(
                        Sid=[
                            "FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            ],
                        Date=[
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03",
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03"
                            ],
                        Close=[
                            20.10,
                            20.50,
                            19.40,
                            50.5,
                            52.5,
                            51.59,
                            ],
                        Volume=[
                            15000,
                            7800,
                            12400,
                            98000,
                            179000,
                            142500
                        ]
                    ))
            else:
                prices = pd.DataFrame(
                    dict(
                        Sid=[
                            "FI12345",
                            "FI12345",
                            "FI12345",
                            ],
                        Date=[
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03",
                            ],
                        Close=[
                            5900,
                            5920,
                            5950],
                    ))
            prices.to_csv(f, index=False)

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "nyse-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            # prioritize usa-stk-1d by passing first
                            prices = get_prices(["usa-stk-1d", "nyse-stk-1d"],
                                                fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.5},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 52.5},
             {'Date': '2018-04-03T00:00:00', "FI12345": 19.4, "FI23456": 51.59}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 15000, "FI23456": 98000},
             {'Date': '2018-04-02T00:00:00', "FI12345": 7800, "FI23456": 179000},
             {'Date': '2018-04-03T00:00:00', "FI12345": 12400, "FI23456": 142500}]
        )

        # repeat test but prioritize nyse-stk-1d by passing first
        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            prices = get_prices(["nyse-stk-1d", "usa-stk-1d"],
                                                fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 5900.0, "FI23456": 50.5},
             {'Date': '2018-04-02T00:00:00', "FI12345": 5920.0, "FI23456": 52.5},
             {'Date': '2018-04-03T00:00:00', "FI12345": 5950.0, "FI23456": 51.59}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        # Since volume is null in nyse-stk-1d, we get the volume from usa-stk-1d
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 15000, "FI23456": 98000},
             {'Date': '2018-04-02T00:00:00', "FI12345": 7800, "FI23456": 179000},
             {'Date': '2018-04-03T00:00:00', "FI12345": 12400, "FI23456": 142500}]
        )

    def test_parse_bar_sizes(self):
        """
        Tests that when querying a history and real-time database which have
        different bar size strings which are nevertheless equivalent, this is
        allowed.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "15 mins", # 15 mins plural (IB format)
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_get_realtime_db_config(db):
            return {
                "bar_size": "15 min", # 15 min Pandas format
                "fields": ["LastClose","LastOpen","LastHigh","LastLow", "VolumeClose"]
            }
        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T15:30:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T15:30:00-04:00",
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T15:30:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T15:30:00-04:00",
                        ],
                    Close=[
                        20.10,
                        20.50,
                        19.40,
                        18.56,
                        50.5,
                        52.5,
                        51.59,
                        54.23
                        ],
                    Volume=[
                        15000,
                        7800,
                        12400,
                        14500,
                        98000,
                        179000,
                        142500,
                        124000,
                    ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_market_data_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        ],
                    Date=[
                        "2018-04-01T19:30:00+00",
                        "2018-04-02T19:30:00+00",
                        "2018-04-01T19:30:00+00",
                        "2018-04-02T19:30:00+00",
                        ],
                    LastClose=[
                        30.50,
                        39.40,
                        79.5,
                        79.59,
                        ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-15min",
                "japan-stk-1d",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"usa-stk-snapshot": ["usa-stk-snapshot-15min"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.get_realtime_db_config', new=mock_get_realtime_db_config):
                            with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                                with patch('quantrocket.price.download_market_data_file', new=mock_download_market_data_file):
                                    with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                                        prices = get_prices(
                                            ["usa-stk-15min", "usa-stk-snapshot-15min"],
                                            fields=["Close", "LastClose", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": 20.1, "FI23456": 50.5},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 20.5, "FI23456": 52.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": 19.4, "FI23456": 51.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 18.56, "FI23456": 54.23}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": 15000.0, "FI23456": 98000.0},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 7800.0, "FI23456": 179000.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": 12400.0, "FI23456": 142500.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 14500.0, "FI23456": 124000.0}]
        )

        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', "FI12345": "nan", "FI23456": "nan"},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', "FI12345": 30.5, "FI23456": 79.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', "FI12345": "nan", "FI23456": "nan"},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', "FI12345": 39.4, "FI23456": 79.59}]
        )

    def test_no_complain_no_data_multiple_dbs(self):
        """
        Tests that if multiple dbs are queried and one lacks data but the
        other has data, no error is raised.
        """
        def mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["japan-stk"],
                    "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_download_history_file(code, f, *args, **kwargs):
            if code == "usa-stk-1d":
                prices = pd.DataFrame(
                    dict(
                        Sid=[
                            "FI12345",
                            "FI12345",
                            "FI12345",
                            "FI23456",
                            "FI23456",
                            "FI23456",
                            ],
                        Date=[
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03",
                            "2018-04-01",
                            "2018-04-02",
                            "2018-04-03"
                            ],
                        Close=[
                            20.10,
                            20.50,
                            19.40,
                            50.5,
                            52.5,
                            51.59,
                            ],
                        Volume=[
                            15000,
                            7800,
                            12400,
                            98000,
                            179000,
                            142500
                        ]
                    ))
                prices.to_csv(f, index=False)
            else:
                raise NoHistoricalData("no history matches the query parameters")

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            prices = get_prices(["usa-stk-1d", "japan-stk-1d"], fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.5},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 52.5},
             {'Date': '2018-04-03T00:00:00', "FI12345": 19.4, "FI23456": 51.59}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 15000, "FI23456": 98000},
             {'Date': '2018-04-02T00:00:00', "FI12345": 7800, "FI23456": 179000},
             {'Date': '2018-04-03T00:00:00', "FI12345": 12400, "FI23456": 142500}]
        )

    def test_complain_no_data_multiple_dbs(self):
        """
        Tests that if multiple dbs are queried and all of them lack data, an
        error is raised.
        """
        def mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["japan-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_download_history_file(code, f, *args, **kwargs):
            raise NoHistoricalData("no history matches the query parameters")

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            with self.assertRaises(NoHistoricalData) as cm:
                                get_prices(["usa-stk-1d", "japan-stk-1d"])

        self.assertIn(
            "no price data matches the query parameters in any of usa-stk-1d, japan-stk-1d",
            str(cm.exception))

    def test_complain_no_data_single_db(self):
        """
        Tests that if a single db is queried and it lacks data, an
        error is raised.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            raise NoHistoricalData("this error will be passed through as is")


        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            with self.assertRaises(NoHistoricalData) as cm:
                                get_prices("usa-stk-1d")

        self.assertIn(
            "this error will be passed through as is",
            str(cm.exception))

    def test_complain_if_tick_db(self):
        """
        Tests error handling when multiple dbs are queried and one is a tick
        db rather than a history or real-time agg db.
        """
        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min",
                "japan-stk-15min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"],
                    "etf-taq": ["etf-taq-1h"],
                    }

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):

                        with self.assertRaises(ParameterError) as cm:
                            get_prices(["usa-stk-1d", "etf-taq", "japan-stk-15min"])

        self.assertIn((
            "etf-taq is a real-time tick database, only history databases or "
            "real-time aggregate databases are supported"
            ), str(cm.exception))

    def test_complain_if_unknown_db(self):
        """
        Tests error handling when an unknown db is queried.
        """
        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min",
                "japan-stk-15min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"],
                    "etf-taq": ["etf-taq-1h"],
                    }

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):

                        with self.assertRaises(ParameterError) as cm:
                            get_prices(["asx-stk-1d"])

        self.assertIn((
           "no history or real-time aggregate databases or Zipline bundles called asx-stk-1d"
            ), str(cm.exception))

    def test_complain_if_multiple_bar_sizes(self):
        """
        Tests error handling when multiple dbs are queried and they have
        different bar sizes.
        """
        def mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "30 mins",
                    "universes": ["japan-stk"],
                    "vendor": "ibkr",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min",
                "japan-stk-15min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        def mock_get_bundle_config(db):
            return {
                "ingest_type": "usstock",
                "sids": None,
                "universes": None,
                "free": False,
                "data_frequency": "minute",
                "calendar": "XNYS",
                "start_date": "2007-01-03"
                }

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
                        with patch('quantrocket.price.get_bundle_config', new=mock_get_bundle_config):

                            # two history dbs
                            with self.assertRaises(ParameterError) as cm:
                                get_prices(["usa-stk-1d", "japan-stk-15min"])

                                self.assertIn((
                                    "all databases must contain same bar size but usa-stk-1d, japan-stk-15min have different "
                                    "bar sizes:"
                                    ), str(cm.exception))

                            # history db and zipline bundle
                            with self.assertRaises(ParameterError) as cm:
                                get_prices(["usa-stk-1d", "usstock-1min"])

                                self.assertIn((
                                    "all databases must contain same bar size but usa-stk-1d, usstock-1min have different "
                                    "bar sizes:"
                                    ), str(cm.exception))

    def test_warn_if_no_history_service(self):
        """
        Tests that a warning is triggered if the history service is not
        available.
        """
        def mock_list_history_databases():
            response = requests.models.Response()
            response.status_code = 502
            raise requests.HTTPError(response=response)

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"],
                    "etf-taq": ["etf-taq-1h"],
                    }

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):

                        with self.assertWarns(RuntimeWarning) as warning_cm:
                            with self.assertRaises(ParameterError) as cm:
                                get_prices(["asx-stk-1d"])

        self.assertIn(
            "Error while checking if asx-stk-1d is a history database, will assume it's not",
            str(warning_cm.warning))
        self.assertIn((
           "no history or real-time aggregate databases or Zipline bundles called asx-stk-1d"
            ), str(cm.exception))

    def test_warn_if_no_realtime_service(self):
        """
        Tests that a warning is triggered if the realtime service is not
        available.
        """

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min",
                "japan-stk-15min"
            ]

        def mock_list_realtime_databases():
            response = requests.models.Response()
            response.status_code = 502
            raise requests.HTTPError(response=response)

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):

                        with self.assertWarns(RuntimeWarning) as warning_cm:
                            with self.assertRaises(ParameterError) as cm:
                                get_prices(["asx-stk-1d"])

        self.assertIn(
            "Error while checking if asx-stk-1d is a realtime database, will assume it's not",
            str(warning_cm.warning))
        self.assertIn((
           "no history or real-time aggregate databases or Zipline bundles called asx-stk-1d"
            ), str(cm.exception))

    def test_warn_if_no_zipline_service(self):
        """
        Tests that a warning is triggered if the zipline service is not
        available.
        """
        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min",
                "japan-stk-15min"
            ]
        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"],
                    "etf-taq": ["etf-taq-1h"],
                    }

        def mock_list_bundles():
            response = requests.models.Response()
            response.status_code = 502
            raise requests.HTTPError(response=response)

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):

                        with self.assertWarns(RuntimeWarning) as warning_cm:
                            with self.assertRaises(ParameterError) as cm:
                                get_prices(["asx-stk-1d"])

        self.assertIn(
            "Error while checking if asx-stk-1d is a Zipline bundle, will assume it's not",
            str(warning_cm.warning))
        self.assertIn((
           "no history or real-time aggregate databases or Zipline bundles called asx-stk-1d"
            ), str(cm.exception))

    def test_intraday_pass_timezone(self):
        """
        Tests handling of an intraday db when a timezone is specified.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "30 mins",
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        "FI23456"
                        ],
                    Date=[
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T10:00:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T10:00:00-04:00",
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T10:00:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T10:00:00-04:00",
                        ],
                    Close=[
                        20.10,
                        20.25,
                        20.50,
                        20.38,
                        50.15,
                        50.59,
                        51.59,
                        51.67
                        ],
                    Volume=[
                        1500,
                        7800,
                        1400,
                        800,
                        9000,
                        7100,
                        1400,
                        1500
                    ]
                ))
            prices.to_csv(f, index=False)
        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            prices = get_prices(
                                "usa-stk-15min",
                                times=["09:30:00", "10:00:00"],
                                fields=["Close", "Volume"],
                                timezone="America/Chicago"
                            )

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        self.assertSetEqual(
            set(prices.index.get_level_values("Field")),
            {"Close", "Volume"})
        self.assertSetEqual(
            set(prices.index.get_level_values("Time")),
            {"08:30:00", "09:00:00"})
        closes = prices.loc["Close"]
        closes_830 = closes.xs("08:30:00", level="Time")
        closes_830 = closes_830.reset_index()
        closes_830.loc[:, "Date"] = closes_830.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_830.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.15},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 51.59}]
        )
        closes_900 = closes.xs("09:00:00", level="Time")
        closes_900 = closes_900.reset_index()
        closes_900.loc[:, "Date"] = closes_900.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_900.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.25, "FI23456": 50.59},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.38, "FI23456": 51.67}]
        )

        volumes = prices.loc["Volume"]
        volumes_830 = volumes.xs("08:30:00", level="Time")
        volumes_830 = volumes_830.reset_index()
        volumes_830.loc[:, "Date"] = volumes_830.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes_830.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 1500.0, "FI23456": 9000.0},
             {'Date': '2018-04-02T00:00:00', "FI12345": 1400.0, "FI23456": 1400.0}]
        )

    def test_intraday_infer_timezone_from_securities(self):
        """
        Tests handling of an intraday db when a timezone is not specified and
        is therefore inferred from the securities master.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "30 mins",
                "universes": ["usa-stk"],
                "vendor": "ibkr"
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        "FI23456"
                        ],
                    Date=[
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T10:00:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T10:00:00-04:00",
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T10:00:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T10:00:00-04:00",
                        ],
                    Close=[
                        20.10,
                        20.25,
                        20.50,
                        20.38,
                        50.15,
                        50.59,
                        51.59,
                        51.67
                        ],
                    Volume=[
                        1500,
                        7800,
                        1400,
                        800,
                        9000,
                        7100,
                        1400,
                        1500
                    ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                            with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                                prices = get_prices(
                                    "usa-stk-15min",
                                    times=["09:30:00", "10:00:00"],
                                    fields=["Close", "Volume"]
                                )

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        self.assertSetEqual(
            set(prices.index.get_level_values("Field")),
            {"Close", "Volume"})
        self.assertSetEqual(
            set(prices.index.get_level_values("Time")),
            {"09:30:00", "10:00:00"})
        closes = prices.loc["Close"]
        closes_930 = closes.xs("09:30:00", level="Time")
        closes_930 = closes_930.reset_index()
        closes_930.loc[:, "Date"] = closes_930.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_930.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.15},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 51.59}]
        )
        closes_1000 = closes.xs("10:00:00", level="Time")
        closes_1000 = closes_1000.reset_index()
        closes_1000.loc[:, "Date"] = closes_1000.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_1000.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.25, "FI23456": 50.59},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.38, "FI23456": 51.67}]
        )

        volumes = prices.loc["Volume"]
        volumes_930 = volumes.xs("09:30:00", level="Time")
        volumes_930 = volumes_930.reset_index()
        volumes_930.loc[:, "Date"] = volumes_930.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes_930.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 1500.0, "FI23456": 9000.0},
             {'Date': '2018-04-02T00:00:00', "FI12345": 1400.0, "FI23456": 1400.0}]
        )

    def test_complain_if_cannot_infer_timezone_from_securities(self):
        """
        Tests error handling when a timezone is not specified and it cannot
        be inferred from the securities master because multiple timezones are
        represented.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "5 mins",
                "universes": ["aapl-arb"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        "FI23456"
                        ],
                    Date=[
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T10:00:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T10:00:00-04:00",
                        "2018-04-01T08:30:00-05:00",
                        "2018-04-01T09:00:00-05:00",
                        "2018-04-02T08:30:00-05:00",
                        "2018-04-02T09:00:00-05:00",
                        ],
                    Close=[
                        20.10,
                        20.25,
                        20.50,
                        20.38,
                        50.15,
                        50.59,
                        51.59,
                        51.67
                        ],
                    Volume=[
                        1500,
                        7800,
                        1400,
                        800,
                        9000,
                        7100,
                        1400,
                        1500
                    ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "America/Mexico_City"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min",
                "aapl-arb-5min",
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {}

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):
                            with patch("quantrocket.price.list_bundles", new=mock_list_bundles):
                                with self.assertRaises(ParameterError) as cm:
                                    get_prices(
                                        "aapl-arb-5min",
                                        fields=["Close", "Volume"],
                                    )

        self.assertIn((
            "cannot infer timezone because multiple timezones are present "
            "in data, please specify timezone explicitly (timezones: America/New_York, America/Mexico_City)"
            ), str(cm.exception))

    def test_multiple_timezones(self):
        """
        Tests that multiple timezones are properly handled.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "5 mins",
                "universes": ["aapl-arb"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI23456",
                        "FI23456",
                        "FI23456",
                        "FI23456"
                        ],
                    # These bars align but the times are different due to different timezones
                    Date=[
                        "2018-04-01T09:30:00-04:00",
                        "2018-04-01T10:00:00-04:00",
                        "2018-04-02T09:30:00-04:00",
                        "2018-04-02T10:00:00-04:00",
                        "2018-04-01T08:30:00-05:00",
                        "2018-04-01T09:00:00-05:00",
                        "2018-04-02T08:30:00-05:00",
                        "2018-04-02T09:00:00-05:00",
                        ],
                    Close=[
                        20.10,
                        20.25,
                        20.50,
                        20.38,
                        50.15,
                        50.59,
                        51.59,
                        51.67
                        ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Timezone=["America/New_York", "America/Mexico_City"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "aapl-arb-5min",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                            with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                                prices = get_prices(
                                    "aapl-arb-5min",
                                    fields=["Close"],
                                    timezone="America/New_York"
                                )

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        self.assertSetEqual(
            set(prices.index.get_level_values("Field")), {"Close"})
        self.assertSetEqual(
            set(prices.index.get_level_values("Time")),
            {"09:30:00", "10:00:00"})
        closes = prices.loc["Close"]
        closes_930 = closes.xs("09:30:00", level="Time")
        closes_930 = closes_930.reset_index()
        closes_930.loc[:, "Date"] = closes_930.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_930.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.15},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 51.59}]
        )
        closes_1000 = closes.xs("10:00:00", level="Time")
        closes_1000 = closes_1000.reset_index()
        closes_1000.loc[:, "Date"] = closes_1000.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_1000.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.25, "FI23456": 50.59},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.38, "FI23456": 51.67}]
        )

        # repeat with a different timezone
        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                            with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                                prices = get_prices(
                                    "aapl-arb-5min",
                                    fields=["Close"],
                                    timezone="America/Mexico_City"
                                )

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345","FI23456"])
        self.assertSetEqual(
            set(prices.index.get_level_values("Field")), {"Close"})
        self.assertSetEqual(
            set(prices.index.get_level_values("Time")),
            {"08:30:00", "09:00:00"})
        closes = prices.loc["Close"]
        closes_830 = closes.xs("08:30:00", level="Time")
        closes_830 = closes_830.reset_index()
        closes_830.loc[:, "Date"] = closes_830.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_830.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.1, "FI23456": 50.15},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.5, "FI23456": 51.59}]
        )
        closes_900 = closes.xs("09:00:00", level="Time")
        closes_900 = closes_900.reset_index()
        closes_900.loc[:, "Date"] = closes_900.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_900.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', "FI12345": 20.25, "FI23456": 50.59},
             {'Date': '2018-04-02T00:00:00', "FI12345": 20.38, "FI23456": 51.67}]
        )

    def test_intraday_fill_missing_times(self):
        """
        Tests that each date is expanded to include all times represented in
        any date. (This makes the data easier to work when there early close
        days or when the data is queried intraday.)
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "2 hours",
                "universes": ["usa-stk"],
                "vendor": "ibkr",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    Sid=[
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345",
                        "FI12345"
                        ],
                    Date=[
                        # complete day
                        "2018-04-01T10:00:00-04:00",
                        "2018-04-01T12:00:00-04:00",
                        "2018-04-01T14:00:00-04:00",
                        # early close
                        "2018-04-02T10:00:00-04:00",
                        "2018-04-02T12:00:00-04:00",
                        # Complete day
                        "2018-04-03T10:00:00-04:00",
                        "2018-04-03T12:00:00-04:00",
                        "2018-04-03T14:00:00-04:00",
                        # intraday query
                        "2018-04-04T10:00:00-04:00",
                        ],
                    Close=[
                        20.10,
                        20.25,
                        20.50,
                        20.38,
                        21.15,
                        22.59,
                        21.59,
                        20.67,
                        21.34
                        ]
                ))
            prices.to_csv(f, index=False)

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-2h",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        def mock_list_bundles():
            return {"usstock-1min": True}

        with patch('quantrocket.price.list_bundles', new=mock_list_bundles):
            with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
                with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                    with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                        with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                            prices = get_prices(
                                "usa-stk-2h",
                                timezone="America/New_York"
                            )

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "Sid")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), ["FI12345"])
        self.assertSetEqual(
            set(prices.index.get_level_values("Field")),
            {"Close"})
        self.assertSetEqual(
            set(prices.index.get_level_values("Time")),
            {"10:00:00", "12:00:00", "14:00:00"})

        closes = prices.loc["Close"]

        all_days = pd.DatetimeIndex(["2018-04-01", "2018-04-02", "2018-04-03", "2018-04-04"])

        for time in ["10:00:00", "12:00:00", "14:00:00"]:
            idx = closes.xs(time, level="Time").index
            self.assertListEqual(
                list(idx),
                list(all_days),
                msg=time
            )

        # replace nan with "nan" to allow equality comparisons
        closes = closes.where(closes.notnull(), "nan")
        closes = closes["FI12345"]
        self.assertEqual(
            closes.xs("14:00:00", level="Time").loc["2018-04-02"], "nan"
        )
        self.assertEqual(
            closes.xs("12:00:00", level="Time").loc["2018-04-04"], "nan"
        )
        self.assertEqual(
            closes.xs("14:00:00", level="Time").loc["2018-04-04"], "nan"
        )

class GetPricesReindexedLikeTestCase(unittest.TestCase):
    """
    Test cases for `quantrocket.get_prices_reindexed_like`.
    """

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
            get_prices_reindexed_like(closes, "custom-fundamental-1d")

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
            get_prices_reindexed_like(closes, "custom-fundamental-1d")

        self.assertIn("reindex_like must have a DatetimeIndex", str(cm.exception))

    @patch("quantrocket.price.get_prices")
    def test_pass_args_correctly(self, mock_get_prices):
        """
        Tests that codes, sids, date ranges and and other args are correctly
        passed to get_prices.
        """
        closes = pd.DataFrame(
            np.random.rand(6,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-03-06", periods=6, freq="D", name="Date"))

        def _mock_get_prices(*args, **kwargs):
            dt_idx = pd.DatetimeIndex(["2018-03-04"])
            fields = ["EPS","Revenue"]
            idx = pd.MultiIndex.from_product([fields, dt_idx], names=["Field", "Date"])

            prices = pd.DataFrame(
                {
                    "FI12345": [
                        # EPS
                        9,
                        # Revenue
                        10e5,
                    ],
                    "FI23456": [
                        # EPS
                        19.89,
                        # Revenue
                        5e8
                    ],
                 },
                index=idx
            )
            return prices

        mock_get_prices.return_value = _mock_get_prices()

        get_prices_reindexed_like(
            closes, "custom-fundamental-1d",
            fields=["Revenue", "EPS"],
            lookback_window=2,
            timezone="America/New_York",
            times=["11:00:00", "12:00:00"],
            cont_fut=False,
            data_frequency="daily")

        get_prices_call = mock_get_prices.mock_calls[0]
        _, args, kwargs = get_prices_call
        self.assertEqual(args[0], "custom-fundamental-1d")
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2018-03-04") # 2018-03-06 - 2 day lookback window
        self.assertEqual(kwargs["end_date"], "2018-03-11")
        self.assertEqual(kwargs["fields"], ["Revenue", "EPS"])
        self.assertEqual(kwargs["timezone"], "America/New_York")
        self.assertEqual(kwargs["times"], ["11:00:00", "12:00:00"])
        self.assertFalse(kwargs["cont_fut"])
        self.assertEqual(kwargs["data_frequency"], "daily")

        # repeat with default lookback
        get_prices_reindexed_like(
            closes, "custom-fundamental-1d",
            fields="Revenue")

        get_prices_call = mock_get_prices.mock_calls[1]
        _, args, kwargs = get_prices_call
        self.assertEqual(args[0], "custom-fundamental-1d")
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["start_date"], "2018-02-24") # 2018-03-06 - 10 day lookback window
        self.assertEqual(kwargs["end_date"], "2018-03-11")
        self.assertEqual(kwargs["fields"], "Revenue")

    def test_tz_aware_index(self):
        """
        Tests that reindex_like.index can be tz-naive or tz-aware.
        """

        def mock_get_prices(*args, **kwargs):
            dt_idx = pd.DatetimeIndex(["2018-03-31", "2018-06-30"])
            fields = ["EPS"]
            idx = pd.MultiIndex.from_product([fields, dt_idx], names=["Field", "Date"])

            prices = pd.DataFrame(
                {
                    "FI12345": [
                        9,
                        9.5,
                    ],
                    "FI23456": [
                        19.89,
                        17.60
                    ],
                 },
                index=idx
            )
            return prices

        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            # request with tz_naive
            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-06-30", periods=4, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS")

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            eps.to_dict(orient="records"),
            [{'Date': '2018-06-30T00:00:00', 'FI12345': 9.0},
            {'Date': '2018-07-01T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-02T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-03T00:00:00', 'FI12345': 9.5}]
        )

        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            # request with tz aware
            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-06-30", periods=4, freq="D",
                    tz="America/New_York",name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS")

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            eps.to_dict(orient="records"),
            [{'Date': '2018-06-30T00:00:00-0400', 'FI12345': 9.0},
            {'Date': '2018-07-01T00:00:00-0400', 'FI12345': 9.5},
            {'Date': '2018-07-02T00:00:00-0400', 'FI12345': 9.5},
            {'Date': '2018-07-03T00:00:00-0400', 'FI12345': 9.5}])

    def test_shift(self):
        """
        Tests handling of the shift parameter.
        """

        def mock_get_prices(*args, **kwargs):
            dt_idx = pd.DatetimeIndex(["2018-03-31", "2018-06-30"])
            fields = ["EPS"]
            idx = pd.MultiIndex.from_product([fields, dt_idx], names=["Field", "Date"])

            prices = pd.DataFrame(
                {
                    "FI12345": [
                        9,
                        9.5,
                    ],
                    "FI23456": [
                        19.89,
                        17.60
                    ],
                 },
                index=idx
            )
            return prices

        # the default shift is 1
        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-06-30", periods=4, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS",
                lookback_window=180)

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            eps.to_dict(orient="records"),
            [{'Date': '2018-06-30T00:00:00', 'FI12345': 9.0},
            {'Date': '2018-07-01T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-02T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-03T00:00:00', 'FI12345': 9.5}]
        )

        # try shifting 2
        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            closes = pd.DataFrame(
                np.random.rand(3,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-07-01", periods=3, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS", shift=2,
                lookback_window=180)

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            eps.to_dict(orient="records"),
            [{'Date': '2018-07-01T00:00:00', 'FI12345': 9.0},
            {'Date': '2018-07-02T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-03T00:00:00', 'FI12345': 9.5}]
        )

        # try shift 0
        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-06-30", periods=4, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS",
                shift=0,
                lookback_window=180)

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            eps.to_dict(orient="records"),
            [{'Date': '2018-06-30T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-01T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-02T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-03T00:00:00', 'FI12345': 9.5}]
        )

    def test_no_ffill(self):
        """
        Tests handling of the ffill parameter.
        """

        def mock_get_prices(*args, **kwargs):
            dt_idx = pd.DatetimeIndex(["2018-03-31", "2018-06-30"])
            fields = ["EPS"]
            idx = pd.MultiIndex.from_product([fields, dt_idx], names=["Field", "Date"])

            prices = pd.DataFrame(
                {
                    "FI12345": [
                        9,
                        9.5,
                    ],
                    "FI23456": [
                        19.89,
                        17.60
                    ],
                 },
                index=idx
            )
            return prices

        # the default shift is forward-filled
        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-06-30", periods=4, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS",
                lookback_window=180)

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            eps.to_dict(orient="records"),
            [{'Date': '2018-06-30T00:00:00', 'FI12345': 9.0},
            {'Date': '2018-07-01T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-02T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-03T00:00:00', 'FI12345': 9.5}]
        )

        # try not forward-filling
        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            closes = pd.DataFrame(
                np.random.rand(4,1),
                columns=["FI12345"],
                index=pd.date_range(start="2018-06-30", periods=4, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS", ffill=False,
                lookback_window=180)

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        # replace Nan with "nan" to allow equality comparisons
        eps = eps.fillna("nan")

        self.assertListEqual(
            eps.to_dict(orient="records"),
            [{'Date': '2018-06-30T00:00:00', 'FI12345': 9.0},
            {'Date': '2018-07-01T00:00:00', 'FI12345': 9.5},
            {'Date': '2018-07-02T00:00:00', 'FI12345': 'nan'},
            {'Date': '2018-07-03T00:00:00', 'FI12345': 'nan'}]
        )

    def test_daily_dataframe_with_daily_database(self):
        """
        Tests the scenario of using a daily dataframe to query a daily database.
        """

        def mock_get_prices(*args, **kwargs):
            dt_idx = pd.DatetimeIndex(["2018-03-31", "2018-06-30"])
            fields = ["EPS"]
            idx = pd.MultiIndex.from_product([fields, dt_idx], names=["Field", "Date"])

            prices = pd.DataFrame(
                {
                    "FI12345": [
                        9,
                        9.5,
                    ],
                    "FI23456": [
                        19.89,
                        17.60
                    ],
                 },
                index=idx
            )
            return prices

        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            closes = pd.DataFrame(
                np.random.rand(4,3),
                columns=["FI12345", "FI23456", "FI34567"],
                index=pd.date_range(start="2018-06-30", periods=4, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS",
                lookback_window=180)

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        # replace Nan with "nan" to allow equality comparisons
        eps = eps.fillna("nan")
        self.assertListEqual(
            eps.to_dict(orient="records"),
            [
                {'Date': '2018-06-30T00:00:00', 'FI12345': 9.0, 'FI23456': 19.89, 'FI34567': 'nan'},
                {'Date': '2018-07-01T00:00:00', 'FI12345': 9.5, 'FI23456': 17.6, 'FI34567': 'nan'},
                {'Date': '2018-07-02T00:00:00', 'FI12345': 9.5, 'FI23456': 17.6, 'FI34567': 'nan'},
                {'Date': '2018-07-03T00:00:00', 'FI12345': 9.5, 'FI23456': 17.6, 'FI34567': 'nan'}
            ]
        )

    def test_intraday_dataframe_with_daily_database(self):
        """
        Tests the scenario of using a intraday dataframe to query a daily database.
        """

        def mock_get_prices(*args, **kwargs):
            dt_idx = pd.DatetimeIndex(["2018-03-31", "2018-06-30"])
            fields = ["EPS"]
            idx = pd.MultiIndex.from_product([fields, dt_idx], names=["Field", "Date"])

            prices = pd.DataFrame(
                {
                    "FI12345": [
                        9,
                        9.5,
                    ],
                    "FI23456": [
                        19.89,
                        17.60
                    ],
                 },
                index=idx
            )
            return prices

        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            dt_idx = pd.date_range(start="2018-06-30", periods=2, freq="D", name="Date")
            times = ["11:00:00", "12:00:00"]
            idx = pd.MultiIndex.from_product([dt_idx, times], names=["Date", "Time"])

            closes = pd.DataFrame(
                np.random.rand(4,3),
                columns=["FI12345", "FI23456", "FI34567"],
                index=idx)

            data = get_prices_reindexed_like(
                closes, "custom-fundamental", fields="EPS",
                lookback_window=180)

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"EPS"})

        eps = data.loc["EPS"]
        self.assertListEqual(list(eps.index), list(closes.index))
        self.assertListEqual(list(eps.columns), list(closes.columns))
        eps = eps.reset_index()
        eps.loc[:, "Date"] = eps.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        # replace Nan with "nan" to allow equality comparisons
        eps = eps.fillna("nan")
        self.assertListEqual(
            eps.to_dict(orient="records"),
            [
                {'Date': '2018-06-30T00:00:00', 'Time': '11:00:00', 'FI12345': 9.0, 'FI23456': 19.89, 'FI34567': 'nan'},
                {'Date': '2018-06-30T00:00:00', 'Time': '12:00:00', 'FI12345': 9.0, 'FI23456': 19.89, 'FI34567': 'nan'},
                {'Date': '2018-07-01T00:00:00', 'Time': '11:00:00', 'FI12345': 9.5, 'FI23456': 17.6, 'FI34567': 'nan'},
                {'Date': '2018-07-01T00:00:00', 'Time': '12:00:00', 'FI12345': 9.5, 'FI23456': 17.6, 'FI34567': 'nan'}]
        )

    def test_intraday_agg(self):
        """
        Tests handling of the agg parameter when querying an intraday database.
        """

        def mock_get_prices(*args, **kwargs):
            dt_idx = pd.DatetimeIndex(["2018-06-29", "2018-06-30"])
            times = ["11:00:00", "12:00:00"]
            fields = ["Close"]
            idx = pd.MultiIndex.from_product([fields, dt_idx, times], names=["Field", "Date", "Time"])

            prices = pd.DataFrame(
                {
                    "FI12345": [
                        9,
                        9.5,
                        10.20,
                        10.30
                    ],
                    "FI23456": [
                        19.89,
                        17.60,
                        20.20,
                        20.10
                    ],
                 },
                index=idx
            )
            return prices

        # the default agg is last
        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            closes = pd.DataFrame(
                np.random.rand(3,3),
                columns=["FI12345", "FI23456", "FI34567"],
                index=pd.date_range(start="2018-06-30", periods=3, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-data", fields="Close")

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"Close"})

        data = data.loc["Close"]
        self.assertListEqual(list(data.index), list(closes.index))
        self.assertListEqual(list(data.columns), list(closes.columns))
        data = data.reset_index()
        data.loc[:, "Date"] = data.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        # replace Nan with "nan" to allow equality comparisons
        data = data.fillna("nan")
        self.assertListEqual(
            data.to_dict(orient="records"),
            [
                {'Date': '2018-06-30T00:00:00', 'FI12345': 9.5, 'FI23456': 17.6, 'FI34567': 'nan'},
                {'Date': '2018-07-01T00:00:00', 'FI12345': 10.3, 'FI23456': 20.1, 'FI34567': 'nan'},
                {'Date': '2018-07-02T00:00:00', 'FI12345': 10.3, 'FI23456': 20.1, 'FI34567': 'nan'}]
        )

        # repeat with agg "mean"
        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            closes = pd.DataFrame(
                np.random.rand(3,3),
                columns=["FI12345", "FI23456", "FI34567"],
                index=pd.date_range(start="2018-06-30", periods=3, freq="D", name="Date"))

            data = get_prices_reindexed_like(
                closes, "custom-data", fields="Close", agg="mean")

        data = data.loc["Close"]
        data = data.reset_index()
        data.loc[:, "Date"] = data.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        # replace Nan with "nan" to allow equality comparisons
        data = data.fillna("nan")
        self.assertListEqual(
            data.to_dict(orient="records"),
            [
                {'Date': '2018-06-30T00:00:00', 'FI12345': 9.25, 'FI23456': 18.745, 'FI34567': 'nan'},
                {'Date': '2018-07-01T00:00:00', 'FI12345': 10.25, 'FI23456': 20.15, 'FI34567': 'nan'},
                {'Date': '2018-07-02T00:00:00', 'FI12345': 10.25, 'FI23456': 20.15, 'FI34567': 'nan'}]
        )

    def test_intraday_dataframe_with_intraday_database(self):
        """
        Tests the scenario of using a intraday dataframe to query an intraday database.
        """

        def mock_get_prices(*args, **kwargs):
            dt_idx = pd.DatetimeIndex(["2018-06-29", "2018-06-30"])
            times = ["11:00:00", "12:00:00"]
            fields = ["Close"]
            idx = pd.MultiIndex.from_product([fields, dt_idx, times], names=["Field", "Date", "Time"])

            prices = pd.DataFrame(
                {
                    "FI12345": [
                        9,
                        9.5,
                        10.20,
                        10.30
                    ],
                    "FI23456": [
                        19.89,
                        17.60,
                        20.20,
                        20.10
                    ],
                 },
                index=idx
            )
            return prices

        dt_idx = pd.date_range(start="2018-06-30", periods=2, freq="D", name="Date")
        times = ["11:00:00", "12:00:00"]
        idx = pd.MultiIndex.from_product([dt_idx, times], names=["Date", "Time"])

        closes = pd.DataFrame(
            np.random.rand(4,3),
            columns=["FI12345", "FI23456", "FI34567"],
            index=idx)

        with patch('quantrocket.price.get_prices', new=mock_get_prices):

            data = get_prices_reindexed_like(
                closes, "custom-data", fields="Close")

        self.assertSetEqual(set(data.index.get_level_values("Field")), {"Close"})

        data = data.loc["Close"]
        self.assertListEqual(list(data.index), list(closes.index))
        self.assertListEqual(list(data.columns), list(closes.columns))
        data = data.reset_index()
        data.loc[:, "Date"] = data.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        # replace Nan with "nan" to allow equality comparisons
        data = data.fillna("nan")
        self.assertListEqual(
            data.to_dict(orient="records"),
            [
                {'Date': '2018-06-30T00:00:00', 'Time': '11:00:00', 'FI12345': 9.5, 'FI23456': 17.6, 'FI34567': 'nan'},
                {'Date': '2018-06-30T00:00:00', 'Time': '12:00:00', 'FI12345': 9.5, 'FI23456': 17.6, 'FI34567': 'nan'},
                {'Date': '2018-07-01T00:00:00', 'Time': '11:00:00', 'FI12345': 10.3, 'FI23456': 20.1, 'FI34567': 'nan'},
                {'Date': '2018-07-01T00:00:00', 'Time': '12:00:00', 'FI12345': 10.3, 'FI23456': 20.1, 'FI34567': 'nan'}]
        )
