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
from quantrocket import get_prices
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
    @patch("quantrocket.price.get_history_db_config")
    @patch("quantrocket.price.download_history_file")
    @patch("quantrocket.price.download_master_file")
    def test_pass_args_correctly_single_db_no_master(self,
                                                     mock_download_master_file,
                                                     mock_download_history_file,
                                                     mock_get_history_db_config,
                                                     mock_list_history_databases,
                                                     mock_list_realtime_databases):
        """
        Tests that args are correctly passed to download_history_file,
        download_master_file, and get_db_config, when there is a single db
        and no master fields are needed.
        """
        def _mock_get_history_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["usa-stk"],
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def _mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456
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

        mock_list_history_databases.side_effect = _mock_list_history_databases
        mock_list_realtime_databases.side_effect = _mock_list_realtime_databases

        mock_download_history_file.side_effect = _mock_download_history_file
        mock_get_history_db_config.side_effect = _mock_get_history_db_config

        get_prices(
            "usa-stk-1d", start_date="2018-04-01", end_date="2018-04-03",
            universes="usa-stk", conids=[12345,23456], fields=["Close"],
            exclude_universes=["usa-stk-pharm"],
            exclude_conids=[99999], cont_fut=False, timezone="America/New_York")

        mock_download_master_file.assert_not_called()

        self.assertEqual(len(mock_get_history_db_config.mock_calls), 1)
        db_config_call = mock_get_history_db_config.mock_calls[0]
        _, args, kwargs = db_config_call
        self.assertEqual(args[0], "usa-stk-1d")

        mock_list_history_databases.assert_called_once_with()
        mock_list_realtime_databases.assert_called_once_with()

        history_call = mock_download_history_file.mock_calls[0]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "usa-stk-1d")
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        self.assertListEqual(kwargs["fields"], ["Close"])
        self.assertEqual(kwargs["universes"], "usa-stk")
        self.assertListEqual(kwargs["exclude_conids"], [99999])
        self.assertListEqual(kwargs["exclude_universes"], ["usa-stk-pharm"])
        self.assertFalse(kwargs["cont_fut"])

    @patch("quantrocket.price.list_realtime_databases")
    @patch("quantrocket.price.list_history_databases")
    @patch("quantrocket.price.get_realtime_db_config")
    @patch("quantrocket.price.get_history_db_config")
    @patch("quantrocket.price.download_market_data_file")
    @patch("quantrocket.price.download_history_file")
    @patch("quantrocket.price.download_master_file")
    def test_pass_args_correctly_multi_db_and_master(self,
                                                     mock_download_master_file,
                                                     mock_download_history_file,
                                                     mock_download_market_data_file,
                                                     mock_get_history_db_config,
                                                     mock_get_realtime_db_config,
                                                     mock_list_history_databases,
                                                     mock_list_realtime_databases):
        """
        Tests that args are correctly passed to download_history_file,
        download_master_file, and get_db_config, when there are multiple dbs,
        including 2 history dbs and 1 realtime db, and master fields are
        needed.
        """
        def _mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["japan-stk"],
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def _mock_get_realtime_db_config(db):
            return {
                "bar_size": "1 day",
                "fields": ["LastClose", "LastOpen", "VolumeClose"],
                "tick_db_code": "demo-stk-taq"
            }

        def _mock_download_history_file(code, f, *args, **kwargs):
            if code == "usa-stk-1d":
                prices = pd.DataFrame(
                    dict(
                        ConId=[
                            12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456
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
                        ConId=[
                            56789,
                            56789,
                            56789,
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
                    ConId=[
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456
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

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456,56789],
                                           Symbol=["ABC","DEF", "9456"]))
            securities.to_csv(f, index=False)
            f.seek(0)

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

        mock_download_history_file.side_effect = _mock_download_history_file
        mock_download_market_data_file.side_effect = _mock_download_market_data_file

        mock_get_history_db_config.side_effect = _mock_get_history_db_config
        mock_get_realtime_db_config.side_effect = _mock_get_realtime_db_config

        mock_download_master_file.side_effect = _mock_download_master_file

        mock_list_history_databases.side_effect = _mock_list_history_databases
        mock_list_realtime_databases.side_effect = _mock_list_realtime_databases

        get_prices(
            ["usa-stk-1d", "japan-stk-1d", "demo-stk-taq-1d"],
            start_date="2018-04-01", end_date="2018-04-03",
            conids=[12345,23456,56789], fields=["Close", "LastClose"],
            master_fields="Symbol")

        mock_list_history_databases.assert_called_once_with()
        mock_list_realtime_databases.assert_called_once_with()

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
        self.assertListEqual(kwargs["conids"], [12345,23456,56789])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        # only supported subset of fields is requested
        self.assertListEqual(kwargs["fields"], ["Close"])

        history_call = mock_download_history_file.mock_calls[1]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "japan-stk-1d")
        self.assertListEqual(kwargs["conids"], [12345,23456,56789])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        self.assertListEqual(kwargs["fields"], ["Close"])

        self.assertEqual(len(mock_get_realtime_db_config.mock_calls), 1)
        db_config_call = mock_get_realtime_db_config.mock_calls[0]
        _, args, kwargs = db_config_call
        self.assertEqual(args[0], "demo-stk-taq-1d")

        self.assertEqual(len(mock_download_market_data_file.mock_calls), 1)
        realtime_call = mock_download_market_data_file.mock_calls[0]
        _, args, kwargs = realtime_call
        self.assertEqual(args[0], "demo-stk-taq-1d")
        self.assertListEqual(kwargs["conids"], [12345,23456,56789])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        # only supported subset of fields is requested
        self.assertListEqual(kwargs["fields"], ["LastClose"])

        self.assertEqual(len(mock_download_master_file.mock_calls), 1)
        master_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = master_call
        self.assertListEqual(kwargs["conids"], [12345,23456,56789])
        self.assertListEqual(kwargs["fields"], ["Symbol"])
        self.assertEqual(kwargs["domain"], "main")

    @patch("quantrocket.price.list_realtime_databases")
    @patch("quantrocket.price.list_history_databases")
    @patch("quantrocket.price.get_history_db_config")
    @patch("quantrocket.price.download_history_file")
    @patch("quantrocket.price.download_master_file")
    def test_use_other_master_domain(self,
                                         mock_download_master_file,
                                         mock_download_history_file,
                                         mock_get_history_db_config,
                                         mock_list_history_databases,
                                         mock_list_realtime_databases):
        """
        Tests that the domain is read from the db config and passed to the
        master service.
        """
        def _mock_get_history_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["nyse-stk"],
                "vendor": "sharadar",
                "domain": "sharadar",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def _mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        56789,
                        56789,
                        56789,
                        ],
                    Date=[
                        "2018-04-01",
                        "2018-04-02",
                        "2018-04-03",
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
                        42.32,
                        43.34,
                        43.12
                    ]))
            prices.to_csv(f, index=False)

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456,56789],
                                           Symbol=["ABC","DEF", "9456"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def _mock_list_history_databases():
            return [
                "usa-stk-1d",
                "demo-stk-1min",
                "sharadar-1d",
            ]

        def _mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        mock_list_history_databases.side_effect = _mock_list_history_databases
        mock_list_realtime_databases.side_effect = _mock_list_realtime_databases

        mock_download_history_file.side_effect = _mock_download_history_file

        mock_get_history_db_config.side_effect = _mock_get_history_db_config

        mock_download_master_file.side_effect = _mock_download_master_file

        get_prices(
            "sharadar-1d", start_date="2018-04-01", end_date="2018-04-03",
            conids=[12345,23456,56789], fields=["Close"],
            master_fields="Symbol")

        mock_list_history_databases.assert_called_once_with()
        mock_list_realtime_databases.assert_called_once_with()

        self.assertEqual(len(mock_get_history_db_config.mock_calls), 1)
        db_config_call = mock_get_history_db_config.mock_calls[0]
        _, args, kwargs = db_config_call
        self.assertEqual(args[0], "sharadar-1d")

        self.assertEqual(len(mock_download_history_file.mock_calls), 1)
        history_call = mock_download_history_file.mock_calls[0]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "sharadar-1d")
        self.assertListEqual(kwargs["conids"], [12345,23456,56789])
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        self.assertListEqual(kwargs["fields"], ["Close"])

        self.assertEqual(len(mock_download_master_file.mock_calls), 1)
        master_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = master_call
        self.assertListEqual(kwargs["conids"], [12345,23456,56789])
        self.assertListEqual(kwargs["fields"], ["Symbol"])
        self.assertEqual(kwargs["domain"], "sharadar")

    @patch("quantrocket.price.list_realtime_databases")
    @patch("quantrocket.price.list_history_databases")
    @patch("quantrocket.price.get_history_db_config")
    @patch("quantrocket.price.download_history_file")
    @patch("quantrocket.price.download_master_file")
    def test_pass_master_fieds_as_list_tuple_or_string(self,
                                                       mock_download_master_file,
                                                       mock_download_history_file,
                                                       mock_get_history_db_config,
                                                       mock_list_history_databases,
                                                       mock_list_realtime_databases):
        """
        Tests that master_fields can be passed as a list, tuple, or string.
        """
        def _mock_get_history_db_config(db):
            return {
                "bar_size": "15 mins",
                "universes": ["usa-stk"],
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def _mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456
                        ],
                    Date=[
                        "2018-04-01T09:30:00-00:00",
                        "2018-04-02T09:30:00-00:00",
                        "2018-04-03T09:30:00-00:00",
                        "2018-04-01T09:30:00-00:00",
                        "2018-04-02T09:30:00-00:00",
                        "2018-04-03T09:30:00-00:00"
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

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456],
                                           Symbol=["ABC","DEF"],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def _mock_list_history_databases():
            return [
                "usa-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def _mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        mock_list_history_databases.side_effect = _mock_list_history_databases
        mock_list_realtime_databases.side_effect = _mock_list_realtime_databases

        mock_download_history_file.side_effect = _mock_download_history_file

        mock_get_history_db_config.side_effect = _mock_get_history_db_config

        mock_download_master_file.side_effect = _mock_download_master_file

        # pass as string
        get_prices(
            "usa-stk-15min", start_date="2018-04-01", end_date="2018-04-03",
            fields=["Close"], master_fields="Symbol", times=["09:30:00"])


        self.assertEqual(len(mock_get_history_db_config.mock_calls), 1)
        db_config_call = mock_get_history_db_config.mock_calls[0]
        _, args, kwargs = db_config_call
        self.assertEqual(args[0], "usa-stk-15min")

        self.assertEqual(len(mock_download_history_file.mock_calls), 1)
        history_call = mock_download_history_file.mock_calls[0]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "usa-stk-15min")
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        self.assertListEqual(kwargs["fields"], ["Close"])
        self.assertListEqual(kwargs["times"], ["09:30:00"])

        self.assertEqual(len(mock_download_master_file.mock_calls), 1)
        master_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = master_call
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertListEqual(kwargs["fields"], ["Symbol", "Timezone"])

        # pass as tuple
        get_prices(
            "usa-stk-15min", start_date="2018-04-01", end_date="2018-04-03",
            fields=["Close"], master_fields=("Symbol",), times=["09:30:00"])


        self.assertEqual(len(mock_get_history_db_config.mock_calls), 2)
        db_config_call = mock_get_history_db_config.mock_calls[1]
        _, args, kwargs = db_config_call
        self.assertEqual(args[0], "usa-stk-15min")

        self.assertEqual(len(mock_download_history_file.mock_calls), 2)
        history_call = mock_download_history_file.mock_calls[1]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "usa-stk-15min")
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        self.assertListEqual(kwargs["fields"], ["Close"])
        self.assertListEqual(kwargs["times"], ["09:30:00"])

        self.assertEqual(len(mock_download_master_file.mock_calls), 2)
        master_call = mock_download_master_file.mock_calls[1]
        _, args, kwargs = master_call
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertListEqual(kwargs["fields"], ["Symbol", "Timezone"])

        # pass as list
        get_prices(
            "usa-stk-15min", start_date="2018-04-01", end_date="2018-04-03",
            fields=["Close"], master_fields=["Symbol"], times=["09:30:00"])


        self.assertEqual(len(mock_get_history_db_config.mock_calls), 3)
        db_config_call = mock_get_history_db_config.mock_calls[2]
        _, args, kwargs = db_config_call
        self.assertEqual(args[0], "usa-stk-15min")

        self.assertEqual(len(mock_download_history_file.mock_calls), 3)
        history_call = mock_download_history_file.mock_calls[2]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "usa-stk-15min")
        self.assertEqual(kwargs["start_date"], "2018-04-01")
        self.assertEqual(kwargs["end_date"], "2018-04-03")
        self.assertListEqual(kwargs["fields"], ["Close"])
        self.assertListEqual(kwargs["times"], ["09:30:00"])

        self.assertEqual(len(mock_download_master_file.mock_calls), 3)
        master_call = mock_download_master_file.mock_calls[2]
        _, args, kwargs = master_call
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertListEqual(kwargs["fields"], ["Symbol", "Timezone"])

    @patch("quantrocket.price.list_realtime_databases")
    @patch("quantrocket.price.list_history_databases")
    @patch("quantrocket.price.get_history_db_config")
    @patch("quantrocket.price.download_history_file")
    @patch("quantrocket.price.download_master_file")
    def test_get_master_conids_from_prices(self,
                                           mock_download_master_file,
                                           mock_download_history_file,
                                           mock_get_history_db_config,
                                           mock_list_history_databases,
                                           mock_list_realtime_databases):
        """
        Tests that conids are taken from the prices and passed to
        download_master_file.
        """
        def _mock_get_history_db_config(db):
            return {
                "bar_size": "15 mins",
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def _mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        34567,
                        34567,
                        34567
                        ],
                    Date=[
                        "2018-04-01T09:30:00-00:00",
                        "2018-04-02T09:30:00-00:00",
                        "2018-04-03T09:30:00-00:00",
                        "2018-04-01T09:30:00-00:00",
                        "2018-04-02T09:30:00-00:00",
                        "2018-04-03T09:30:00-00:00"
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

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,34567],
                                           Symbol=["ABC","GHI"],
                                           Timezone=["America/New_York", "America/New_York"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        def _mock_list_history_databases():
            return [
                "usa-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min"
            ]

        def _mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        mock_list_history_databases.side_effect = _mock_list_history_databases
        mock_list_realtime_databases.side_effect = _mock_list_realtime_databases

        mock_download_history_file.side_effect = _mock_download_history_file

        mock_get_history_db_config.side_effect = _mock_get_history_db_config

        mock_download_master_file.side_effect = _mock_download_master_file

        # No universes or conids passed
        get_prices(
            "usa-stk-15min", start_date="2018-04-01", end_date="2018-04-03",
            fields=["Close"], master_fields="Symbol", times=["09:30:00"])


        self.assertEqual(len(mock_download_master_file.mock_calls), 1)
        master_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = master_call
        self.assertNotIn("universes", kwargs)
        self.assertListEqual(kwargs["conids"], [12345,34567])
        self.assertListEqual(kwargs["fields"], ["Symbol", "Timezone"])

    def test_query_eod_history_db(self):
        """
        Tests maniuplation of a single EOD db.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["usa-stk"],
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                        prices = get_prices("usa-stk-1d", fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), [12345,23456])
        self.assertListEqual(
            list(prices.index.get_level_values("Field")),
            ["Close", "Close", "Close", "Volume", "Volume", "Volume"])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.5},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 52.5},
             {'Date': '2018-04-03T00:00:00', 12345: 19.4, 23456: 51.59}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 15000, 23456: 98000},
             {'Date': '2018-04-02T00:00:00', 12345: 7800, 23456: 179000},
             {'Date': '2018-04-03T00:00:00', 12345: 12400, 23456: 142500}]
        )

        # Repeat with master fields
        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456],
                                           Symbol=["ABC","DEF"],
                                           PrimaryExchange=["NYSE", "NASDAQ"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                            prices = get_prices("usa-stk-1d",
                                                fields=["Close", "Volume"],
                                                master_fields=["Symbol", "PrimaryExchange"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), [12345,23456])
        self.assertSetEqual(
            set(prices.index.get_level_values("Field")),
            {"Close", "Volume", "Symbol", "PrimaryExchange"})
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.5},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 52.5},
             {'Date': '2018-04-03T00:00:00', 12345: 19.4, 23456: 51.59}]
        )
        symbols = prices.loc["Symbol"]
        symbols = symbols.reset_index()
        symbols.loc[:, "Date"] = symbols.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            symbols.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: "ABC", 23456: "DEF"}]
        )

        exchanges = prices.loc["PrimaryExchange"]
        exchanges = exchanges.reset_index()
        exchanges.loc[:, "Date"] = exchanges.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            exchanges.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: "NYSE", 23456: "NASDAQ"}]
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
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["japan-stk"],
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_download_history_file(code, f, *args, **kwargs):
            if code == "usa-stk-1d":
                prices = pd.DataFrame(
                    dict(
                        ConId=[
                            12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
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
                        ConId=[
                            56789,
                            56789,
                            56789,
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                        prices = get_prices(["usa-stk-1d", "japan-stk-1d"], fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), [12345,23456,56789])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.5, 56789: 5900.0},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 52.5, 56789: 5920.0},
             {'Date': '2018-04-03T00:00:00', 12345: 19.4, 23456: 51.59, 56789: 5950.0}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 15000, 23456: 98000, 56789: 18000},
             {'Date': '2018-04-02T00:00:00', 12345: 7800, 23456: 179000, 56789: 17600},
             {'Date': '2018-04-03T00:00:00', 12345: 12400, 23456: 142500, 56789: 5600}]
        )

    def test_consolidate_intraday_history_and_realtime_distinct_fields(self):
        """
        Tests that when querying a history and real-time database with
        distinct fields and overlapping dates/conids, both fields are
        preserved.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "15 mins",
                "universes": ["usa-stk"],
                "vendor": "ib",
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
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456,
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
                    ConId=[
                        12345,
                        12345,
                        23456,
                        23456,
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
            securities = pd.DataFrame(dict(ConId=[12345,23456],
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
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: 20.1, 23456: 50.5},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 20.5, 23456: 52.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: 19.4, 23456: 51.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 18.56, 23456: 54.23}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: 15000.0, 23456: 98000.0},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 7800.0, 23456: 179000.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: 12400.0, 23456: 142500.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 14500.0, 23456: 124000.0}]
        )

        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            # Data was UTC but now NY
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: "nan", 23456: "nan"},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 30.5, 23456: 79.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: "nan", 23456: "nan"},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 39.4, 23456: 79.59}]
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
                    ConId=[
                        12345,
                        12345,
                        23456,
                        23456,
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
            securities = pd.DataFrame(dict(ConId=[12345,23456],
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_realtime_db_config', new=mock_get_realtime_db_config):
                    with patch('quantrocket.price.download_market_data_file', new=mock_download_market_data_file):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                            prices = get_prices("mexi-stk-tick-15min")

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])

        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            # Data was UTC but now Mexico_City
            [{'Date': '2018-04-01T00:00:00', 'Time': '14:30:00', 12345: 30.5, 23456: 79.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '14:30:00', 12345: 39.4, 23456: 79.59}]
        )

        last_counts = prices.loc["LastCount"]
        last_counts = last_counts.reset_index()
        last_counts.loc[:, "Date"] = last_counts.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_counts.fillna("nan").to_dict(orient="records"),
            # Data was UTC but now Mexico_City
            [{'Date': '2018-04-01T00:00:00', 'Time': '14:30:00', 12345: 305.0, 23456: 795.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '14:30:00', 12345: 940.0, 23456: 959.0}]
        )

    @patch("quantrocket.price.download_market_data_file")
    @patch("quantrocket.price.download_history_file")
    def test_apply_times_filter_to_history_vs_realtime_database(self,
                                                                mock_download_history_file,
                                                                mock_download_market_data_file):
        """
        Tests that the times filter is applied to a history database via the
        history query but is applied to the realtime database after
        converting to the exchange timezone.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "15 mins",
                "universes": ["usa-stk"],
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_get_realtime_db_config(db):
            return {
                "bar_size": "15 min",
                "fields": ["LastClose","LastOpen","LastHigh","LastLow", "VolumeClose"]
            }
        def _mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456,
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

        def _mock_download_market_data_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456,
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

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456],
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

        mock_download_history_file.side_effect = _mock_download_history_file
        mock_download_market_data_file.side_effect = _mock_download_market_data_file

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.get_realtime_db_config', new=mock_get_realtime_db_config):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                            prices = get_prices(
                                ["usa-stk-15min", "usa-stk-snapshot-15min"],
                                fields=["Close", "LastClose", "Volume"],
                                times=["09:30:00", "15:30:00"],
                            )

        self.assertEqual(len(mock_download_history_file.mock_calls), 1)
        history_call = mock_download_history_file.mock_calls[0]
        _, args, kwargs = history_call
        self.assertEqual(args[0], "usa-stk-15min")
        # only supported subset of fields is requested
        self.assertSetEqual(set(kwargs["fields"]), {"Close", "Volume"})
        # times filter was passed
        self.assertListEqual(kwargs["times"], ["09:30:00", "15:30:00"])

        self.assertEqual(len(mock_download_market_data_file.mock_calls), 1)
        realtime_call = mock_download_market_data_file.mock_calls[0]
        _, args, kwargs = realtime_call
        self.assertEqual(args[0], "usa-stk-snapshot-15min")
        # only supported subset of fields is requested
        self.assertListEqual(kwargs["fields"], ["LastClose"])
        # times filter not passed
        self.assertNotIn("times", list(kwargs.keys()))

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.fillna("nan").to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: 20.1, 23456: 50.5},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 20.5, 23456: 52.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: 19.4, 23456: 51.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 18.56, 23456: 54.23}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: 15000.0, 23456: 98000.0},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 7800.0, 23456: 179000.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: 12400.0, 23456: 142500.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 14500.0, 23456: 124000.0}]
        )

        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            # Data was UTC but now NY
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: 'nan', 23456: 'nan'},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 39.4, 23456: 79.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: 'nan', 23456: 'nan'},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 46.78, 23456: 81.56}]
        )

    def test_consolidate_overlapping_fields_and_respect_priority(self):
        """
        Tests that when querying two databases with overlapping
        dates/conids/fields, the value is taken from the db which was passed
        first as an argument.
        """
        def mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["nyse-stk"],
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_download_history_file(code, f, *args, **kwargs):
            if code == "usa-stk-1d":
                prices = pd.DataFrame(
                    dict(
                        ConId=[
                            12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
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
                        ConId=[
                            12345,
                            12345,
                            12345,
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                        # prioritize usa-stk-1d by passing first
                        prices = get_prices(["usa-stk-1d", "nyse-stk-1d"],
                                            fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.5},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 52.5},
             {'Date': '2018-04-03T00:00:00', 12345: 19.4, 23456: 51.59}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 15000, 23456: 98000},
             {'Date': '2018-04-02T00:00:00', 12345: 7800, 23456: 179000},
             {'Date': '2018-04-03T00:00:00', 12345: 12400, 23456: 142500}]
        )

        # repeat test but prioritize nyse-stk-1d by passing first
        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                        prices = get_prices(["nyse-stk-1d", "usa-stk-1d"],
                                            fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 5900.0, 23456: 50.5},
             {'Date': '2018-04-02T00:00:00', 12345: 5920.0, 23456: 52.5},
             {'Date': '2018-04-03T00:00:00', 12345: 5950.0, 23456: 51.59}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        # Since volume is null in nyse-stk-1d, we get the volume from usa-stk-1d
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 15000, 23456: 98000},
             {'Date': '2018-04-02T00:00:00', 12345: 7800, 23456: 179000},
             {'Date': '2018-04-03T00:00:00', 12345: 12400, 23456: 142500}]
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
                "vendor": "ib",
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
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456,
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
                    ConId=[
                        12345,
                        12345,
                        23456,
                        23456,
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
            securities = pd.DataFrame(dict(ConId=[12345,23456],
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
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: 20.1, 23456: 50.5},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 20.5, 23456: 52.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: 19.4, 23456: 51.59},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 18.56, 23456: 54.23}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: 15000.0, 23456: 98000.0},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 7800.0, 23456: 179000.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: 12400.0, 23456: 142500.0},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 14500.0, 23456: 124000.0}]
        )

        last_closes = prices.loc["LastClose"]
        last_closes = last_closes.reset_index()
        last_closes.loc[:, "Date"] = last_closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            last_closes.fillna("nan").to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 'Time': '09:30:00', 12345: "nan", 23456: "nan"},
             {'Date': '2018-04-01T00:00:00', 'Time': '15:30:00', 12345: 30.5, 23456: 79.5},
             {'Date': '2018-04-02T00:00:00', 'Time': '09:30:00', 12345: "nan", 23456: "nan"},
             {'Date': '2018-04-02T00:00:00', 'Time': '15:30:00', 12345: 39.4, 23456: 79.59}]
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
                    "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["japan-stk"],
                    "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_download_history_file(code, f, *args, **kwargs):
            if code == "usa-stk-1d":
                prices = pd.DataFrame(
                    dict(
                        ConId=[
                            12345,
                            12345,
                            12345,
                            23456,
                            23456,
                            23456,
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                        prices = get_prices(["usa-stk-1d", "japan-stk-1d"], fields=["Close", "Volume"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), [12345,23456])
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.5},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 52.5},
             {'Date': '2018-04-03T00:00:00', 12345: 19.4, 23456: 51.59}]
        )
        volumes = prices.loc["Volume"]
        volumes = volumes.reset_index()
        volumes.loc[:, "Date"] = volumes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 15000, 23456: 98000},
             {'Date': '2018-04-02T00:00:00', 12345: 7800, 23456: 179000},
             {'Date': '2018-04-03T00:00:00', 12345: 12400, 23456: 142500}]
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
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["japan-stk"],
                    "vendor": "ib",
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
                "vendor": "ib",
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):

                    with self.assertRaises(ParameterError) as cm:
                        get_prices(["asx-stk-1d"])

        self.assertIn((
           "no history or real-time aggregate databases called asx-stk-1d"
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
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "30 mins",
                    "universes": ["japan-stk"],
                    "vendor": "ib",
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):

                    with self.assertRaises(ParameterError) as cm:
                        get_prices(["usa-stk-1d", "japan-stk-15min"])

        self.assertIn((
            "all databases must contain same bar size but usa-stk-1d, japan-stk-15min have different "
            "bar sizes:"
            ), str(cm.exception))

    def test_complain_if_multiple_domains(self):
        """
        Tests error handling when multiple dbs are queried and they have
        different domains.
        """
        def mock_get_history_db_config(db):
            if db == "usa-stk-1d":
                return {
                    "bar_size": "1 day",
                    "universes": ["usa-stk"],
                    "vendor": "ib",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }
            else:
                return {
                    "bar_size": "1 day",
                    "universes": ["sharadar-stk"],
                    "vendor": "sharadar",
                    "domain": "sharadar",
                    "fields": ["Close","Open","High","Low", "Volume"]
                }

        def mock_list_history_databases():
            return [
                "usa-stk-1d",
                "japan-stk-1d",
                "usa-stk-15min",
                "demo-stk-1min",
                "sharadar-1d"
            ]

        def mock_list_realtime_databases():
            return {"demo-stk-taq": ["demo-stk-taq-1h"]}

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):

                    with self.assertRaises(ParameterError) as cm:
                        get_prices(["usa-stk-1d", "sharadar-1d"])

        self.assertIn((
            "all databases must use the same securities master domain but usa-stk-1d, sharadar-1d use different "
            "domains:"
            ), str(cm.exception))

    def test_intraday_pass_timezone(self):
        """
        Tests handling of an intraday db when a timezone is specified.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "30 mins",
                "universes": ["usa-stk"],
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456
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
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
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
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.15},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 51.59}]
        )
        closes_900 = closes.xs("09:00:00", level="Time")
        closes_900 = closes_900.reset_index()
        closes_900.loc[:, "Date"] = closes_900.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_900.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.25, 23456: 50.59},
             {'Date': '2018-04-02T00:00:00', 12345: 20.38, 23456: 51.67}]
        )

        volumes = prices.loc["Volume"]
        volumes_830 = volumes.xs("08:30:00", level="Time")
        volumes_830 = volumes_830.reset_index()
        volumes_830.loc[:, "Date"] = volumes_830.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes_830.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 1500.0, 23456: 9000.0},
             {'Date': '2018-04-02T00:00:00', 12345: 1400.0, 23456: 1400.0}]
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
                "vendor": "ib"
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456
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
            securities = pd.DataFrame(dict(ConId=[12345,23456],
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                            prices = get_prices(
                                "usa-stk-15min",
                                times=["09:30:00", "10:00:00"],
                                fields=["Close", "Volume"],
                                master_fields=["Timezone"]
                            )

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
        # even though we didn't ask for Timezone, it gets appended since it
        # was required
        self.assertSetEqual(
            set(prices.index.get_level_values("Field")),
            {"Close", "Volume", "Timezone"})
        self.assertSetEqual(
            set(prices.index.get_level_values("Time")),
            {"09:30:00", "10:00:00"})
        closes = prices.loc["Close"]
        closes_930 = closes.xs("09:30:00", level="Time")
        closes_930 = closes_930.reset_index()
        closes_930.loc[:, "Date"] = closes_930.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_930.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.15},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 51.59}]
        )
        closes_1000 = closes.xs("10:00:00", level="Time")
        closes_1000 = closes_1000.reset_index()
        closes_1000.loc[:, "Date"] = closes_1000.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_1000.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.25, 23456: 50.59},
             {'Date': '2018-04-02T00:00:00', 12345: 20.38, 23456: 51.67}]
        )

        volumes = prices.loc["Volume"]
        volumes_930 = volumes.xs("09:30:00", level="Time")
        volumes_930 = volumes_930.reset_index()
        volumes_930.loc[:, "Date"] = volumes_930.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes_930.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 1500.0, 23456: 9000.0},
             {'Date': '2018-04-02T00:00:00', 12345: 1400.0, 23456: 1400.0}]
        )

        timezones = prices.loc["Timezone"]
        timezones = timezones.reset_index()
        timezones.loc[:, "Date"] = timezones.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        # securities master fields are indexed to min date
        self.assertListEqual(
            timezones.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00',
              'Time': '09:30:00',
              12345: 'America/New_York',
              23456: 'America/New_York'}]
        )

    def test_intraday_infer_timezone_from_securities_but_dont_return_timezone(self):
        """
        Tests handling of an intraday db when a timezone is not specified and
        is therefore inferred from the securities master, but the Timezone
        field is not requested nor returned.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "30 mins",
                "universes": ["usa-stk"],
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456
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
            securities = pd.DataFrame(dict(ConId=[12345,23456],
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                            prices = get_prices(
                                "usa-stk-15min",
                                times=["09:30:00", "10:00:00"],
                                fields=["Close", "Volume"],
                            )

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
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
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.15},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 51.59}]
        )
        closes_1000 = closes.xs("10:00:00", level="Time")
        closes_1000 = closes_1000.reset_index()
        closes_1000.loc[:, "Date"] = closes_1000.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_1000.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.25, 23456: 50.59},
             {'Date': '2018-04-02T00:00:00', 12345: 20.38, 23456: 51.67}]
        )

        volumes = prices.loc["Volume"]
        volumes_930 = volumes.xs("09:30:00", level="Time")
        volumes_930 = volumes_930.reset_index()
        volumes_930.loc[:, "Date"] = volumes_930.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            volumes_930.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 1500.0, 23456: 9000.0},
             {'Date': '2018-04-02T00:00:00', 12345: 1400.0, 23456: 1400.0}]
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
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456
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
            securities = pd.DataFrame(dict(ConId=[12345,23456],
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

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
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
                        23456
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
            securities = pd.DataFrame(dict(ConId=[12345,23456],
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
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
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
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.15},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 51.59}]
        )
        closes_1000 = closes.xs("10:00:00", level="Time")
        closes_1000 = closes_1000.reset_index()
        closes_1000.loc[:, "Date"] = closes_1000.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_1000.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.25, 23456: 50.59},
             {'Date': '2018-04-02T00:00:00', 12345: 20.38, 23456: 51.67}]
        )

        # repeat with a different timezone
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
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345,23456])
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
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.15},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 51.59}]
        )
        closes_900 = closes.xs("09:00:00", level="Time")
        closes_900 = closes_900.reset_index()
        closes_900.loc[:, "Date"] = closes_900.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes_900.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.25, 23456: 50.59},
             {'Date': '2018-04-02T00:00:00', 12345: 20.38, 23456: 51.67}]
        )

    def test_cast_boolean_master_fields(self):
        """
        Tests that master fields Etf and Delisted are cast to boolean.
        """
        def mock_get_history_db_config(db):
            return {
                "bar_size": "1 day",
                "universes": ["usa-stk"],
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        23456,
                        23456,
                        23456,
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
                        ]
                ))
            prices.to_csv(f, index=False)

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(ConId=[12345,23456],
                                           Symbol=["ABC","DEF"],
                                           Delisted=[0, 1],
                                           Etf=[1, 0]))
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):
                        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

                            prices = get_prices("usa-stk-1d",
                                                fields=["Close", "Volume"],
                                                master_fields=["Symbol", "Delisted", "Etf"])

        self.assertListEqual(list(prices.index.names), ["Field", "Date"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz) # EOD is tz-naive
        self.assertListEqual(list(prices.columns), [12345,23456])
        self.assertSetEqual(
            set(prices.index.get_level_values("Field")),
            {"Close", "Symbol", "Delisted", "Etf"})
        closes = prices.loc["Close"]
        closes = closes.reset_index()
        closes.loc[:, "Date"] = closes.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            closes.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: 20.1, 23456: 50.5},
             {'Date': '2018-04-02T00:00:00', 12345: 20.5, 23456: 52.5},
             {'Date': '2018-04-03T00:00:00', 12345: 19.4, 23456: 51.59}]
        )
        symbols = prices.loc["Symbol"]
        symbols = symbols.reset_index()
        symbols.loc[:, "Date"] = symbols.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            symbols.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: "ABC", 23456: "DEF"}]
        )

        delisted = prices.loc["Delisted"]
        delisted = delisted.reset_index()
        delisted.loc[:, "Date"] = delisted.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            delisted.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: False, 23456: True}]
        )

        etfs = prices.loc["Etf"]
        etfs = etfs.reset_index()
        etfs.loc[:, "Date"] = etfs.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            etfs.to_dict(orient="records"),
            [{'Date': '2018-04-01T00:00:00', 12345: True, 23456: False}]
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
                "vendor": "ib",
                "fields": ["Close","Open","High","Low", "Volume"]
            }

        def mock_download_history_file(code, f, *args, **kwargs):
            prices = pd.DataFrame(
                dict(
                    ConId=[
                        12345,
                        12345,
                        12345,
                        12345,
                        12345,
                        12345,
                        12345,
                        12345,
                        12345
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

        with patch('quantrocket.price.list_realtime_databases', new=mock_list_realtime_databases):
            with patch('quantrocket.price.list_history_databases', new=mock_list_history_databases):
                with patch('quantrocket.price.get_history_db_config', new=mock_get_history_db_config):
                    with patch('quantrocket.price.download_history_file', new=mock_download_history_file):

                        prices = get_prices(
                            "usa-stk-2h",
                            timezone="America/New_York"
                        )

        self.assertListEqual(list(prices.index.names), ["Field", "Date", "Time"])
        self.assertEqual(prices.columns.name, "ConId")
        dt = prices.index.get_level_values("Date")
        self.assertTrue(isinstance(dt, pd.DatetimeIndex))
        self.assertIsNone(dt.tz)
        self.assertListEqual(list(prices.columns), [12345])
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
        closes = closes[12345]
        self.assertEqual(
            closes.xs("14:00:00", level="Time").loc["2018-04-02"], "nan"
        )
        self.assertEqual(
            closes.xs("12:00:00", level="Time").loc["2018-04-04"], "nan"
        )
        self.assertEqual(
            closes.xs("14:00:00", level="Time").loc["2018-04-04"], "nan"
        )
