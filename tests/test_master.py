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
from quantrocket.master import get_securities_reindexed_like

class SecuritiesReindexedLikeTestCase(unittest.TestCase):

    @patch("quantrocket.master.download_master_file")
    def test_pass_conids_and_domain_based_on_reindex_like(self, mock_download_master_file):
        """
        Tests that conids and domain are correctly passed to the download_master_file
        function based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(ConId=[12345,
                            23456],
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

        get_securities_reindexed_like(closes, domain="main", fields=["Symbol", "Etf", "Delisted", "Currency"])

        download_master_file_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = download_master_file_call
        self.assertListEqual(kwargs["conids"], [12345,23456])
        self.assertEqual(kwargs["domain"], "main")
        self.assertListEqual(kwargs["fields"], ["Symbol", "Etf", "Delisted", "Currency"])

    def test_securities_reindexed_like(self):
        """
        Tests get_securities_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=[12345,23456],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(ConId=[12345,
                            23456],
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
                domain="main",
                fields=["Symbol", "Etf", "Delisted", "Currency"])

            securities = securities.reset_index()
            securities.loc[:, "Date"] = securities.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                securities.to_dict(orient="records"),
                [{'Field': 'Currency',
                  'Date': '2018-05-01T00:00:00-0400',
                  12345: 'USD',
                  23456: 'EUR'},
                 {'Field': 'Currency',
                  'Date': '2018-05-02T00:00:00-0400',
                  12345: 'USD',
                  23456: 'EUR'},
                 {'Field': 'Currency',
                  'Date': '2018-05-03T00:00:00-0400',
                  12345: 'USD',
                  23456: 'EUR'},
                 {'Field': 'Delisted',
                  'Date': '2018-05-01T00:00:00-0400',
                  12345: False,
                  23456: True},
                 {'Field': 'Delisted',
                  'Date': '2018-05-02T00:00:00-0400',
                  12345: False,
                  23456: True},
                 {'Field': 'Delisted',
                  'Date': '2018-05-03T00:00:00-0400',
                  12345: False,
                  23456: True},
                 {'Field': 'Etf',
                  'Date': '2018-05-01T00:00:00-0400',
                  12345: True,
                  23456: False},
                 {'Field': 'Etf',
                  'Date': '2018-05-02T00:00:00-0400',
                  12345: True,
                  23456: False},
                 {'Field': 'Etf',
                  'Date': '2018-05-03T00:00:00-0400',
                  12345: True,
                  23456: False},
                 {'Field': 'Symbol',
                  'Date': '2018-05-01T00:00:00-0400',
                  12345: 'ABC',
                  23456: 'DEF'},
                 {'Field': 'Symbol',
                  'Date': '2018-05-02T00:00:00-0400',
                  12345: 'ABC',
                  23456: 'DEF'},
                 {'Field': 'Symbol',
                  'Date': '2018-05-03T00:00:00-0400',
                  12345: 'ABC',
                  23456: 'DEF'}]
            )

