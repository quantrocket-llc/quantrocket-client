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
from quantrocket.utils import segmented_date_range

class DateUtilsTestCase(unittest.TestCase):
    """
    Test cases for `quantrocket.utils.segment_date_range`.
    """

    def test_segmented_date_range(self):
        segments = segmented_date_range("2010-01-01", "2018-01-01")
        self.assertListEqual(
            segments,
            [('2010-01-01', '2010-12-30'),
             ('2010-12-31', '2011-12-30'),
             ('2011-12-31', '2012-12-30'),
             ('2012-12-31', '2013-12-30'),
             ('2013-12-31', '2014-12-30'),
             ('2014-12-31', '2015-12-30'),
             ('2015-12-31', '2016-12-30'),
             ('2016-12-31', '2017-12-30'),
             ('2017-12-31', '2018-01-01')]
        )

        segments = segmented_date_range("2010-06-15","2014-04-05", segment="A")
        self.assertListEqual(
            segments,
            [('2010-06-15', '2010-12-30'),
             ('2010-12-31', '2011-12-30'),
             ('2011-12-31', '2012-12-30'),
             ('2012-12-31', '2013-12-30'),
             ('2013-12-31', '2014-04-05')]
        )

        segments = segmented_date_range("2010-06-15","2014-04-05", segment="2A")
        self.assertListEqual(
            segments,
            [('2010-06-15', '2010-12-30'),
             ('2010-12-31', '2012-12-30'),
             ('2012-12-31', '2014-04-05')]
        )

        segments = segmented_date_range("2010-01-01","2014-01-01", segment="6M")
        self.assertListEqual(
            segments,
            [('2010-01-01', '2010-01-30'),
             ('2010-01-31', '2010-07-30'),
             ('2010-07-31', '2011-01-30'),
             ('2011-01-31', '2011-07-30'),
             ('2011-07-31', '2012-01-30'),
             ('2012-01-31', '2012-07-30'),
             ('2012-07-31', '2013-01-30'),
             ('2013-01-31', '2013-07-30'),
             ('2013-07-31', '2014-01-01')]
        )
