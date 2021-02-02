# Copyright 2017 QuantRocket - All Rights Reserved
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

import requests

class ImproperlyConfigured(Exception):
    pass

class CannotConnectToHouston(Exception):
    pass

class ParameterError(ValueError):
    pass

class MissingData(ValueError):
    pass

class NoData(requests.HTTPError):

    def __init__(self, e):
        if isinstance(e, requests.HTTPError):
            self.__dict__ = e.__dict__
            super(NoData, self).__init__(e.args)
        else:
            super(NoData, self).__init__(e)


class NoHistoricalData(NoData):
    pass

class NoRealtimeData(NoData):
    pass

class NoFundamentalData(NoData):
    pass

class DataInsertionError(Exception):
    pass
