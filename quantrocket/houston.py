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

import os
import requests
from .exceptions import ImproperlyConfigured

class Houston(requests.Session):

    DEFAULT_TIMEOUT = 5

    def __init__(self):
        super(Houston, self).__init__()
        if "HOUSTON_USERNAME" in os.environ and "HOUSTON_PASSWORD" in os.environ:
            self.auth = (os.environ["HOUSTON_USERNAME"], os.environ["HOUSTON_PASSWORD"])

    @property
    def base_url(self):
        if "HOUSTON_URL" not in os.environ:
            raise ImproperlyConfigured("HOUSTON_URL is not set")
        return os.environ["HOUSTON_URL"]

    def request(self, method, url, *args, **kwargs):
        if url.startswith('/'):
            url = self.base_url + url
        timeout = kwargs.get("timeout", None)
        if timeout is None:
            kwargs["timeout"] = self.DEFAULT_TIMEOUT
        return super(Houston, self).request(method, url, *args, **kwargs)

# Instantiate houston so that all callers can share a TCP connection (for
# performance's sake)
houston = Houston()
