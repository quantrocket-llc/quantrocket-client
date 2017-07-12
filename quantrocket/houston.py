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
from quantrocket.cli.utils.output import json_to_cli

class Houston(requests.Session):
    """
    Subclass of `requests.Session` that provides an interface to the houston
    API gateway. Reads HOUSTON_URL (and Basic Auth credentials if applicable)
    from environment variables and applies them to each request. Simply provide
    the path, starting with /, for example:

    >>> response = houston.get("/countdown/crontab")

    Since each instance of Houston is a session, you can improve performance
    by using a single session for all requests. The module provides an instance
    of `Houston`, named `houston`.

    Use the same session as other requests:

    >>> from quantrocket.houston import houston

    Use a new session:

    >>> from quantrocket.houston import Houston
    >>> houston = Houston()
    """

    DEFAULT_TIMEOUT = 30

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
        stream = kwargs.get("stream", None)
        if timeout is None and not stream:
            kwargs["timeout"] = self.DEFAULT_TIMEOUT
        return super(Houston, self).request(method, url, *args, **kwargs)

    @staticmethod
    def raise_for_status_with_json(response):
        """
        Raises 400/500 error codes, attaching a json response to the
        exception, if possible.
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            try:
                e.json_response = response.json()
                e.args = e.args + (e.json_response,)
            except:
                e.json_response = {}
            raise e

# Instantiate houston so that all callers can share a TCP connection (for
# performance's sake)
houston = Houston()

def ping():
    """
    Pings houston.

    Returns
    -------
    json
        reply from houston
    """
    response = houston.get("/ping")
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_ping():
    return json_to_cli(ping)