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
import six
import requests
from .exceptions import ImproperlyConfigured, CannotConnectToHouston
from quantrocket.cli.utils.output import json_to_cli

CANNOT_CONNECT_TO_HOUSTON_ERROR_LOCAL = """{error}

Could not connect to houston at {scheme}://{netloc}

Please verify that houston is running and bound to port {port} by checking the output of:

    docker ps

The output should resemble:

    quantrocket/houston ... 0.0.0.0:{port}->80/tcp ... quantrocket_houston_1
    ...

If not running you can deploy it with:

    docker-compose -p quantrocket up -d
"""

CANNOT_CONNECT_TO_HOUSTON_ERROR_CLOUD = """{error}

Could not connect to houston at {scheme}://{netloc}

Please verify that the URL is correct and houston is running.
"""

def _get_force_timeout():
    force_timeout = os.environ.get("QUANTROCKET_TIMEOUT", None)
    if not force_timeout:
        return

    try:
        return int(force_timeout)
    except:
        return None

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
        self.force_timeout = _get_force_timeout()

    @property
    def base_url(self):
        if "HOUSTON_URL" not in os.environ:
            raise ImproperlyConfigured("""HOUSTON_URL is not set

--------------------------------------------------------------------------------
Please set HOUSTON_URL environment variable.

For local deployments: http://localhost:1969

--------------------
|  Windows syntax  |
--------------------

To set the environment variable on Windows, run:

    [Environment]::SetEnvironmentVariable("HOUSTON_URL", "http://localhost:1969", "User")

IMPORTANT: you must close and re-open PowerShell for the environment variable to take effect!

--------------------
|    Mac syntax    |
--------------------

To set the environment variable on Mac, run:

    touch ~/.profile
    echo 'export HOUSTON_URL=http://localhost:1969' >> ~/.profile
    source ~/.profile

--------------------
|   Linux syntax   |
--------------------

To set the environment variable on Linux, run:

    touch ~/.bashrc
    echo 'export HOUSTON_URL=http://localhost:1969' >> ~/.bashrc
    source ~/.bashrc
""")
        return os.environ["HOUSTON_URL"]

    def request(self, method, url, *args, **kwargs):
        if url.startswith('/'):
            url = self.base_url + url
        timeout = kwargs.get("timeout", None)
        stream = kwargs.get("stream", None)
        if not stream:
            # Use QUANTROCKET_TIMEOUT if set, else the requested
            # timeout, else the default timeout
            if self.force_timeout:
                kwargs["timeout"] = self.force_timeout
            elif timeout is None:
                kwargs["timeout"] = self.DEFAULT_TIMEOUT

        # Move params to data if too long
        for param_name, param_vals in kwargs.get("params", {}).copy().items():
            if isinstance(param_vals, list) and len(param_vals) > 50:
                data = kwargs.get("data", {}) or {}
                data[param_name] = param_vals
                kwargs["params"].pop(param_name)
                kwargs["data"] = data

        try:
            return super(Houston, self).request(method, url, *args, **kwargs)
        except requests.ConnectionError as error:
            if "Failed to establish a new connection" not in str(error):
                raise

            parsed = six.moves.urllib.parse.urlparse(error.request.url)

            if parsed.hostname == "houston" and parsed.port in (None, 80):
                # don't do anything special within containers
                raise

            if parsed.port == 443:
                raise CannotConnectToHouston(CANNOT_CONNECT_TO_HOUSTON_ERROR_CLOUD.format(
                    error=error,
                    scheme=parsed.scheme,
                    netloc=parsed.netloc
                ))

            raise CannotConnectToHouston(CANNOT_CONNECT_TO_HOUSTON_ERROR_LOCAL.format(
                error=error,
                scheme=parsed.scheme,
                netloc=parsed.netloc,
                port=parsed.port
            ))


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
                e.args = e.args + ("please check the logs for more details",)
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