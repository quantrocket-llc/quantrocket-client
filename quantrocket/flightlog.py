#!/usr/bin/env python
#
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

import logging, logging.handlers
import socket
import six
import sys
import os
from six.moves import queue, urllib
from .exceptions import ImproperlyConfigured
from .houston import Houston, houston
from quantrocket.cli.utils.output import json_to_cli

FLIGHTLOG_PATH = "/flightlog/handler"

LOG_RECORD_TYPE_BOUNDARY = "||||q5%XfK4#||||"

class _ImpatientHttpHandler(logging.handlers.HTTPHandler):
    """
    An HttpHandler that sets a short timeout (instead of the default no
    timeout), as well as serializing the record attribute types so they can
    be reconstituted.
    """

    def mapLogRecord(self, record):
        dict_with_types = {}
        for k, v in record.__dict__.items():
            type_name = type(v).__name__
            dict_with_types[k] = "{0}{1}{2}".format(
                type_name, LOG_RECORD_TYPE_BOUNDARY, v)
        return dict_with_types

    def emit(self, record):
        orig_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(3)
        super(_ImpatientHttpHandler, self).emit(record)
        socket.setdefaulttimeout(orig_timeout)

def FlightlogHandler(background=None):
    """
    Returns a log handler that logs to flightlog.

    Parameters
    ----------
    background : bool
        If True, causes logging to happen in a background thread so that logging
        doesn't block. Background logging requires Python 3.2 or higher,
        and defaults to True for supported versions and False otherwise.

    Returns
    -------
    `logging.handlers.QueueHandler` or `quantrocket.flightlog._ImpatientHttpHandler`

    Examples
    --------
    Log a message using the FlightlogHandler:

    >>> import logging
    >>> from quantrocket.flightlog import FlightlogHandler
    >>> logger = logging.getLogger('myapp')
    >>> logger.setLevel(logging.DEBUG)
    >>> handler = FlightlogHandler()
    >>> logger.addHandler(handler)
    >>> logger.info('my app just opened a position')
    """
    base_url = os.environ.get("HOUSTON_URL", None)
    if not base_url:
        raise ImproperlyConfigured("HOUSTON_URL is not set")
    parsed = urllib.parse.urlparse(base_url)
    secure = parsed.scheme == "https"
    if "HOUSTON_USERNAME" in os.environ and "HOUSTON_PASSWORD" in os.environ:
        credentials = (os.environ["HOUSTON_USERNAME"], os.environ["HOUSTON_PASSWORD"])
    else:
        credentials = None

    path = os.environ.get("FLIGHTLOG_PATH") or FLIGHTLOG_PATH

    if six.PY2:
        if secure:
            raise NotImplementedError("Logging to Flightlog over HTTPS requires Python 3")
        if credentials:
            raise NotImplementedError("Logging to Flightlog using Basic Auth requires Python 3")

    if six.PY2:
        http_handler = _ImpatientHttpHandler(
            parsed.netloc, path, method="POST")
    else:
        http_handler = _ImpatientHttpHandler(
            parsed.netloc, path, method="POST", secure=secure, credentials=credentials)

    if six.PY2 or sys.version_info.minor < 2:
        if background:
            import warnings
            warnings.warn('Background logging requires Python 3.2 or higher. Logging in foreground...')
        background = False
    elif background is None:
        background = True

    if background:
        log_queue = queue.Queue(-1)  # no limit on size
        queue_handler = logging.handlers.QueueHandler(log_queue)
        listener = logging.handlers.QueueListener(log_queue, http_handler)
        listener.start()
        return queue_handler
    else:
        return http_handler

def _cli_log_message(msg, logger_name=None, level="INFO"):
    """
    Log a single message to Flightlog. Intended for CLI usage. Calling this
    function multiple times within the same process will configure duplicate
    handlers and result in duplicate messages.
    """
    logger = logging.getLogger(logger_name)
    levelnum = logging.getLevelName(level.upper())
    try:
        int(levelnum)
    except ValueError:
        raise ValueError("level must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL")

    handler = FlightlogHandler(background=False)
    logger.addHandler(handler)
    logger.setLevel(levelnum)
    if msg == "-":
        msg = sys.stdin.read()
    for line in msg.splitlines():
        if line:
            logger.log(levelnum, line)

    exit_code = 0
    return None, exit_code

def stream_logs(detail=False, hist=None, color=True):
    """
    Stream application logs, `tail -f` style.

    Parameters
    ----------
    detail : bool
        if True, show detailed logs from logspout, otherwise show log messages
        from flightlog only (default False)

    hist : int, optional
        number of log lines to show right away (ignored if showing detailed logs)

    color : bool
        colorize the logs

    Yields
    -------
    str
        each log line as it arrives
    """
    params = {}
    if detail:
        path = "/logspout/logs"
        if not color:
            params["colors"] = "off"
    else:
        path = "/flightlog/stream/logs"
        if hist:
            params['hist'] = hist
        if not color:
            params["nocolor"] = "true"

    houston = Houston()
    response = houston.get(path, stream=True, params=params)
    houston.raise_for_status_with_json(response)
    try:
        for line in response.iter_lines():
            if six.PY3:
                line = line.decode("utf-8")
            yield line
    except KeyboardInterrupt:
        houston.close()
        return

def _cli_print_stream(*args, **kwargs):
    generator = stream_logs(*args, **kwargs)
    for chunk in generator:
        try:
            # disable output buffering using flush to allow grepping
            # stream (flush arg not available before Python 3.3);
            # called via exec in order to catch SyntaxError on Python 2
            exec("print(chunk, flush=True)")
        except (SyntaxError, TypeError):
            print(chunk)

def _cli_stream_logs(*args, **kwargs):
    return json_to_cli(_cli_print_stream, *args, **kwargs)

def download_logfile(outfile, detail=False, match=None):
    """
    Download the logfile.

    Parameters
    ----------
    outfile: str, required
        filename to write the logfile to

    detail : bool
        if True, show detailed logs from logspout, otherwise show log messages
        from flightlog only (default False)

    match : str, optional
        filter the logfile to lines containing this string

    Returns
    -------
    None
    """
    if detail:
        logtype = "system"
    else:
        logtype = "app"

    params = {}
    if match:
        params["match"] = match

    response = houston.get("/flightlog/logfile/{0}".format(logtype), params=params, stream=True)
    houston.raise_for_status_with_json(response)

    if response.status_code == 204:
        return response.json()

    with open(outfile, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

def _cli_download_logfile(*args, **kwargs):
    return json_to_cli(download_logfile, *args, **kwargs)

def get_timezone():
    """
    Return the flightlog timezone.

    Returns
    -------
    dict
        dict with key timezone
    """
    response = houston.get("/flightlog/timezone")
    houston.raise_for_status_with_json(response)
    return response.json()

def set_timezone(tz):
    """
    Set the flightlog timezone.

    Parameters
    ----------
    tz : str, required
        the timezone to set (pass a partial timezone string such as 'newyork'
        or 'europe' to see close matches, or pass '?' to see all choices)

    Returns
    -------
    dict
        status message

    Examples
    --------
    Set the flightlog timezone to America/New_York:

    >>> set_timezone("America/New_York")
    """
    params = {"tz": tz}
    response = houston.put("/flightlog/timezone", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_timezone(tz=None, *args, **kwargs):
    if tz:
        return json_to_cli(set_timezone, tz, *args, **kwargs)
    else:
        return json_to_cli(get_timezone, *args, **kwargs)

def get_papertrail_config():
    """
    Return the current Papertrail log configuration, if any.

    See http://qrok.it/h/pt to learn more.

    Returns
    -------
    dict
        config details
    """
    response = houston.get("/flightlog/papertrail")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_papertrail_config(host, port):
    """
    Set the Papertrail log configuration.

    See http://qrok.it/h/pt to learn more.

    Parameters
    ----------
    host : str, required
        the Papertrail host to log to

    port : int, required
        the Papertrail port to log to

    Returns
    -------
    dict
        status message

    Examples
    --------
    Configure flightlog to log to Papertrail:

    >>> set_papertrail_config("logs.papertrailapp.com", 55555)
    """
    params = {"host": host, "port": port}
    response = houston.put("/flightlog/papertrail", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_papertrail_config(host=None, port=None, *args, **kwargs):
    if host or port:
        return json_to_cli(set_papertrail_config, host, port, *args, **kwargs)
    else:
        return json_to_cli(get_papertrail_config, *args, **kwargs)
