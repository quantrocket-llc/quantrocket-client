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

import sys
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer

def execute_command(cmd, return_file=None, filepath_or_buffer=None, service="satellite"):
    """
    Execute an abitrary command on a satellite service and optionally return a file.

    Parameters
    ----------
    cmd: str, required
        the command to run

    return_file : str, optional
        the path of a file to be returned after the command completes

    filepath_or_buffer : str, optional
        the location to write the return_file (omit to write to stdout)

    service : str, optional
        the service name (default 'satellite')

    Returns
    -------
    dict or None
        None if return_file, otherwise status message
    """
    params = {}
    if not service:
        raise ValueError("a service is required")
    if not cmd:
        raise ValueError("a command is required")
    params["cmd"] = cmd
    if return_file:
        params["return_file"] = return_file

    if not service.startswith("satellite"):
        raise ValueError("service must start with 'satellite'")

    response = houston.post("/{0}/commands".format(service), params=params, timeout=60*60*24)

    houston.raise_for_status_with_json(response)

    if return_file:
        filepath_or_buffer = filepath_or_buffer or sys.stdout
        write_response_to_filepath_or_buffer(filepath_or_buffer, response)
    else:
        return response.json()

def _cli_execute_command(*args, **kwargs):
    return json_to_cli(execute_command, *args, **kwargs)
