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
from quantrocket.cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs

def execute_command(cmd, return_file=None, filepath_or_buffer=None,
                    params=None,
                    service="satellite"):
    """
    Execute a Python function or abitrary shell command on a satellite service.

    Parameters
    ----------
    cmd: str, required
        the shell command to run, or the Python function in dot notation (must
        start with "codeload." to be interpreted as a Python function).

    return_file : str, optional
        the path of a file to be returned after the command completes

    filepath_or_buffer : str, optional
        the location to write the return_file (omit to write to stdout)

    params : dict of PARAM:VALUE, optional
        one or more params to pass to the Python function (pass as {param:value})

    service : str, optional
        the service name (default 'satellite')

    Returns
    -------
    dict or None
        None if return_file, otherwise status message. If cmd uses Python dot
        notation and the Python function returns a value, it will be included in
        the status message as the "output" key. Return values must be JSON-serializable.

    Examples
    --------
    Run a Python function called 'create_calendar_spread' defined in '/codeload/scripts/combos.py'
    and pass it arguments:

    >>> execute_command("codeload.scripts.combos.create_calendar_spread",
                        params={"universe":"cl-fut", "contract_months":[1,2]})

    Run a Python function called 'calculate_signal' defined in '/codeload/scripts/custom.py'
    and retrieve the return value:

    >>> response = execute_command("codeload.scripts.custom.calculate_signal")
    >>> if response["status"] == "success":
            print(response["output"])

    Run a backtrader backtest and save the performance chart to file:

    >>> execute_command("python /codeload/backtrader/dual_moving_average.py",
                        return_file="/tmp/backtrader-plot.pdf"
                        outfile="backtrader-plot.pdf")
    """
    _params = {}
    if not service:
        raise ValueError("a service is required")
    if not cmd:
        raise ValueError("a command is required")
    _params["cmd"] = cmd
    if params:
        _params["params"] = dict_to_dict_strs(params)
    if return_file:
        _params["return_file"] = return_file

    if not service.startswith("satellite"):
        raise ValueError("service must start with 'satellite'")

    response = houston.post("/{0}/commands".format(service), params=_params, timeout=60*60*24)

    houston.raise_for_status_with_json(response)

    if return_file:
        filepath_or_buffer = filepath_or_buffer or sys.stdout
        write_response_to_filepath_or_buffer(filepath_or_buffer, response)
    else:
        return response.json()

def _cli_execute_command(*args, **kwargs):
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(execute_command, *args, **kwargs)
