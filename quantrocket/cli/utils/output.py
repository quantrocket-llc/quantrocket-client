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
import yaml
import json
import requests

def json_to_cli(func, *args, **kwargs):
    """
    Converts a JSON response to a more appropriate CLI response.

    If JSON is preferred, the response will be returned as-is. Otherwise:

    - If the JSON is a list of scalars, the output will be simplified to a
    string of newline-separated values suitable for the command line (unless
    simplify_lists is False).

    - If the JSON is empty, nothing will be returned.

    - YAML will be returned.
    """
    exit_code = 0
    simplify_list = kwargs.pop("simplify_list", True)
    try:
        json_response = func(*args, **kwargs)
    except requests.exceptions.HTTPError as e:
        # use json response from service, if available
        json_response = getattr(e, "json_response", {}) or {"status": "error", "msg": repr(e)}
        exit_code = 1
    if not json_response:
        return None, exit_code
    if os.environ.get("QUANTROCKET_CLI_OUTPUT_FORMAT", "").lower() == "json":
        return json.dumps(json_response), exit_code
    if simplify_list and isinstance(json_response, list) and not any([
        isinstance(item, (dict, list, tuple, set)) for item in json_response]):
        return "\n".join([str(item) for item in json_response]), exit_code
    return yaml.safe_dump(json_response, default_flow_style=False).strip(), exit_code
