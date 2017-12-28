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

from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli

def get_license_profile(force_refresh=False):
    """
    Return the current license profile.

    Parameters
    ----------
    force_refresh : bool
        refresh the license profile before returning it (default is to
        return the cached profile, which is refreshed every few minutes)

    Returns
    -------
    dict
        license profile
    """
    params = {}
    if force_refresh:
        params["force_refresh"] = force_refresh

    response = houston.get("/license-service/license", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_license_profile(*args, **kwargs):
    return json_to_cli(get_license_profile, *args, **kwargs)
