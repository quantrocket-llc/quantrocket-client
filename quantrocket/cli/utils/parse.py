# Copyright 2017 QuantRocket LLC - All Rights Reserved
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

def list_or_int_or_float_or_str(value):
    """
    Parses the value as an int, else a float, else a string. If the value
    contains commas, treats it as a list of ints or floats or strings. Also
    handles None, True, and False.
    """
    if "," in value:
        return [list_or_int_or_float_or_str(item) for item in value.split(",")]

    special_vals = {
        "None": None,
        "True": True,
        "False": False
    }
    if value in special_vals:
        return special_vals[value]

    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return str(value)

def dict_str(value):
    if ":" not in value:
        raise ValueError("value {0} should have format key:value".format(value))
    return value

def dict_strs_to_dict(*dict_strs):
    """
    Parses a list of key:value strings into a dict. Used to go from CLI->Python.
    """
    d = {}
    for dict_str in dict_strs:
        try:
            k, v = dict_str.split(":", 1)
        except ValueError:
            raise ValueError("can't parse string '{0}': must have format 'key:value'".format(
                dict_str))
        d[k] = v
    return d

def dict_to_dict_strs(d):
    """
    Parses a dict to a list of key:value strings. Used to go from Python->HTTP.
    """
    return ["{0}:{1}".format(k,v) for k,v in d.items()]