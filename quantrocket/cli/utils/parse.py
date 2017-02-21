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

import dateutil.parser

def parse_date(datestr):
    return dateutil.parser.parse(datestr).date()

def parse_datetime(datetimestr):
    return dateutil.parser.parse(datetimestr)

def parse_date_offset(offset):
    """
    Calculates a date with the specified offset from today. Positive offsets
    are in the future, negative offsets are in the past.
    """
    return datetime.date.today() + datetime.timedelta(days=int(offset))

def list_or_int_or_float_or_str(value):
    """
    Parses the value as an int, else a float, else a string. If the value
    contains commas, treats it as a list of ints or floats or strings.
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

def parse_dict(dict_str):
    """
    Parses a key:value string into a dict.
    """
    k, v = dict_str.split(":", 1)
    return {k:list_or_int_or_float_or_str(v)}
