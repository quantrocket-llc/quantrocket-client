# Copyright 2017-2024 QuantRocket LLC - All Rights Reserved
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

import argparse

class HelpFormatter(argparse.RawDescriptionHelpFormatter):

    def format_help(self):
        import re
        help_text = super().format_help()
        # Strip the RST code blocks directives which are used for doc
        # generation but not desired for end-user viewing
        help_text = help_text.replace(".. code-block:: bash\n\n", "")

        # show URLs in blue, underlined, unless they're in a code block
        BLUE = "\033[4;94m"
        NO_COLOR = "\033[0m"
        urls_except_in_code_blocks = r"^(?!\s{4}quantrocket)(?:.*)\b(https?://[^\s]+)"
        help_text = re.sub(urls_except_in_code_blocks, lambda match: match.group(0).replace(match.group(1), BLUE + match.group(1) + NO_COLOR), help_text, flags=re.MULTILINE)

        # show code examples in white text with black background
        CODE_BLOCK = "\033[37;40m"
        pattern = r'^(\n {4}quantrocket.*)' # <4 spaces>quantrocket ...
        help_text = re.sub(pattern, lambda match: CODE_BLOCK +"\n" + match.group() + "\n" + NO_COLOR, help_text, flags=re.MULTILINE)

        return help_text

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