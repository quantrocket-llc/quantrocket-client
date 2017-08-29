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

import six
import sys

def write_response_to_filepath_or_buffer(filepath_or_buffer, response):
    """
    Writes the response content to the filepath or buffer.
    """
    if hasattr(filepath_or_buffer, "write"):
        if six.PY3 and filepath_or_buffer is sys.stdout:
            # Write bytes to stdout (https://stackoverflow.com/a/23932488)
            filepath_or_buffer = filepath_or_buffer.buffer
        mode = getattr(filepath_or_buffer, "mode", "w")
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                if "b" not in mode and six.PY3:
                    chunk = chunk.decode("utf-8")
                filepath_or_buffer.write(chunk)
        if filepath_or_buffer.seekable():
            filepath_or_buffer.seek(0)
    else:
        with open(filepath_or_buffer, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
