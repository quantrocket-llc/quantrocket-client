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

def print_stream(func):
    """
    Decorator that returns a function which prints the chunks returned by the
    wrapped func.
    """
    def wrapper(*args, **kwargs):
        generator = func(*args, **kwargs)
        for chunk in generator:
            try:
                # disable output buffering using flush to allow grepping
                # stream (flush arg not available before Python 3.3)
                print(chunk, flush=True)
            except TypeError:
                print(chunk)

    return wrapper

def to_bytes(f):
    """
    Yields the file line-by-line, converting str to bytes.
    """
    for line in f:
        if six.PY3 and isinstance(line, six.string_types):
            line = line.encode("utf-8")
        yield line
