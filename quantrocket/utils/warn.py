# Copyright 2018 QuantRocket LLC - All Rights Reserved
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

from functools import wraps
import warnings

def deprecated_replaced_by(new, old_name=None):
    """
    Function that returns a decorator that applies a DeprecationWarning to
    the wrapped function.

    Parameters
    ----------
    new : func or str, required
        the function, or name of the function, that replaces the deprecated
        function

    old_name : str, optional
        the name of the deprecated function (defaults to func.__name__)
    """
    new_name = getattr(new, "__name__", new)

    def decorator(func):

        @wraps(func)
        def wrapped(*args, **kwargs):
            # DeprecationWarning is ignored by default but we want the user
            # to see it
            warnings.simplefilter("always", DeprecationWarning)
            warnings.warn(
                "`{0}` is deprecated and will be removed in a "
                "future release, please use `{1}` instead".format(
                    old_name or func.__name__, new_name),
                DeprecationWarning)
            return func(*args, **kwargs)

        return wrapped

    return decorator
