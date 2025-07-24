# Copyright 2021 Agnostiq Inc.
# Copyright 2025 National Institute of Advanced Industrial Science and Technology.
#
# This file is part of Covalent.
#
# Licensed under the Apache License 2.0 (the "License"). A copy of the
# License may be obtained with this software package or at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Use of this file is prohibited except in compliance with the License.
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This script executes the electron function on the PBS Professional cluster."""

import sys

import cloudpickle as pickle


def _execute(func_filename: str) -> dict:
    """Load and execute the @electron function"""
    with open(func_filename, "rb") as f:
        function, args, kwargs = pickle.load(f)

    result = None
    exception = None

    try:
        result = function(*args, **kwargs)
    except Exception as err:
        exception = err

    return {
        "result": result,
        "exception": exception,
    }


def _record_output(result, exception, result_filename) -> None:
    """Record the output of the @electron function"""

    with open(result_filename, "wb") as f:
        pickle.dump((result, exception), f)


def main():
    """Execute the @electron function on the PBS Professional cluster."""

    func_filename = sys.argv[1]
    output_data = {
        "result": None,
        "exception": None,
        "result_filename": sys.argv[2],
    }

    # execute the function
    try:
        output_data.update(**_execute(func_filename))
    except Exception as err:
        output_data.update(exception=err)

    # record the output
    _record_output(**output_data)


if __name__ == "__main__":
    main()
