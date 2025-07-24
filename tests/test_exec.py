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

"""Tests for the @electron task execution script that runs on the PBS Professional cluster"""

import sys
import tempfile
from pathlib import Path

import cloudpickle as pickle
import pytest

from covalent_pbspro_plugin import exec as exec_script


@pytest.fixture(scope="function")
def pkl_file():
    """make temporary pkl file and finally delete.

    Yields:
        Generator[Path, Any, None]: file path of the temporary file
    """
    path = Path(tempfile.NamedTemporaryFile(suffix=".pkl", delete=False).name)
    yield str(path)

    # delete temporary file
    path.unlink(missing_ok=True)


def test_execute(pkl_file):
    """Test _execute correctly reports the result"""

    def mock_electron_function(x, y):
        return x + y

    args = (1,)
    kwargs = {"y": 2}

    with open(pkl_file, "wb") as f:
        pickle.dump((mock_electron_function, args, kwargs), f)

    actual_result = exec_script._execute(pkl_file)

    assert actual_result["result"] == 3
    assert actual_result["exception"] is None


def test_execute_with_error(pkl_file):
    """Test _execute correctly reports the error of electron function"""

    def mock_electron_function(x, y):

        raise RuntimeError("Mock error")

    args = (1,)
    kwargs = {"y": 2}

    with open(pkl_file, "wb") as f:
        pickle.dump((mock_electron_function, args, kwargs), f)

    actual_result = exec_script._execute(pkl_file)

    assert actual_result["result"] is None
    assert isinstance(actual_result["exception"], RuntimeError)
    assert str(actual_result["exception"]) == "Mock error"


def test_record_output(pkl_file):
    """Test _record_output correctly writes the result to the output file"""

    mock_result = 123
    mock_exception = None
    exec_script._record_output(mock_result, mock_exception, pkl_file)

    with open(pkl_file, "rb") as f:
        contents = f.read()
        actual_result, actual_exception = pickle.loads(contents)
    assert actual_result == mock_result
    assert actual_exception is None


def test_record_output_with_error(pkl_file):
    """Test _record_output correctly writes an error to the output file"""

    mock_result = None
    mock_exception = RuntimeError("Mock error")
    exec_script._record_output(mock_result, mock_exception, pkl_file)

    with open(pkl_file, "rb") as f:
        contents = f.read()
        actual_result, actual_exception = pickle.loads(contents)
    assert actual_result is None
    assert isinstance(actual_exception, RuntimeError)
    assert str(actual_exception) == "Mock error"


def test_main(mocker, pkl_file):
    """Test the main function correctly reports the result"""

    mock_result = 123

    mocker.patch(
        "covalent_pbspro_plugin.exec._execute",
        return_value={"result": mock_result, "exception": None},
    )
    mocker.patch.object(sys, "argv", ["", "mock_func_filename", pkl_file])

    exec_script.main()

    with open(pkl_file, "rb") as f:
        contents = f.read()
        result, exception = pickle.loads(contents)

    assert result == mock_result
    assert exception is None


def test_main_with_error(mocker, pkl_file):
    """Test the main function correctly reports an error"""

    mock_result = None
    mock_exception = RuntimeError("Mock error")

    mocker.patch(
        "covalent_pbspro_plugin.exec._execute",
        return_value={"result": mock_result, "exception": mock_exception},
    )
    mocker.patch.object(sys, "argv", ["", "mock_func_filename", pkl_file])

    exec_script.main()

    with open(pkl_file, "rb") as f:
        contents = f.read()
        result, exception = pickle.loads(contents)

    assert result is None
    assert isinstance(exception, RuntimeError)
    assert str(exception) == "Mock error"


def test_main_raised_error(mocker, pkl_file):
    """Test the main function handles an error raised by _execute"""
    mock_exception = RuntimeError("Mock error from _execute")

    mocker.patch(
        "covalent_pbspro_plugin.exec._execute",
        side_effect=mock_exception,
    )
    mocker.patch.object(sys, "argv", ["", "mock_func_filename", pkl_file])

    exec_script.main()

    with open(pkl_file, "rb") as f:
        contents = f.read()
        result, exception = pickle.loads(contents)

    assert result is None
    assert isinstance(exception, RuntimeError)
    assert str(exception) == "Mock error from _execute"
