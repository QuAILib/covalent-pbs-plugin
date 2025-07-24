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

import tempfile
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

import pytest
from covalent._shared_files.config import get_config, set_config
from pytest_metadata.plugin import metadata_key

from covalent_pbspro_plugin.pbspro import _EXECUTOR_PLUGIN_DEFAULTS


@pytest.fixture(scope="session", autouse=True)
def setup():
    # preprocess
    start_config = deepcopy(get_config())
    pbspro_config = _EXECUTOR_PLUGIN_DEFAULTS
    new_config = get_config()
    new_config["executors"]["pbspro"] = pbspro_config
    set_config(new_config)

    yield

    # postprocess
    set_config(start_config)


@pytest.fixture
def conn_mock():
    return mock.MagicMock()


@pytest.fixture
def proc_mock():
    return mock.MagicMock()


@pytest.fixture
def pkl_file():
    """make temporary pkl file and finally delete.

    Yields:
        Generator[Path, Any, None]: file path of the temporary file
    """
    path = Path(tempfile.NamedTemporaryFile(suffix=".pkl", delete=False).name)
    yield str(path)

    # delete temporary file
    path.unlink(missing_ok=True)


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    key_list = list(session.config.stash[metadata_key].keys())
    for key in key_list:
        if key not in {"Python", "Platform", "Packages", "Plugins"}:
            del session.config.stash[metadata_key][key]


def pytest_html_results_table_header(cells):
    cells.insert(2, "<th>Description</th>")
    cells.insert(1, '<th class="sortable time" data-column-type="time">Time</th>')


def pytest_html_results_table_row(report, cells):
    cells.insert(2, f"<td>{report.description}</td>")
    cells.insert(1, f'<td class="col-time">{datetime.now(ZoneInfo("Asia/Tokyo"))}</td>')


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    report.description = str(item.function.__doc__)
