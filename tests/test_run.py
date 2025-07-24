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

"""Tests for run() of the PBS Professional executor plugin."""

import importlib.metadata
import io
from functools import partial
from pathlib import Path
from unittest import mock

import pytest
from covalent._shared_files.exceptions import TaskCancelledError, TaskRuntimeError
from covalent._workflow import TransportableObject

from covalent_pbspro_plugin.pbspro import (
    PBSProExecutor,
    _CommandResult,
    _JobStateCategory,
    _JobStats,
)


def mock_wrapper_fn(function: TransportableObject, *args, **kwargs):
    """Minimum mocking of wrapper_fn"""

    fn = function.get_deserialized()

    output = fn(*args, **kwargs)

    return TransportableObject(output)


@pytest.fixture
def run_input_mock():
    # mock objects
    def f(x, y):
        return x + y

    mock_function = partial(mock_wrapper_fn, TransportableObject(f))
    mock_task_metadata = {
        "dispatch_id": "378cdd10-8443-11ee-99e2-a35cb4664dfd",
        "node_id": 1,
        "results_dir": "path/to/results_dir",
    }
    mock_func_args = [1]
    mock_func_kwargs = {"y": 2}
    mock_result = f(*mock_func_args, **mock_func_kwargs)

    return mock_function, mock_task_metadata, mock_func_args, mock_func_kwargs, mock_result


@pytest.fixture
def run_common_patches(mocker, conn_mock):
    mock_version_info = {"python": "3.9.16", "covalent": importlib.metadata.version("covalent")}
    mocker.patch.object(
        PBSProExecutor, "get_version_info", new=mock.AsyncMock(return_value=mock_version_info)
    )
    mocker.patch.object(PBSProExecutor, "set_job_handle", new=mock.AsyncMock())

    mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(
                command="mock command", returncode=0, stdout="stdout", stderr="stderr"
            )
        ),
    )
    conn_mock.close = mock.MagicMock(return_value=None)
    conn_mock.wait_closed = mock.AsyncMock(return_value=None)

    mocker.patch.object(
        PBSProExecutor, "_client_connect", new=mock.AsyncMock(return_value=conn_mock)
    )

    patch_format_submit_script = mocker.patch.object(
        PBSProExecutor,
        "_format_submit_script",
        new=mock.MagicMock(return_value="mock jobscript"),
    )

    patch_upload_task = mocker.patch.object(
        PBSProExecutor, "_upload_task", new=mock.AsyncMock(return_value=None)
    )

    patch_submit_task = mocker.patch.object(
        PBSProExecutor,
        "submit_task",
        new=mock.AsyncMock(
            return_value=mock.MagicMock(returncode=0, stdout="1.hostname", stderr="")
        ),
    )

    # Mock of return value of poll_task with job_state_category.COMPLETED.
    remote_stdout_filename = "/path/to/remote_stdout"
    remote_stderr_filename = "/path/to/remote_stderr"
    mock_final_state_completed = _JobStats(
        job_state_category=_JobStateCategory.COMPLETED,
        job_state="F",
        substate=92,
        exit_status=0,
        stdout_path=remote_stdout_filename,
        stderr_path=remote_stderr_filename,
    )

    patch_poll_task = mocker.patch.object(
        PBSProExecutor, "_poll_task", new=mock.AsyncMock(return_value=mock_final_state_completed)
    )

    patch_get_cancel_requested = mocker.patch.object(
        PBSProExecutor, "get_cancel_requested", new=mock.AsyncMock(return_value=False)
    )

    patches = {
        "_format_submit_script": patch_format_submit_script,
        "_upload_task": patch_upload_task,
        "submit_task": patch_submit_task,
        "_poll_task": patch_poll_task,
        "get_cancel_requested": patch_get_cancel_requested,
    }

    return patches


@pytest.mark.asyncio
async def test_run_success(mocker, tmpdir, conn_mock, run_input_mock, run_common_patches):
    """Test PBSProExecutor.run() works as expected."""

    mock_function, mock_task_metadata, mock_func_args, mock_func_kwargs, mock_result = (
        run_input_mock
    )

    patches = run_common_patches
    patch_format_submit_script = patches["_format_submit_script"]
    patch_upload_task = patches["_upload_task"]
    patch_submit_task = patches["submit_task"]
    patch_poll_task = patches["_poll_task"]
    remote_stdout_filename = patch_poll_task.return_value.stdout_path
    remote_stderr_filename = patch_poll_task.return_value.stderr_path

    mock_stdout = "mock stdout of the job\n"
    mock_stderr = "mock stderr of the job\n"
    mock_exception = None

    patch_query_result = mocker.patch.object(
        PBSProExecutor,
        "query_result",
        new=mock.AsyncMock(return_value=(mock_result, mock_stdout, mock_stderr, mock_exception)),
    )

    # Use temporary directory as cache_dir
    remote_workdir = "/home/test_user/workdir"
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        create_unique_workdir=False,
        cache_dir=str(tmpdir),
    )
    executor._task_stdout = io.StringIO()
    executor._task_stderr = io.StringIO()

    func_filename = f"func-{mock_task_metadata['dispatch_id']}-{mock_task_metadata['node_id']}.pkl"
    submit_script_filename = (
        f"jobscript-{mock_task_metadata['dispatch_id']}-{mock_task_metadata['node_id']}.sh"
    )
    py_script_filename = (
        f"script-{mock_task_metadata['dispatch_id']}-{mock_task_metadata['node_id']}.py"
    )
    result_filename = (
        f"result-{mock_task_metadata['dispatch_id']}-{mock_task_metadata['node_id']}.pkl"
    )

    actual_result = await executor.run(
        mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata
    )

    # Check the return value of run.
    assert actual_result == mock_result

    # Check each function called with appropriate arguments.
    patch_format_submit_script.assert_called_once_with(
        python_version=".".join(mock_function.args[0].python_version.split(".")[:2]),
        covalent_version=(await executor.get_version_info())["covalent"],
        py_script_filename=py_script_filename,
        func_filename=func_filename,
        result_filename=result_filename,
        current_remote_workdir=remote_workdir,
    )
    patch_upload_task.assert_called_once_with(
        conn_mock,
        str(tmpdir / func_filename),
        str(tmpdir / py_script_filename),
        str(tmpdir / submit_script_filename),
        remote_workdir,
    )
    patch_submit_task.assert_called_once_with(
        conn_mock, str(Path(remote_workdir) / submit_script_filename)
    )
    patch_poll_task.assert_called_once_with(conn_mock, patch_submit_task.return_value.stdout)
    patch_query_result.assert_called_once_with(
        conn_mock,
        str(Path(remote_workdir) / result_filename),
        remote_stdout_filename,
        remote_stderr_filename,
        Path(mock_task_metadata["results_dir"]) / mock_task_metadata["dispatch_id"],
    )

    # Check whether stdout and stderr obtained from query_result are written in _task_stdout and _task_stderr.
    assert executor._task_stdout.getvalue() == mock_stdout
    assert executor._task_stderr.getvalue() == mock_stderr

    # Check whether private instance variables for teardown are set as expected.
    assert executor._cleanup_files.remote_func_filename == str(Path(remote_workdir, func_filename))
    assert executor._cleanup_files.remote_job_script_filename == str(
        Path(remote_workdir, submit_script_filename)
    )
    assert executor._cleanup_files.remote_py_script_filename == str(
        Path(remote_workdir, py_script_filename)
    )
    assert executor._cleanup_files.remote_result_filename == str(
        Path(remote_workdir, result_filename)
    )
    assert executor._cleanup_files.remote_stdout_filename == remote_stdout_filename
    assert executor._cleanup_files.remote_stderr_filename == remote_stderr_filename
    assert executor._cleanup_files.local_job_script_filename == str(
        tmpdir / submit_script_filename
    )
    assert executor._cleanup_files.local_py_script_filename == str(tmpdir / py_script_filename)
    assert executor._cleanup_files.local_func_filename == str(tmpdir / func_filename)

    # Check whether writing scripts works as expected.
    with open(executor._cleanup_files.local_job_script_filename) as job_script:
        assert job_script.read() == patch_format_submit_script.return_value
    with open(executor._cleanup_files.local_py_script_filename) as py_script:
        assert py_script.read() == (
            Path(__file__).parent / ".." / "covalent_pbspro_plugin" / "exec.py"
        ).read_text("utf-8")

    # Check whether current_workdir is the expected name if create_unique_workdir is True.
    executor_with_unique_workdir = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        create_unique_workdir=True,
        cache_dir=str(tmpdir),
    )
    executor_with_unique_workdir._task_stdout = io.StringIO()
    executor_with_unique_workdir._task_stderr = io.StringIO()

    await executor_with_unique_workdir.run(
        mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata
    )

    patch_upload_task.assert_called_with(
        conn_mock,
        str(tmpdir / func_filename),
        str(tmpdir / py_script_filename),
        str(tmpdir / submit_script_filename),
        str(
            Path(remote_workdir)
            / mock_task_metadata["dispatch_id"]
            / f"node_{mock_task_metadata['node_id']}"
        ),
    )


@pytest.mark.asyncio
async def test_run_failed_make_remote_workdir(
    mocker, tmpdir, conn_mock, run_input_mock, run_common_patches
):
    """Test PBSProExecutor.run() failed to make remote workdir."""

    mock_function, mock_task_metadata, mock_func_args, mock_func_kwargs, _ = run_input_mock

    # mock to fail conn.run(f"mkdir -p {current_remote_workdir}")
    patch_run_command_failed = mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(
                command="mock command", returncode=1, stdout="stdout", stderr="stderr"
            )
        ),
    )
    mocker.patch.object(
        PBSProExecutor, "_client_connect", new=mock.AsyncMock(return_value=conn_mock)
    )

    remote_workdir = "/home/test_user/workdir"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        create_unique_workdir=False,
        cache_dir=str(tmpdir),
    )
    with pytest.raises(RuntimeError):
        await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
    patch_run_command_failed.assert_called_once_with(conn_mock, f"mkdir -p {remote_workdir}")


@pytest.mark.asyncio
async def test_run_failed_submit_task(
    mocker, tmpdir, conn_mock, proc_mock, run_input_mock, run_common_patches
):
    """Test PBSProExecutor.run() failed to submit_task."""

    mock_function, mock_task_metadata, mock_func_args, mock_func_kwargs, _ = run_input_mock

    # mock that submit_task fail.
    patch_submit_task_fail = mocker.patch.object(
        PBSProExecutor,
        "submit_task",
        new=mock.AsyncMock(return_value=mock.MagicMock(returncode=1, stdout="", stderr="")),
    )
    remote_workdir = "/home/test_user/workdir"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        create_unique_workdir=False,
        cache_dir=str(tmpdir),
    )
    with pytest.raises(RuntimeError):
        await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
    patch_submit_task_fail.assert_called_once_with(
        conn_mock,
        str(
            Path(
                remote_workdir,
                f"jobscript-{mock_task_metadata['dispatch_id']}-{mock_task_metadata['node_id']}.sh",
            )
        ),
    )


@pytest.mark.asyncio
async def test_run_failed_job_script(
    mocker, tmpdir, conn_mock, run_input_mock, run_common_patches
):
    """Test PBSProExecutor.run() handles error when job script failed."""

    mock_function, mock_task_metadata, mock_func_args, mock_func_kwargs, _ = run_input_mock

    # Mock of return value of poll_task with job_state_category.FAILED.
    mock_final_state_failed = _JobStats(
        job_state_category=_JobStateCategory.FAILED,
        job_state="F",
        substate=93,
        exit_status=1,
        stdout_path="/path/to/remote_stdout",
        stderr_path="/path/to/remote_stderr",
    )

    mocker.patch.object(
        PBSProExecutor, "_poll_task", new=mock.AsyncMock(return_value=mock_final_state_failed)
    )

    mock_stdout = "mock stdout of the job\n"
    mock_stderr = "mock stderr of the job\n"
    mock_exception = None

    patch_query_result = mocker.patch.object(
        PBSProExecutor,
        "query_result",
        new=mock.AsyncMock(return_value=(None, mock_stdout, mock_stderr, mock_exception)),
    )
    patch_query_stdout_stderr = mocker.patch.object(
        PBSProExecutor,
        "_query_stdout_stderr",
        new=mock.AsyncMock(return_value=(mock_stdout, mock_stderr)),
    )

    remote_workdir = "/home/test_user/workdir"
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        create_unique_workdir=False,
        cache_dir=str(tmpdir),
    )
    executor._task_stdout = io.StringIO()
    executor._task_stderr = io.StringIO()

    with pytest.raises(TaskRuntimeError):
        await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
    assert patch_query_result.call_count == 0
    assert patch_query_stdout_stderr.call_count == 1
    assert executor._task_stdout.getvalue() == mock_stdout
    assert executor._task_stderr.getvalue() == mock_stderr


@pytest.mark.asyncio
async def test_run_failed_python_script(
    mocker, tmpdir, conn_mock, run_input_mock, run_common_patches
):
    """Test PBSProExecutor.run() handles error when job script completed but python script raises an exception."""

    mock_function, mock_task_metadata, mock_func_args, mock_func_kwargs, mock_result = (
        run_input_mock
    )

    mock_stdout = "mock stdout of the job\n"
    mock_stderr = "mock stderr of the job\n"
    mock_exception = RuntimeError("mock exception of the job")

    patch_query_result = mocker.patch.object(
        PBSProExecutor,
        "query_result",
        new=mock.AsyncMock(return_value=(None, mock_stdout, mock_stderr, mock_exception)),
    )
    patch_query_stdout_stderr = mocker.patch.object(
        PBSProExecutor,
        "_query_stdout_stderr",
        new=mock.AsyncMock(return_value=(mock_stdout, mock_stderr)),
    )

    remote_workdir = "/home/test_user/workdir"
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        create_unique_workdir=False,
        cache_dir=str(tmpdir),
    )
    executor._task_stdout = io.StringIO()
    executor._task_stderr = io.StringIO()

    with pytest.raises(TaskRuntimeError):
        await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
    assert patch_query_result.call_count == 1
    assert patch_query_stdout_stderr.call_count == 0
    assert executor._task_stdout.getvalue() == mock_stdout
    assert (
        executor._task_stderr.getvalue()
        == f"{mock_stderr}{type(mock_exception).__name__}: {mock_exception}"
    )


@pytest.mark.asyncio
async def test_run_handling_cancel_request(
    mocker, tmpdir, conn_mock, run_input_mock, run_common_patches
):
    """Test function run handling the request that cancel the task as expected."""

    mock_function, mock_task_metadata, mock_func_args, mock_func_kwargs, mock_result = (
        run_input_mock
    )

    # Use temporary directory as cache_dir
    remote_workdir = "/home/test_user/workdir"
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        create_unique_workdir=False,
        cache_dir=str(tmpdir),
    )
    executor._task_stdout = io.StringIO()
    executor._task_stderr = io.StringIO()

    def get_cancel_requested_return_value(times: int):
        ret = [False] * times
        ret[-1] = True
        return ret

    # Case request that task to be cancelled before calling _client_connect.
    with mock.patch.object(
        PBSProExecutor,
        "get_cancel_requested",
        side_effect=get_cancel_requested_return_value(1),
    ):
        with pytest.raises(TaskCancelledError):
            await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
        assert PBSProExecutor._client_connect.call_count == 0

    # Case request that task to be cancelled after calling _client_connect.
    with mock.patch.object(
        PBSProExecutor,
        "get_cancel_requested",
        side_effect=get_cancel_requested_return_value(2),
    ):
        with pytest.raises(TaskCancelledError):
            await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
        assert PBSProExecutor._client_connect.call_count == 1
        assert PBSProExecutor._format_submit_script.call_count == 0

    # Case request that task to be cancelled before uploading task to remote machine.
    with mock.patch.object(
        PBSProExecutor,
        "get_cancel_requested",
        side_effect=get_cancel_requested_return_value(3),
    ):
        with pytest.raises(TaskCancelledError):
            await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
        assert PBSProExecutor._format_submit_script.call_count == 1
        assert PBSProExecutor._upload_task.call_count == 0

    # Case request that task to be cancelled before submitting task to PBS Professional.
    with mock.patch.object(
        PBSProExecutor,
        "get_cancel_requested",
        side_effect=get_cancel_requested_return_value(4),
    ):
        with pytest.raises(TaskCancelledError):
            await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
        assert PBSProExecutor._upload_task.call_count == 1
        assert PBSProExecutor._poll_task.call_count == 0

    # Case request that task to be cancelled between _poll_task.
    with mock.patch.object(
        PBSProExecutor,
        "get_cancel_requested",
        side_effect=get_cancel_requested_return_value(5),
    ):
        with pytest.raises(TaskCancelledError):
            await executor.run(mock_function, mock_func_args, mock_func_kwargs, mock_task_metadata)
        assert PBSProExecutor._poll_task.call_count == 1
