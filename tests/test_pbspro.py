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

"""Tests for the PBS Professional executor plugin."""

import asyncio
import importlib.metadata
import json
import os
from copy import deepcopy
from pathlib import Path
from unittest import mock

import cloudpickle
import pytest
from covalent._shared_files.config import get_config, set_config

from covalent_pbspro_plugin.pbspro import (
    PBSProExecutor,
    _CleanupFiles,
    _CommandResult,
    _JobStateCategory,
    _JobStats,
)

DATA_DIR = Path(os.path.dirname(__file__)) / "data"


def test_init_defaults() -> None:
    """Test that initialization properly sets member variables in case of default values."""

    executor = PBSProExecutor()

    assert executor.username == ""
    assert executor.address == ""
    assert executor.port == 22
    assert executor.ssh_key_file is None
    assert executor.cert_file is None
    assert executor.passphrase is None
    assert executor.bashrc_path == "$HOME/.bashrc"
    assert executor.prerun_commands == []
    assert executor.postrun_commands == []
    assert executor.qsub_args == {}
    assert executor.embedded_qsub_args == {}
    assert executor.poll_freq == 60
    assert executor.cleanup is True
    assert executor.skip_ssh is False
    assert executor.remote_workdir == "covalent-workdir"
    assert executor.create_unique_workdir is False
    assert executor.cache_dir == str(
        Path(get_config("dispatcher.cache_dir")).expanduser().resolve()
    )
    assert executor.log_stdout == "stdout.log"
    assert executor.log_stderr == "stderr.log"
    assert executor.time_limit == -1
    assert executor.retries == 0
    assert executor._cleanup_files is None


def test_init_poll_freq_auto_raised():
    """Test that poll_freq is auto-raised"""

    poll_freq = 10
    executor = PBSProExecutor(poll_freq=poll_freq)

    assert executor.poll_freq == 30


def test_init_nondefaults():
    """Test that initialization properly sets member variables in case of non-default values."""

    args = {
        "username": "username",
        "address": "host",
        "port": 10022,
        "ssh_key_file": "~/.ssh/id_rsa",
        "cert_file": "~/.ssh/id_rsa.pub",
        "passphrase": "passphrase",
        "bashrc_path": "path/to/.bashrc",
        "prerun_commands": ["module load module1", "module load module2"],
        "postrun_commands": ["tracejob 1"],
        "qsub_args": {
            "q": "queue1",
        },
        "embedded_qsub_args": {
            "l": ["walltime=1:00:00", "select=mem=400mb"],
            "V": "",
        },
        "poll_freq": 45,
        "cleanup": False,
        "skip_ssh": True,
        "remote_workdir": "path/to/remote_workdir",
        "create_unique_workdir": True,
        "cache_dir": "path_to/cache_dir",
        "log_stdout": "path/to/log_stdout.log",
        "log_stderr": "path/to/log_stderr.log",
        "time_limit": 60,
        "retries": 3,
    }
    executor = PBSProExecutor(**args)

    assert executor.username == args["username"]
    assert executor.address == args["address"]
    assert executor.port == args["port"]
    assert executor.ssh_key_file == str(Path(args["ssh_key_file"]).expanduser().resolve())
    assert executor.cert_file == str(Path(args["cert_file"]).expanduser().resolve())
    assert executor.passphrase == args["passphrase"]
    assert executor.bashrc_path == args["bashrc_path"]
    assert executor.prerun_commands == args["prerun_commands"]
    assert executor.postrun_commands == args["postrun_commands"]
    assert executor.qsub_args == args["qsub_args"]
    assert executor.embedded_qsub_args == args["embedded_qsub_args"]
    assert executor.poll_freq == args["poll_freq"]
    assert executor.cleanup == args["cleanup"]
    assert executor.skip_ssh == args["skip_ssh"]
    assert executor.remote_workdir == args["remote_workdir"]
    assert executor.create_unique_workdir == args["create_unique_workdir"]
    assert executor.cache_dir == str(Path(args["cache_dir"]).expanduser().resolve())
    assert executor.log_stdout == args["log_stdout"]
    assert executor.log_stderr == args["log_stderr"]
    assert executor.time_limit == args["time_limit"]
    assert executor.retries == args["retries"]
    assert executor._cleanup_files is None

    os.removedirs(args["cache_dir"])


def test_init_config_bashrc_path_not_exist():
    """Test that initialization is succeeded if config of bashrc_path is not exist and the arg is None."""

    start_config = deepcopy(get_config())
    config = get_config()
    config["executors"]["pbspro"].pop("bashrc_path", None)
    set_config(config)
    executor = PBSProExecutor(
        username="username",
        address="host",
        ssh_key_file="~/.ssh/id_rsa",
    )
    assert not executor.__dict__["bashrc_path"]
    set_config(start_config)


def test_init_config_bashrc_path_empty():
    """Test that initialization is succeeded if bashrc_path is empty string."""

    executor = PBSProExecutor(
        username="username",
        address="host",
        ssh_key_file="~/.ssh/id_rsa",
        bashrc_path="",
    )
    assert executor.bashrc_path == ""


@pytest.mark.asyncio
async def test_run_command(mocker, conn_mock, proc_mock):
    """Test for _run_command works as expected with no error."""

    proc_mock.returncode = 0
    proc_mock.stdout = "stdout"
    proc_mock.stderr = "stderr"
    conn_mock.run = mock.AsyncMock(return_value=proc_mock)

    patch_run_async_subprocess = mocker.patch.object(
        PBSProExecutor,
        "run_async_subprocess",
        new=mock.AsyncMock(return_value=(proc_mock, b"stdout", b"stderr")),
    )
    command = "ls -l"
    expected_result = _CommandResult(
        command=command, returncode=0, stdout="stdout", stderr="stderr"
    )

    executor = PBSProExecutor(
        username="username",
        address="host",
        ssh_key_file="~/.ssh/id_rsa",
        bashrc_path="",
    )

    remote_command_result = await executor._run_command(conn_mock, command)
    conn_mock.run.assert_called_once_with(command)
    assert remote_command_result == expected_result

    local_command_result = await executor._run_command(None, command)
    patch_run_async_subprocess.assert_called_once_with(command)
    assert local_command_result == expected_result


@pytest.mark.asyncio
async def test_validate_credentials_no_exception(mocker):
    """Test for _validate_credentials raises no exception"""

    mocker.patch.object(Path, "is_file", return_value=True)

    executor = PBSProExecutor(
        username="user", address="address", ssh_key_file="ssh_key_file", cert_file="cert_file"
    )
    assert await executor._validate_credentials() is True

    # In case cert_file is not specified
    executor = PBSProExecutor(username="user", address="address", ssh_key_file="ssh_key_file")
    assert await executor._validate_credentials() is True

    # In case ssh_key_file is not specified
    executor = PBSProExecutor(username="user", address="address")
    assert await PBSProExecutor(username="user", address="address")._validate_credentials() is True


@pytest.mark.asyncio
async def test_validate_credentials_value_error(mocker):
    """Test for _validate_credentials raises ValueError"""

    # Test for ValueError
    with pytest.raises(ValueError):
        executor = PBSProExecutor(username="user", address="address", ssh_key_file="ssh_key_file")
        executor.username = None
        await executor._validate_credentials()

    with pytest.raises(ValueError):
        executor = PBSProExecutor(username="user", address="address", ssh_key_file="ssh_key_file")
        executor.username = ""
        await executor._validate_credentials()

    with pytest.raises(ValueError):
        executor = PBSProExecutor(username="user", address="address", ssh_key_file="ssh_key_file")
        executor.address = None
        await executor._validate_credentials()

    with pytest.raises(ValueError):
        executor = PBSProExecutor(username="user", address="address", ssh_key_file="ssh_key_file")
        executor.address = ""
        await executor._validate_credentials()


@pytest.mark.asyncio
async def test_validate_credentials_file_not_found(mocker):
    """Test for _validate_credentials raises FileNotFoundError"""

    not_exist_file_path = "file/does/not/exist"

    def mock_is_file(path_object: Path):
        if "file/does/not/exist" in str(path_object):
            return False
        return True

    mocker.patch.object(Path, "is_file", new=mock_is_file)

    with pytest.raises(FileNotFoundError):
        executor = PBSProExecutor(
            username="user", address="address", ssh_key_file=not_exist_file_path
        )
        await executor._validate_credentials()

    with pytest.raises(FileNotFoundError):
        executor = PBSProExecutor(
            username="user",
            address="address",
            ssh_key_file="ssh_key_file",
            cert_file=not_exist_file_path,
        )
        await executor._validate_credentials()


@pytest.mark.asyncio
async def test_client_connect_no_exception(mocker, conn_mock):
    """Test for _client_connect with mocking .connect()"""

    mocker.patch("asyncssh.connect", new=mock.AsyncMock(return_value=conn_mock))

    mocker.patch.object(
        PBSProExecutor, "_validate_credentials", new=mock.AsyncMock(return_value=True)
    )

    executor = PBSProExecutor(
        address="test_address", username="test_use", ssh_key_file="ssh_key_file"
    )
    await executor._client_connect()

    executor = PBSProExecutor(
        address="test_address",
        username="test_use",
        ssh_key_file="ssh_key_file",
        cert_file="cert_file",
    )
    await executor._client_connect()

    executor = PBSProExecutor(
        address="test_address",
        username="test_use",
        passphrase="passphrase",
        ssh_key_file="ssh_key_file",
        cert_file="cert_file",
    )
    await executor._client_connect()

    # In case ssh_key_file is not specified, this function does not raise exception and try to connect.
    executor = PBSProExecutor(address="test_address", username="test_use")
    await executor._client_connect()


@pytest.mark.asyncio
async def test_client_connect_raises_exception(mocker):
    """Test RuntimeError is raised when .connect() is failed with mocking .connect()"""

    mocked_error = Exception("mocked error")
    mocker.patch("asyncssh.connect", side_effect=mocked_error)

    mocker.patch.object(
        PBSProExecutor, "_validate_credentials", new=mock.AsyncMock(return_value=True)
    )

    with pytest.raises(RuntimeError):
        executor = PBSProExecutor(username="user", address="address", ssh_key_file="ssh_key_file")
        await executor._client_connect()


@pytest.mark.asyncio
async def test_client_connect_skip_ssh(mocker, conn_mock):
    """Test for _client_connect return nothing when skip_ssh is True"""
    executor = PBSProExecutor(
        username="user", address="address", ssh_key_file="ssh_key_file", skip_ssh=True
    )
    conn = await executor._client_connect()
    assert conn is None


def test_format_submit_script():
    """Test that the shell script (in string form) which is to be submitted on
    the remote server is created with no errors."""

    remote_workdir = "/home/test_user/workdir"
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        embedded_qsub_args={
            "l": ["select=ncpus=2:mem=4gb", "walltime=1:00:00"],
            "f": "",
            "N": "JobName",
        },
        prerun_commands=["prerun1", "prerun2"],
        postrun_commands=["postrun1", "postrun2", "postrun3"],
        remote_workdir=remote_workdir,
        cache_dir="~/.cache/covalent",
    )

    python_version = ".".join(map(str, __import__("sys").version_info[:2]))
    covalent_version = importlib.metadata.version("covalent")
    cloudpickle_version = importlib.metadata.version("cloudpickle")
    dispatch_id = "378cdd10-8443-11ee-99e2-a35cb4664dfd"
    node_id = 1
    py_script_filename = f"script-{dispatch_id}-{node_id}.py"
    func_filename = f"func-{dispatch_id}-{node_id}.pkl"
    result_filename = f"result-{dispatch_id}-{node_id}.pkl"

    expected_directives = (
        "#PBS -l select=ncpus=2:mem=4gb\n#PBS -l walltime=1:00:00\n#PBS -f\n#PBS -N JobName\n"
    )
    expected_prerun_commands_lines = "prerun1\nprerun2\n"
    expected_postrun_commands_lines = "postrun1\npostrun2\npostrun3\n"
    expected_python_command = f"python3 {py_script_filename} {func_filename} {result_filename}"

    expected_python_version_str = f'$remote_py_version != "{python_version}"'
    expected_covalent_version_str = f'$covalent_version != "{covalent_version}"'
    expected_cloudpickle_version_str = f'$cloudpickle_version != "{cloudpickle_version}"'

    actual = executor._format_submit_script(
        python_version=python_version,
        covalent_version=covalent_version,
        py_script_filename=py_script_filename,
        func_filename=func_filename,
        result_filename=result_filename,
        current_remote_workdir=remote_workdir,
    )
    assert actual.startswith("#!/bin/bash -e")
    assert expected_directives in actual
    assert expected_prerun_commands_lines in actual
    assert expected_postrun_commands_lines in actual
    assert expected_python_command in actual
    assert expected_python_version_str in actual
    assert expected_covalent_version_str in actual
    assert expected_cloudpickle_version_str in actual


@pytest.mark.asyncio
async def test_upload_task(mocker, conn_mock):
    """Test that required_files for the task are uploaded to the remote machine with no errors."""

    patch_scp = mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())

    remote_workdir = "/home/test_user/workdir"
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
    )

    local_workdir = "home/user/local_workdir/"
    func_filename = local_workdir + "func-test.pkl"
    py_script_filename = local_workdir + "script-test.py"
    submit_script_filename = local_workdir + "submit-script-test.sh"

    await executor._upload_task(
        conn_mock, func_filename, py_script_filename, submit_script_filename, remote_workdir
    )

    assert patch_scp.call_count == 3
    patch_scp.assert_has_calls(
        [
            mocker.call(func_filename, (conn_mock, remote_workdir)),
            mocker.call(py_script_filename, (conn_mock, remote_workdir)),
            mocker.call(submit_script_filename, (conn_mock, remote_workdir)),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_upload_task_skip_ssh(mocker, conn_mock):
    """Test that required_files for the task are copied to the workdir with no errors. when skip_ssh is True."""

    patch_shutil_copy = mocker.patch("shutil.copy", return_value=mock.AsyncMock())

    remote_workdir = "/home/test_user/workdir"
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
    )

    local_workdir = "home/user/local_workdir/"
    func_filename = local_workdir + "func-test.pkl"
    py_script_filename = local_workdir + "script-test.py"
    submit_script_filename = local_workdir + "submit-script-test.sh"

    await executor._upload_task(
        None, func_filename, py_script_filename, submit_script_filename, remote_workdir
    )

    assert patch_shutil_copy.call_count == 3
    patch_shutil_copy.assert_has_calls(
        [
            mocker.call(func_filename, remote_workdir),
            mocker.call(py_script_filename, remote_workdir),
            mocker.call(submit_script_filename, remote_workdir),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_submit_task(mocker, conn_mock):
    """Test that the command is executed with no error."""

    mock_command_result = _CommandResult(
        command="mock command", returncode=0, stdout="1.hostname", stderr=""
    )
    patch_run_command = mocker.patch.object(
        PBSProExecutor, "_run_command", new=mock.AsyncMock(return_value=mock_command_result)
    )

    remote_workdir = "/home/test_user/workdir"
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        qsub_args={"l": ["walltime=1:00:00", "select=mem=400mb"], "V": ""},
    )

    remote_submit_script_filename = f"{remote_workdir}/submit-script-test.sh"

    qsub_args_str = "-l walltime=1:00:00 -l select=mem=400mb -V"
    expected_cmd = f"qsub {qsub_args_str} {remote_submit_script_filename}"
    proc = await executor.submit_task(conn_mock, remote_submit_script_filename)

    assert proc == mock_command_result
    patch_run_command.assert_called_with(conn_mock, expected_cmd)

    # without qsub_args
    executor.qsub_args = {}
    expected_cmd = f"qsub {remote_submit_script_filename}"
    proc = await executor.submit_task(conn_mock, remote_submit_script_filename)

    patch_run_command.assert_called_with(conn_mock, expected_cmd)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_state, sub_state, exit_status, stdout_path, stderr_path, expected_status",
    [
        ("E", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.ACTIVE),
        ("F", 92, 0, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.COMPLETED),
        ("F", 93, 1, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.FAILED),
        ("F", 91, 271, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.FAILED),
        ("H", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.QUEUED),
        ("M", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.QUEUED),
        ("Q", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.QUEUED),
        ("R", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.ACTIVE),
        ("S", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.QUEUED),
        ("T", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.QUEUED),
        ("U", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.QUEUED),
        ("W", 92, None, "/path/to/stdout_log", "/path/to/stderr_log", _JobStateCategory.QUEUED),
    ],
)
async def test_get_status(
    mocker,
    conn_mock,
    job_state,
    sub_state,
    exit_status,
    stdout_path,
    stderr_path,
    expected_status,
):
    """Test that get_status works as expected."""

    job_id = "12.server-host"
    test_username = "test-user"

    def mock_run_command_impl(*_):
        with open(DATA_DIR / "sample_qstat_format.json", "r") as f:
            sample_json = json.loads(f.read())
            sample_json["Jobs"][job_id]["job_state"] = job_state
            sample_json["Jobs"][job_id]["substate"] = sub_state
            sample_json["Jobs"][job_id][
                "Error_Path"
            ] = f"{sample_json['pbs_server']}:{stderr_path}"
            sample_json["Jobs"][job_id][
                "Output_Path"
            ] = f"{sample_json['pbs_server']}:{stdout_path}"
            if exit_status is not None:
                sample_json["Jobs"][job_id]["Exit_status"] = exit_status
            else:
                sample_json["Jobs"][job_id].pop("Exit_status")
            result = _CommandResult(
                command="mock command", returncode=0, stdout=json.dumps(sample_json), stderr=""
            )
        return result

    patch_run_command = mocker.patch.object(
        PBSProExecutor, "_run_command", new=mock.AsyncMock(side_effect=mock_run_command_impl)
    )

    executor = PBSProExecutor(
        username=test_username,
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    actual_result = await executor.get_status(conn_mock, job_id)
    patch_run_command.assert_called_with(conn_mock, f"qstat -x -f -F json {job_id}")

    assert actual_result.job_state_category == expected_status
    assert actual_result.substate == sub_state
    assert actual_result.exit_status == exit_status
    assert actual_result.stdout_path == stdout_path
    assert actual_result.stderr_path == stderr_path


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_state, sub_state, exit_status",
    [
        ("A", 91, 0),
        ("X", 91, 0),
        ("B", 91, 0),
    ],
)
async def test_get_status_unknown_state(mocker, conn_mock, job_state, sub_state, exit_status):
    """Test that get_status raises an error when unknown state is encountered."""

    job_id = "12.server-host"
    test_username = "test-user"

    def mock_run_command_impl(*_):
        with open(DATA_DIR / "sample_qstat_format.json", "r") as f:
            sample_json = json.loads(f.read())
            sample_json["Jobs"][job_id]["job_state"] = job_state
            sample_json["Jobs"][job_id]["substate"] = sub_state
            sample_json["Jobs"][job_id]["Exit_status"] = exit_status

            result = _CommandResult(
                command="mock command", returncode=0, stdout=json.dumps(sample_json), stderr=""
            )
        return result

    mocker.patch.object(
        PBSProExecutor, "_run_command", new=mock.AsyncMock(side_effect=mock_run_command_impl)
    )

    executor = PBSProExecutor(
        username=test_username,
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    with pytest.raises(RuntimeError):
        await executor.get_status(conn_mock, job_id)


@pytest.mark.asyncio
async def test_get_status_failed_run_qstat(mocker, conn_mock):
    """Test that get_status raises an error when qstat execution fails"""

    job_id = "12.server-host"
    test_username = "test-user"

    mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(
                command="mock command", returncode=1, stdout="", stderr="stderr"
            )
        ),
    )

    executor = PBSProExecutor(
        username=test_username,
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    with pytest.raises(RuntimeError):
        await executor.get_status(conn_mock, job_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "last_state",
    [
        _JobStats(_JobStateCategory.COMPLETED, "F", 92, 0, "/path/to/stdout", "/path/to/stderr"),
        _JobStats(_JobStateCategory.FAILED, "F", 93, 1, "/path/to/stdout", "/path/to/stderr"),
        _JobStats(_JobStateCategory.FAILED, "F", 91, None, "/path/to/stdout", "/path/to/stderr"),
    ],
)
async def test_poll_task(mocker, conn_mock, last_state):
    """Test that _poll_task continues if the state of the job is a particular state and finish with no error."""

    # _poll_task should finish polling at the last_state, so that get_status should be called 6 times.
    patch_get_status = mocker.patch.object(
        PBSProExecutor,
        "get_status",
        side_effect=[
            _JobStats(
                _JobStateCategory.QUEUED, "Q", 0, None, "/path/to/stdout", "/path/to/stderr"
            ),
            _JobStats(
                _JobStateCategory.QUEUED, "Q", 0, None, "/path/to/stdout", "/path/to/stderr"
            ),
            _JobStats(
                _JobStateCategory.ACTIVE, "R", 0, None, "/path/to/stdout", "/path/to/stderr"
            ),
            _JobStats(
                _JobStateCategory.ACTIVE, "R", 0, None, "/path/to/stdout", "/path/to/stderr"
            ),
            _JobStats(
                _JobStateCategory.ACTIVE, "R", 0, None, "/path/to/stdout", "/path/to/stderr"
            ),
            last_state,
            _JobStats(
                _JobStateCategory.QUEUED, "R", 0, None, "/path/to/stdout", "/path/to/stderr"
            ),
            _JobStats(
                _JobStateCategory.ACTIVE, "R", 0, None, "/path/to/stdout", "/path/to/stderr"
            ),
        ],
    )

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        poll_freq=30,
    )
    # To speed up of this test, set poll_freq to 1 specially.
    executor.poll_freq = 1

    job_id = "12.server-host"
    actual_last_state = await asyncio.wait_for(executor._poll_task(conn_mock, job_id), timeout=10)
    assert actual_last_state == last_state
    assert patch_get_status.call_count == 6


@pytest.mark.asyncio
async def test_query_remote_file_no_error(mocker, conn_mock):
    """Test _query_remote_file works as expected with no error."""

    patch_scp = mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())

    # Don't actually try to remove result files:
    mock_async_os_remove = mock.AsyncMock(return_value=None)
    patch_remove = mocker.patch("aiofiles.os.remove", side_effect=mock_async_os_remove)

    mock_contents = "mock contents"

    patch_run_command = mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(command="mock command", returncode=0, stdout="", stderr="")
        ),
    )

    def mock_open(*args, **kwargs):
        mock_file_object = mock.MagicMock()

        mock_file_object.__aenter__.return_value.read.return_value = mock_contents

        return mock_file_object

    patch_aiofiles_open = mocker.patch("aiofiles.open", side_effect=mock_open)

    task_results_dir = "/path/to/results_dir"
    remote_filename = "/path/to/remote_file.log"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    actual_contents = await executor._query_remote_file(
        conn_mock,
        remote_filename,
        task_results_dir,
        missing_ok=False,
    )
    assert actual_contents == mock_contents
    assert isinstance(actual_contents, str)
    assert patch_run_command.call_count == 1
    assert patch_scp.call_count == 1
    assert patch_aiofiles_open.call_count == 1
    assert patch_remove.call_count == 1


@pytest.mark.asyncio
async def test_query_remote_file_binary(mocker, conn_mock, pkl_file):
    """Test _query_remote_file that reads an binary file works as expected with no error."""

    patch_scp = mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())

    # Don't actually try to remove result files:
    mock_async_os_remove = mock.AsyncMock(return_value=None)
    patch_remove = mocker.patch("aiofiles.os.remove", side_effect=mock_async_os_remove)

    mock_contents = [1, 2, 3]
    with open(pkl_file, "wb") as f:
        cloudpickle.dump(mock_contents, f)
    with open(pkl_file, "rb") as f:
        expected_contents = f.read()

    patch_run_command = mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(command="mock command", returncode=0, stdout="", stderr="")
        ),
    )

    def mock_open(*args, **kwargs):
        mock_file_object = mock.MagicMock()

        mock_file_object.__aenter__.return_value.read.return_value = expected_contents

        return mock_file_object

    patch_aiofiles_open = mocker.patch("aiofiles.open", side_effect=mock_open)

    task_results_dir = "/path/to/results_dir"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    actual_contents = await executor._query_remote_file(
        conn_mock,
        pkl_file,
        task_results_dir,
        missing_ok=False,
        binary_mode=True,
    )
    assert actual_contents == expected_contents
    assert isinstance(actual_contents, bytes)
    assert cloudpickle.loads(actual_contents) == mock_contents
    assert patch_run_command.call_count == 1
    assert patch_scp.call_count == 1
    assert patch_aiofiles_open.call_count == 1
    patch_aiofiles_open.assert_called_with(Path(task_results_dir, Path(pkl_file).name), "rb")
    assert patch_remove.call_count == 1


@pytest.mark.asyncio
async def test_query_remote_file_skip_ssh(mocker):
    """Test _query_remote_file that reads a file works as expected with no error when skip_ssh is True."""

    patch_run_command = mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(
                command="mock command", returncode=0, stdout="stdout", stderr="stderr"
            )
        ),
    )

    mock_contents = "mock contents"

    def mock_open(*args, **kwargs):
        mock_file_object = mock.MagicMock()

        mock_file_object.__aenter__.return_value.read.return_value = mock_contents

        return mock_file_object

    patch_aiofiles_open = mocker.patch("aiofiles.open", side_effect=mock_open)

    task_results_dir = "/path/to/results_dir"
    remote_filename = "/path/to/remote_file.log"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    actual_contents = await executor._query_remote_file(
        None,
        remote_filename,
        task_results_dir,
        missing_ok=False,
        binary_mode=False,
    )
    assert actual_contents == mock_contents
    assert isinstance(actual_contents, str)
    assert patch_run_command.call_count == 1
    assert patch_aiofiles_open.call_count == 1
    patch_aiofiles_open.assert_called_with(remote_filename, "r")


@pytest.mark.asyncio
async def test_query_remote_file_missing_ok(mocker, conn_mock):
    """Test _query_remote_file raises no error when file is not found and missing_ok is True."""

    patch_scp = mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())

    # Don't actually try to remove result files:
    mock_async_os_remove = mock.AsyncMock(return_value=None)
    patch_remove = mocker.patch("aiofiles.os.remove", side_effect=mock_async_os_remove)

    # When file is not found, expected return value is empty bytes
    mock_contents = b""

    patch_run_command_fail = mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(command="mock command", returncode=1, stdout="", stderr="")
        ),
    )

    def mock_open(*args):
        mock_file_object = mock.MagicMock()

        mock_file_object.__aenter__.return_value.read.return_value = mock_contents

        return mock_file_object

    patch_aiofiles_open = mocker.patch("aiofiles.open", side_effect=mock_open)

    task_results_dir = "/path/to/results_dir"
    remote_filename = "/path/to/remote_file.log"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    actual_contents = await executor._query_remote_file(
        conn_mock,
        remote_filename,
        task_results_dir,
        missing_ok=True,
    )
    assert actual_contents == mock_contents
    assert patch_run_command_fail.call_count == 1
    assert patch_scp.call_count == 0
    assert patch_aiofiles_open.call_count == 0
    assert patch_remove.call_count == 0


@pytest.mark.asyncio
async def test_query_remote_file_raise_error(mocker, conn_mock):
    """Test _query_remote_file raises error when file is not found and missing_ok is False."""

    patch_scp = mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())

    # Don't actually try to remove result files:
    mock_async_os_remove = mock.AsyncMock(return_value=None)
    patch_remove = mocker.patch("aiofiles.os.remove", side_effect=mock_async_os_remove)

    patch_run_command_fail = mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(command="mock command", returncode=1, stdout="", stderr="")
        ),
    )

    mock_contents = b""

    def mock_open(*args):
        mock_file_object = mock.MagicMock()

        mock_file_object.__aenter__.return_value.read.return_value = mock_contents

        return mock_file_object

    patch_aiofiles_open = mocker.patch("aiofiles.open", side_effect=mock_open)

    task_results_dir = "/path/to/results_dir"
    remote_filename = "/path/to/remote_file.log"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    with pytest.raises(FileNotFoundError):
        actual_contents = await executor._query_remote_file(
            conn_mock,
            remote_filename,
            task_results_dir,
        )


@pytest.mark.asyncio
async def test_query_stdout_stderr(mocker, conn_mock):
    expected_stdout = "mock stdout"
    expected_stderr = "mock stderr"

    patch_query_remote_file = mocker.patch.object(
        PBSProExecutor, "_query_remote_file", side_effect=[expected_stdout, expected_stderr]
    )

    remote_stdout_filename = "path/to/remote_stdout"
    remote_stderr_filename = "path/to/remote_stderr"
    task_results_dir = "path/to/results_dir"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    stdout, stderr = await executor._query_stdout_stderr(
        conn_mock,
        remote_stdout_filename,
        remote_stderr_filename,
        task_results_dir,
    )

    assert stdout == expected_stdout
    assert stderr == expected_stderr
    assert patch_query_remote_file.call_count == 2
    patch_query_remote_file.assert_has_calls(
        [
            mocker.call(
                conn_mock,
                remote_stdout_filename,
                task_results_dir,
                missing_ok=True,
                binary_mode=False,
            ),
            mocker.call(
                conn_mock,
                remote_stderr_filename,
                task_results_dir,
                missing_ok=True,
                binary_mode=False,
            ),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result, exception",
    [
        ([1, 2, 3], None),
        (None, RuntimeError("mock error")),
    ],
)
async def test_query_result_no_error(mocker, conn_mock, pkl_file, result, exception):
    """Test query_result works as expected with no error."""

    # Mock the opening of specific result files:
    with open(pkl_file, "wb") as f:
        cloudpickle.dump((result, exception), f)

    with open(pkl_file, "rb") as f:
        expected_contents = f.read()
    expected_stdout = "output logs"
    expected_stderr = "output errors"

    patch_query_remote_file = mocker.patch.object(
        PBSProExecutor, "_query_remote_file", return_value=expected_contents
    )
    patch_query_stdout_stderr = mocker.patch.object(
        PBSProExecutor, "_query_stdout_stderr", return_value=(expected_stdout, expected_stderr)
    )

    current_remote_workdir = "/home/test_user/workdir"
    task_results_dir = "/path/to/results_dir"
    remote_stdout_filename = f"{current_remote_workdir}/stdout.log"
    remote_stderr_filename = f"{current_remote_workdir}/stderr.log"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=current_remote_workdir,
    )

    actual_results, actual_stdout, actual_stderr, actual_error = await executor.query_result(
        conn_mock,
        pkl_file,
        remote_stdout_filename,
        remote_stderr_filename,
        task_results_dir,
    )
    assert actual_results == result
    assert actual_stdout == expected_stdout
    assert actual_stderr == expected_stderr
    if exception is not None:
        assert type(actual_error) is type(exception)
        assert f"{actual_error}" == f"{exception}"
    else:
        assert actual_error is None

    patch_query_remote_file.assert_called_once_with(
        conn_mock, pkl_file, task_results_dir, missing_ok=False, binary_mode=True
    )
    patch_query_stdout_stderr.assert_called_once_with(
        conn_mock, remote_stdout_filename, remote_stderr_filename, task_results_dir
    )


def test_validate_qsub_args_no_error():
    """Test _validate_qsub_args works as expected with no error."""

    qsub_args = {
        "a": "mock",
        "A": "mock",
        "c": "mock",
        "e": "mock",
        "f": "mock",
        "j": "mock",
        "l": "mock",
        "m": "mock",
        "M": "mock",
        "N": "mock",
        "o": "mock",
        "p": "mock",
        "P": "mock",
        "q": "mock",
        "r": "mock",
        "R": "mock",
        "S": "mock",
        "u": "mock",
        "v": "mock",
        "V": "mock",
        "W": "mock",
    }

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        qsub_args=qsub_args,
    )

    try:
        executor._validate_qsub_args()
    except RuntimeError:
        pytest.fail("_validate_qsub_args raised RuntimeError unexpectedly.")

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        embedded_qsub_args=qsub_args,
    )

    try:
        executor._validate_qsub_args()
    except RuntimeError:
        pytest.fail("_validate_qsub_args raised RuntimeError unexpectedly.")


@pytest.mark.parametrize(
    "qsub_arg",
    [
        {"C": "mock"},
        {"G": "mock"},
        {"h": "mock"},
        {"I": "mock"},
        {"J": "mock"},
        {"k": "mock"},
        {"koe": "mock"},
        {"X": "mock"},
        {"z": ""},
    ],
)
def test_validate_qsub_args_raises_error(qsub_arg):
    """Test _validate_qsub_args raises expected error."""

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        qsub_args=qsub_arg,
    )

    with pytest.raises(RuntimeError):
        executor._validate_qsub_args()

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        embedded_qsub_args=qsub_arg,
    )

    with pytest.raises(RuntimeError):
        executor._validate_qsub_args()


@pytest.mark.parametrize(
    "remote_workdir, create_unique_workdir, skip_ssh, expected",
    [
        ("/home/test_user/workdir", False, False, "/home/test_user/workdir"),
        ("workdir", False, False, "workdir"),
        (
            "/home/test_user/workdir",
            True,
            False,
            "/home/test_user/workdir/378cdd10-8443-11ee-99e2-a35cb4664dfd/node_1",
        ),
        ("workdir", True, False, "workdir/378cdd10-8443-11ee-99e2-a35cb4664dfd/node_1"),
        ("/home/test_user/workdir", False, True, "/home/test_user/workdir"),
        ("workdir", False, True, str(Path().home() / "workdir")),
        (
            "/home/test_user/workdir",
            True,
            True,
            "/home/test_user/workdir/378cdd10-8443-11ee-99e2-a35cb4664dfd/node_1",
        ),
        (
            "workdir",
            True,
            True,
            str(Path().home() / "workdir/378cdd10-8443-11ee-99e2-a35cb4664dfd/node_1"),
        ),
    ],
)
def test_make_current_remote_workdir_path(
    remote_workdir, create_unique_workdir, skip_ssh, expected
):
    """Test _make_current_remote_workdir_path() works as expected."""

    dispatch_id = "378cdd10-8443-11ee-99e2-a35cb4664dfd"
    node_id = 1

    # Case that skip_ssh = False, remote_workdir is absolute path and create_unique_workdir = False
    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        create_unique_workdir=create_unique_workdir,
        skip_ssh=skip_ssh,
    )
    actual_result = executor._make_current_remote_workdir_path(dispatch_id, node_id)
    assert actual_result == expected


@pytest.mark.asyncio
async def test_perform_cleanup(mocker, conn_mock):
    """Test perform_cleanup works as expected."""

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    mock_command_result = _CommandResult(
        command="mock command", returncode=0, stdout="stdout", stderr="stderr"
    )
    patch_run_command = mocker.patch.object(
        PBSProExecutor, "_run_command", new=mock.AsyncMock(return_value=mock_command_result)
    )

    patch_async_os_remove = mocker.patch("aiofiles.os.remove", return_value=mock.AsyncMock())

    local_cache_dir = "/home/.cache/covalent/task1"
    remote_workdir = "/home/test_user/workdir"
    local_func_filename = str(Path(local_cache_dir, "func-local-1.pkl"))
    local_job_script_filename = str(Path(local_cache_dir, "jobscript-local-1.sh"))
    local_py_script_filename = str(Path(local_cache_dir, "script-local-1.py"))
    remote_func_filename = str(Path(remote_workdir, "func-test-1.pkl"))
    remote_job_script_filename = str(Path(remote_workdir, "job_script-test-1.sh"))
    remote_py_script_filename = str(Path(remote_workdir, "py_script-test-1.py"))
    remote_result_filename = str(Path(remote_workdir, "result-test-1.pkl"))
    remote_stdout_filename = str(Path(remote_workdir, "stdout.log"))
    remote_stderr_filename = str(Path(remote_workdir, "stderr.log"))

    executor._cleanup_files = _CleanupFiles(
        local_func_filename=local_func_filename,
        local_job_script_filename=local_job_script_filename,
        local_py_script_filename=local_py_script_filename,
        remote_func_filename=remote_func_filename,
        remote_job_script_filename=remote_job_script_filename,
        remote_py_script_filename=remote_py_script_filename,
        remote_result_filename=remote_result_filename,
        remote_stdout_filename=remote_stdout_filename,
        remote_stderr_filename=remote_stderr_filename,
    )

    await executor._perform_cleanup(conn=conn_mock)
    patch_async_os_remove.assert_has_calls(
        [
            mocker.call(local_func_filename),
            mocker.call(local_job_script_filename),
            mocker.call(local_py_script_filename),
        ],
        any_order=True,
    )
    assert patch_async_os_remove.call_count == 3
    patch_run_command.assert_has_calls(
        [
            mocker.call(conn_mock, f"rm {remote_func_filename}"),
            mocker.call(conn_mock, f"rm {remote_job_script_filename}"),
            mocker.call(conn_mock, f"rm {remote_py_script_filename}"),
            mocker.call(conn_mock, f"rm {remote_result_filename}"),
            mocker.call(conn_mock, f"rm {remote_stdout_filename}"),
            mocker.call(conn_mock, f"rm {remote_stderr_filename}"),
        ],
        any_order=True,
    )
    assert patch_run_command.call_count == 6


@pytest.mark.asyncio
async def test_perform_cleanup_cleanup_files_is_None(mocker, conn_mock):
    """Test perform_cleanup works as expected when _cleanup_files is None."""

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    mock_command_result = _CommandResult(
        command="mock command", returncode=0, stdout="stdout", stderr="stderr"
    )
    patch_run_command = mocker.patch.object(
        PBSProExecutor, "_run_command", new=mock.AsyncMock(return_value=mock_command_result)
    )

    patch_async_os_remove = mocker.patch("aiofiles.os.remove", return_value=mock.AsyncMock())

    await executor._perform_cleanup(conn=conn_mock)
    assert patch_async_os_remove.call_count == 0
    assert patch_run_command.call_count == 0


@pytest.mark.asyncio
async def test_perform_cleanup_skip_ssh(mocker):
    """Test perform_cleanup works as expected."""

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )
    mock_command_result = _CommandResult(
        "mock command", returncode=0, stdout="stdout", stderr="stderr"
    )
    patch_run_command = mocker.patch.object(
        PBSProExecutor, "_run_command", new=mock.AsyncMock(return_value=mock_command_result)
    )

    patch_async_os_remove = mocker.patch("aiofiles.os.remove", return_value=mock.AsyncMock())

    local_cache_dir = "/home/.cache/covalent/task1"
    remote_workdir = "/home/test_user/workdir"
    local_func_filename = str(Path(local_cache_dir, "func-local-1.pkl"))
    local_job_script_filename = str(Path(local_cache_dir, "jobscript-local-1.sh"))
    local_py_script_filename = str(Path(local_cache_dir, "script-local-1.py"))
    remote_func_filename = str(Path(remote_workdir, "func-test-1.pkl"))
    remote_job_script_filename = str(Path(remote_workdir, "job_script-test-1.sh"))
    remote_py_script_filename = str(Path(remote_workdir, "py_script-test-1.py"))
    remote_result_filename = str(Path(remote_workdir, "result-test-1.pkl"))
    remote_stdout_filename = str(Path(remote_workdir, "stdout.log"))
    remote_stderr_filename = str(Path(remote_workdir, "stderr.log"))

    executor._cleanup_files = _CleanupFiles(
        local_func_filename=local_func_filename,
        local_job_script_filename=local_job_script_filename,
        local_py_script_filename=local_py_script_filename,
        remote_func_filename=remote_func_filename,
        remote_job_script_filename=remote_job_script_filename,
        remote_py_script_filename=remote_py_script_filename,
        remote_result_filename=remote_result_filename,
        remote_stdout_filename=remote_stdout_filename,
        remote_stderr_filename=remote_stderr_filename,
    )

    await executor._perform_cleanup(conn=None)
    patch_async_os_remove.assert_has_calls(
        [
            mocker.call(local_func_filename),
            mocker.call(local_job_script_filename),
            mocker.call(local_py_script_filename),
        ],
        any_order=True,
    )
    assert patch_async_os_remove.call_count == 3
    patch_run_command.assert_has_calls(
        [
            mocker.call(None, f"rm {remote_func_filename}"),
            mocker.call(None, f"rm {remote_job_script_filename}"),
            mocker.call(None, f"rm {remote_py_script_filename}"),
            mocker.call(None, f"rm {remote_result_filename}"),
            mocker.call(None, f"rm {remote_stdout_filename}"),
            mocker.call(None, f"rm {remote_stderr_filename}"),
        ],
        any_order=True,
    )
    assert patch_run_command.call_count == 6


@pytest.mark.asyncio
async def test_teardown_success(mocker, conn_mock):
    """Test teardown works as expected."""

    executor_not_cleanup = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        cleanup=False,
    )

    conn_mock.wait_closed = mock.AsyncMock()

    mocker.patch.object(
        PBSProExecutor, "_client_connect", new=mock.AsyncMock(return_value=conn_mock)
    )

    patch_perform_cleanup = mocker.patch.object(
        PBSProExecutor, "_perform_cleanup", new=mock.AsyncMock(return_value=None)
    )

    # Test _perform_cleanup is not called if cleanup is False.
    await executor_not_cleanup.teardown(task_metadata={})
    assert patch_perform_cleanup.call_count == 0

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        cleanup=True,
    )

    # Test _perform_cleanup is called if cleanup is True.
    await executor.teardown(task_metadata={})
    assert patch_perform_cleanup.call_count == 1


@pytest.mark.asyncio
async def test_teardown_handling_error(conn_mock):
    """Test teardown handles exceptions as expected."""

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        cleanup=True,
    )

    conn_mock.wait_closed = mock.AsyncMock()

    patch_client_connect_succeeded = mock.patch.object(
        PBSProExecutor, "_client_connect", new=mock.AsyncMock(return_value=conn_mock)
    )
    patch_client_connect_failed = mock.patch.object(
        PBSProExecutor, "_client_connect", side_effect=RuntimeError("failed _client_connect")
    )

    patch_perform_cleanup_failed = mock.patch.object(
        PBSProExecutor, "_perform_cleanup", side_effect=Exception("Mock error")
    )

    # teardown does not raise an exception.
    with patch_client_connect_failed:
        await executor.teardown(task_metadata={})

    with patch_client_connect_succeeded, patch_perform_cleanup_failed:
        await executor.teardown(task_metadata={})


@pytest.mark.asyncio
async def test_cancel(mocker, conn_mock):
    """Test that cancel works as expected."""

    mocker.patch.object(
        PBSProExecutor, "_client_connect", new=mock.AsyncMock(return_value=conn_mock)
    )

    conn_mock.close = mock.MagicMock(return_value=None)
    conn_mock.wait_closed = mock.AsyncMock(return_value=None)

    job_id = "1.hostname"

    executor = PBSProExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
    )

    # Check whether no error is raised in case that executing qdel is succeeded.
    # Also check whether executed command is correct.
    patch_run_command_succeeded = mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(
                command="mock command", returncode=0, stdout="stdout", stderr=""
            )
        ),
    )
    try:
        await executor.cancel({}, job_id)
    except Exception:
        pytest.fail("cancel raised an Error unexpectedly.")
    patch_run_command_succeeded.assert_called_with(conn_mock, f"qdel {job_id}")

    patch_run_command_failed = mocker.patch.object(
        PBSProExecutor,
        "_run_command",
        new=mock.AsyncMock(
            return_value=_CommandResult(
                command="mock command", returncode=1, stdout="stdout", stderr=""
            )
        ),
    )

    # Check whether no error is raised in case that executing qdel is failed.
    try:
        await executor.cancel({}, job_id)
    except Exception:
        pytest.fail("cancel raised an Error unexpectedly.")
    patch_run_command_failed.assert_called_with(conn_mock, f"qdel {job_id}")

    # Check whether no error is raised in case that job_id is None.
    try:
        await executor.cancel({}, None)
    except Exception:
        pytest.fail("cancel raised an Error unexpectedly.")
    patch_run_command_failed.call_count == 0
