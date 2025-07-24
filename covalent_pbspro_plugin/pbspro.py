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

"""PBS Professional executor plugin for the Covalent dispatcher."""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Callable

import aiofiles
import asyncssh
import cloudpickle as pickle
from aiofiles import os as async_os
from covalent._results_manager.result import Result
from covalent._shared_files import logger
from covalent._shared_files.config import get_config
from covalent._shared_files.exceptions import TaskCancelledError, TaskRuntimeError
from covalent.executor.executor_plugins.remote_executor import RemoteExecutor

from covalent_pbspro_plugin.job_script import JobScript

app_log = logger.app_log
log_stack_info = logger.log_stack_info

executor_plugin_name = "PBSProExecutor"

_EXECUTOR_PLUGIN_DEFAULTS = {
    "username": "",
    "address": "",
    "port": 22,
    "bashrc_path": "$HOME/.bashrc",
    "qsub_args": {},
    "embedded_qsub_args": {},
    "cleanup": True,
    "skip_ssh": False,
    "remote_workdir": "covalent-workdir",
    "create_unique_workdir": False,
    "cache_dir": str(Path(get_config("dispatcher.cache_dir")).expanduser().resolve()),
    "poll_freq": 60,
    "remote_cache": ".cache/covalent",
    "log_stdout": "stdout.log",
    "log_stderr": "stderr.log",
}


class _JobStateCategory(Enum):
    """Enum of classified status of jobs submitted to PBS Professional."""

    QUEUED = auto()
    ACTIVE = auto()
    COMPLETED = auto()
    FAILED = auto()


@dataclass
class _JobStats:
    """Helper class to store job stats obtained by executing `qstat`.

    Attributes:
        job_state_category (_JobStateCategory): Classified job state.
        job_state (str): Raw job state string.
        substate (int): Substate of the job.
        exit_status (int | None): Exit code of the job.
        stdout_path (str): stdout log file path.
        stderr_path (str): stderr log file path.
    """

    job_state_category: _JobStateCategory
    job_state: str
    substate: int
    exit_status: int | None
    stdout_path: str
    stderr_path: str


@dataclass
class _CleanupFiles:
    """Helper class to store files to be cleaned up."""

    local_func_filename: str | None = None
    local_job_script_filename: str | None = None
    local_py_script_filename: str | None = None
    remote_func_filename: str | None = None
    remote_job_script_filename: str | None = None
    remote_py_script_filename: str | None = None
    remote_result_filename: str | None = None
    remote_stdout_filename: str | None = None
    remote_stderr_filename: str | None = None


@dataclass
class _CommandResult:
    """Helper class to store the result of a command execution."""

    command: str
    returncode: int
    stdout: str | None
    stderr: str | None


class PBSProExecutor(RemoteExecutor):
    """PBS professional executor plugin class.

    Note: This plugin does not support the following qsub options. If any of the following options are set in qsub_args or embedded_qsub_args, this plugin will generate a RuntimeError when executing the task.
        * -C
        * -G
        * -h
        * -I
        * -J
        * -k
        * -X
        * -z

    Args:
        username: Username used to authenticate over SSH (i.e. what you use to login to `address`).
        address: Remote address or hostname of the PBS Professional login node.
        port: Remote port of the PBS Professional login node.
        ssh_key_file: Private key used to authenticate over SSH.
            Note:
                This key will be used after trying keys from a PKCS11 provider or an ssh-agent, if either of those are configured.
                Please refer to the following documentation for more information.
                https://asyncssh.readthedocs.io/en/stable/api.html#ssh-agent-support
        cert_file: Certificate file used to authenticate over SSH, if required (usually has extension .pub).
        passphrase: passphrase used to decrypt Private key when loading them, if required.
            Note:
                Hard-coding your passphrase is not advised.
                Please consider alternative ways to avoid hard-coding such as loading from environment variables or using ssh-agent.
        bashrc_path: Path to the bashrc file to source before running the function.
            Note:
                If you do not want to source .bashrc, please pass an empty string (`""`) explicitly.
        prerun_commands: List of shell commands to run before running the pickled function.
        postrun_commands: List of shell commands to run after running the pickled function.
        qsub_args: Dictionary of args used when executing command `qsub` on remote machine.
        embedded_qsub_args: Dictionary of args embedded into the submit script.
        cleanup: Whether to perform cleanup or not on remote machine.
        skip_ssh: Whether to run the task via PBS Professional without ssh. If True, the task will be submitted to PBS Professional without ssh on the same machine as the Covalent server is running.
        remote_workdir: Working directory on the remote cluster.
            Note:
                It can be specified as either an absolute or a relative path.
                When specified as a relative path, the location where it is created depends on the value of skip_ssh.
                If skip_ssh=True, the directory will be created under the user home directory on the machine where the Covalent server is running.
                If skip_ssh=False, the directory will be created under the user home directory on the machine where the computation is executed..
        create_unique_workdir: Whether to create a unique working (sub)directory for each task.
        cache_dir: Local cache directory used by this executor for temporary files.
        poll_freq: Frequency with which to poll a submitted job. Always is >= 30.
        remote_cache: Remote server cache directory used for temporary files.
        log_stdout: The path to the file to be used for redirecting stdout.
        log_stderr: The path to the file to be used for redirecting stderr.
        time_limit: time limit for the task.
        retries: Number of times to retry execution upon failure.
    """

    def __init__(
        self,
        # SSH credentials
        username: str | None = None,
        address: str | None = None,
        port: int | None = None,
        ssh_key_file: str | None = None,
        cert_file: str | None = None,
        passphrase: str | None = None,
        # executor parameters
        bashrc_path: str | None = None,
        prerun_commands: list[str] | None = None,
        postrun_commands: list[str] | None = None,
        qsub_args: dict[str, str | list[str] | tuple[str, ...]] | None = None,
        embedded_qsub_args: dict[str, str | list[str] | tuple[str, ...]] | None = None,
        cleanup: bool | None = None,
        skip_ssh: bool | None = None,
        # Covalent parameters
        remote_workdir: str | None = None,
        create_unique_workdir: bool | None = None,
        cache_dir: str | None = None,
        # RemoteExecutor parameters
        poll_freq: int | None = None,
        remote_cache: str = "",
        log_stdout: str = "",
        log_stderr: str = "",
        time_limit: int = -1,
        retries: int = 0,
        *args,
        **kwargs,
    ) -> None:
        poll_freq = poll_freq or get_config("executors.pbspro.poll_freq")

        if poll_freq is not None and poll_freq < 30:
            print("Polling frequency will be increased to 30 seconds.")
            poll_freq = 30

        remote_cache = remote_cache or get_config("executors.pbspro.remote_cache")
        log_stdout = log_stdout or get_config("executors.pbspro.log_stdout")
        log_stderr = log_stderr or get_config("executors.pbspro.log_stderr")

        super().__init__(
            poll_freq=poll_freq,
            remote_cache=remote_cache,
            log_stdout=log_stdout,
            log_stderr=log_stderr,
            time_limit=time_limit,
            retries=retries,
        )

        # SSH credentials
        self.username = username or get_config("executors.pbspro.username")
        self.address = address or get_config("executors.pbspro.address")
        self.port = port or get_config("executors.pbspro.port")

        try:
            self.ssh_key_file = ssh_key_file or get_config("executors.pbspro.ssh_key_file")
            self.ssh_key_file = str(Path(self.ssh_key_file).expanduser().resolve())
        except KeyError:
            self.ssh_key_file = None

        try:
            self.cert_file = cert_file or get_config("executors.pbspro.cert_file")
            self.cert_file = str(Path(self.cert_file).expanduser().resolve())
        except KeyError:
            self.cert_file = None

        try:
            self.passphrase = passphrase or get_config("executors.pbspro.passphrase")
        except KeyError:
            self.passphrase = None

        # Covalent parameters
        self.remote_workdir = remote_workdir or get_config("executors.pbspro.remote_workdir")
        self.create_unique_workdir = (
            create_unique_workdir
            if create_unique_workdir is not None
            else get_config("executors.pbspro.create_unique_workdir")
        )

        self.cache_dir = cache_dir or get_config("executors.pbspro.cache_dir")
        self.cache_dir = str(Path(self.cache_dir).expanduser().resolve())

        # Make sure local cache dir exists
        os.makedirs(self.cache_dir, exist_ok=True)

        # executor parameters

        # Allow user to override bashrc_path with empty string.
        try:
            self.bashrc_path = (
                ""
                if bashrc_path == ""
                else (bashrc_path or get_config("executors.pbspro.bashrc_path"))
            )
        except KeyError:
            self.bashrc_path = None

        try:
            self.prerun_commands = (
                list(prerun_commands)
                if prerun_commands
                else get_config("executors.pbspro.prerun_commands")
            )
        except KeyError:
            self.prerun_commands = []

        try:
            self.postrun_commands = (
                list(postrun_commands)
                if postrun_commands
                else get_config("executors.pbspro.postrun_commands")
            )
        except KeyError:
            self.postrun_commands = []

        # Allow empty dictionary to be passed to qsub_args
        if qsub_args is None:
            qsub_args = get_config("executors.pbspro.qsub_args")
        self.qsub_args = deepcopy(qsub_args)

        # Allow empty dictionary to be passed to embedded_qsub_args
        if embedded_qsub_args is None:
            embedded_qsub_args = get_config("executors.pbspro.embedded_qsub_args")
        self.embedded_qsub_args = deepcopy(embedded_qsub_args)

        self.cleanup = cleanup if cleanup is not None else get_config("executors.pbspro.cleanup")

        self.skip_ssh = (
            skip_ssh if skip_ssh is not None else get_config("executors.pbspro.skip_ssh")
        )

        self._cleanup_files = None

    async def _run_command(
        self, conn: asyncssh.SSHClientConnection | None, command: str
    ) -> _CommandResult:
        """wrapper function for running commands on a remote machine or the machine where the covalent server is running. If conn is None, run on the machine where the covalent server is running.

        Args:
            conn: Connection object to connect the the remote machine.
            command: Command to be executed.
        Returns:
            _CommandResult object containing the output of the command.
        """

        if conn is None:
            proc, stdout, stderr = await self.run_async_subprocess(command)

            return _CommandResult(
                command=command,
                returncode=proc.returncode,
                stdout=stdout.decode(encoding="utf-8"),
                stderr=stderr.decode(encoding="utf-8"),
            )
        else:
            proc = await conn.run(command)
            return _CommandResult(
                command=command, returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr
            )

    async def _validate_credentials(self) -> bool:
        """Validates the credentials required to establish SSH connections, including the username, address, SSH key file (if specified), and certification file (if specified).

        Args:
            None

        Returns:
            boolean indicating if the str parameters are non-empty strings and the specified files exist.

        Raises:
            ValueError: If one of required parameters (username and address) is None or empty string.
            FileNotFoundError: If the specified files do not exist.
        """
        if not self.username:
            raise ValueError("username is a required parameter in the PBS Professional plugin.")

        if not self.address:
            raise ValueError("address is a required parameter in the PBS Professional plugin.")

        if (self.ssh_key_file is not None) and (not Path(self.ssh_key_file).is_file()):
            raise FileNotFoundError(
                f"SSH key file {self.ssh_key_file} is assigned but does not exist."
            )

        if (self.cert_file is not None) and (not Path(self.cert_file).is_file()):
            raise FileNotFoundError(
                f"Certificate file {self.cert_file} is assigned but does not exist."
            )

        return True

    async def _client_connect(self) -> asyncssh.SSHClientConnection | None:
        """Helper function for connecting to the remote host through asyncssh module.
        if `skip_ssh` is True, this function returns None.

        Args:
            None

        Returns:
            The connection object

        Raises:
            RuntimeError: If SSH connection could not be established.
        """

        if self.skip_ssh:
            return None

        await self._validate_credentials()

        client_keys = Path(self.ssh_key_file).expanduser().resolve() if self.ssh_key_file else ()
        client_certs = Path(self.cert_file).expanduser().resolve() if self.cert_file else ()

        try:
            conn = await asyncssh.connect(
                host=self.address,
                port=self.port,
                username=self.username,
                client_keys=client_keys,
                client_certs=client_certs,
                passphrase=self.passphrase,
                known_hosts=None,
            )

        except Exception as e:
            raise RuntimeError(
                f"Could not connect to host: '{self.address}' port: '{self.port}' as user: '{self.username}'",
                e,
            )

        return conn

    def _format_submit_script(
        self,
        python_version: str,
        covalent_version: str,
        py_script_filename: str,
        func_filename: str,
        result_filename: str,
        current_remote_workdir: str,
    ) -> str:
        """Create the shell script that defines the job, that runs the python script.

        Args:
            python_version: Python version required by the pickled function.
            covalent_version: covalent version of covalent server.
            py_script_filename: Name of the python script.
            func_filename: Name of the pickled function.
            result_filename: Name of the pickled result.
            current_remote_workdir: Current working directory on the remote machine.

        Returns:
            script: String object containing a script.
        """

        job_script = JobScript(
            embedded_qsub_args=self.embedded_qsub_args,
            bashrc_path=self.bashrc_path,
            prerun_commands=self.prerun_commands,
            postrun_commands=self.postrun_commands,
        )

        return job_script.format(
            python_version=python_version,
            covalent_version=covalent_version,
            py_script_filename=py_script_filename,
            func_filename=func_filename,
            result_filename=result_filename,
            current_remote_workdir=current_remote_workdir,
        )

    async def _upload_task(
        self,
        conn: asyncssh.SSHClientConnection | None,
        func_filename: str,
        py_script_filename: str,
        submit_script_filename: str,
        current_remote_workdir: str,
    ) -> None:
        """Upload the required files to the remote machine using SCP.
        When skip_ssh is True, copy the required files to the working directory on the covalent server.

        Args:
            conn: Connection object to connect the the remote machine
            func_filename: Path to the function file to be uploaded.
            py_script_filename: Path to the python script runs the function.
            submit_script_filename: Path to the script that will be submitted to PBS Professional.
            current_remote_workdir: Current working directory on the remote machine. files uploaded under this directory.

        Returns:
            None
        """

        if conn is None:
            shutil.copy(func_filename, current_remote_workdir)
            shutil.copy(py_script_filename, current_remote_workdir)
            shutil.copy(submit_script_filename, current_remote_workdir)
        else:
            await asyncssh.scp(func_filename, (conn, current_remote_workdir))
            await asyncssh.scp(py_script_filename, (conn, current_remote_workdir))
            await asyncssh.scp(submit_script_filename, (conn, current_remote_workdir))

    async def submit_task(
        self,
        conn: asyncssh.SSHClientConnection | None,
        remote_submit_script_filename: str,
    ) -> _CommandResult:
        """Submit the task to PBS Professional with `qsub` and return the corresponding output.

        Args:
            conn: Connection object to connect to the remote machine.
            remote_submit_script_filename: Path to the remote submit script file.

        Returns:
            _CommandResult: Containing information about the output of executing `qsub`.
        """

        qsub_args_list = []
        for key, value in self.qsub_args.items():
            if isinstance(value, (list, tuple)):
                for arg_value in value:
                    qsub_args_list.append(f"-{key}" + (f" {arg_value}" if arg_value else ""))
            else:
                qsub_args_list.append(f"-{key}" + (f" {value}" if value else ""))

        qsub_args_str = " ".join(qsub_args_list)

        submit_command = "qsub"
        if qsub_args_str:
            submit_command += f" {qsub_args_str}"
        submit_command += f" {remote_submit_script_filename}"

        return await self._run_command(conn, submit_command)

    async def get_status(
        self, conn: asyncssh.SSHClientConnection | None, job_id: str
    ) -> _JobStats:
        """Query the status of a job previously submitted to PBS Professional with executing `qstat -x -f -F json <job_id>`.

        For more information about job states, see https://help.altair.com/2024.1.0/PBS%20Professional/PBSReferenceGuide2024.1.pdf
        Page 357, section 8.1 "Job States"

        Args:
            conn: SSH connection object.
            job_id: PBS Professional job ID.

        Returns:
            _JobStats object that has the stats of the job such as job_state, substate, and exit_status.

        Raises:
            RuntimeError: If failed to execute `qstat -x -f -F json <job_id>` on the remote machine.
        """

        qstat_cmd = f"qstat -x -f -F json {job_id}"
        qstat_proc = await self._run_command(conn, qstat_cmd)

        if qstat_proc.returncode != 0:
            raise RuntimeError(
                f"returncode: {qstat_proc.returncode}, stderr: {str(qstat_proc.stderr)}"
            )

        job_info = json.loads(str(qstat_proc.stdout).strip())
        raw_job_state = job_info["Jobs"][job_id]["job_state"]
        stdout_path = re.sub(r"^[^:]*:", "", job_info["Jobs"][job_id]["Output_Path"])
        stderr_path = re.sub(r"^[^:]*:", "", job_info["Jobs"][job_id]["Error_Path"])

        job_state_category = None
        substate = job_info["Jobs"][job_id]["substate"]
        exit_status = job_info["Jobs"][job_id].get("Exit_status", None)

        if raw_job_state == "F":
            if substate == 92 and exit_status == 0:
                job_state_category = _JobStateCategory.COMPLETED
            else:
                job_state_category = _JobStateCategory.FAILED
        elif raw_job_state in {"Q", "H", "M", "S", "T", "U", "W"}:
            job_state_category = _JobStateCategory.QUEUED
        elif raw_job_state in {"E", "R"}:
            job_state_category = _JobStateCategory.ACTIVE
        else:
            raise RuntimeError(f"get_status encounters an unknown job state {raw_job_state}")

        return _JobStats(
            job_state_category=job_state_category,
            job_state=raw_job_state,
            substate=substate,
            exit_status=exit_status,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )

    async def _poll_task(
        self, conn: asyncssh.SSHClientConnection | None, job_id: str
    ) -> _JobStats:
        """Poll a PBS Professional job until completion.

        Args:
            conn: SSH connection object.
            job_id: PBS Professional job ID.

        Returns:
            _JobStats object that has the final stats of the job such as job_state, substate, and exit_status.
        """

        job_stats = await self.get_status(conn, job_id)

        while (
            job_stats.job_state_category == _JobStateCategory.QUEUED
            or job_stats.job_state_category == _JobStateCategory.ACTIVE
        ):
            await asyncio.sleep(self.poll_freq)
            job_stats = await self.get_status(conn, job_id)

        return job_stats

    async def _query_remote_file(
        self,
        conn: asyncssh.SSHClientConnection | None,
        remote_filename: str,
        task_results_dir: str,
        missing_ok: bool = False,
        binary_mode: bool = False,
    ) -> bytes:
        """Helper function that copies the remote file to the Covalent server and reads the file.
        If conn is None, this function will read the file of remote_filename on the covalent server directly.

        Args:
            conn: SSH connection object.
            remote_filename: Target file path on the remote machine.
            task_results_dir: Destination directory on the Covalent server where you want to copy the target files.
            missing_ok: Flag if to handle error in case the target file is not found. If True, this function does not raise FileNotFoundError.
            binary_mode: Flag if to read the file in binary mode. If True, the file is read in binary mode.
        Returns:
            bytes: Target file contents. If the file is not found and missing_ok is True, returns empty bytes.
        """

        app_log.debug(f"querying {remote_filename}")

        # Check the file exists on the remote machine.
        proc = await self._run_command(conn, f"test -e {remote_filename}")

        if not missing_ok and proc.returncode != 0:
            raise FileNotFoundError(proc.returncode, str(proc.stderr).strip(), remote_filename)

        contents = b""
        mode = "rb" if binary_mode else "r"
        if proc.returncode == 0:
            if conn is None:
                # Read the target file on the covalent server.
                async with aiofiles.open(remote_filename, mode) as f:
                    contents = await f.read()
            else:
                local_result_dir = Path(task_results_dir)

                # Copy file.
                local_filename = local_result_dir / Path(remote_filename).name
                await asyncssh.scp((conn, remote_filename), local_filename)

                # Read contents.
                async with aiofiles.open(local_filename, mode) as f:
                    contents = await f.read()

                # Remove copied file on Covalent server.
                await async_os.remove(local_filename)

        return contents

    async def _query_stdout_stderr(
        self,
        conn: asyncssh.SSHClientConnection | None,
        remote_stdout_filename: str,
        remote_stderr_filename: str,
        task_results_dir: str,
    ) -> tuple[bytes, bytes]:
        """Query and retrieve the task stdout and stderr logs.

        Args:
            conn: SSH connection object.
            remote_stdout_filename: Name of the log file of stdout on the remote machine.
            remote_stderr_filename: Name of the log file of stderr on the remote machine.
            task_results_dir: Directory on the Covalent server where the result will be copied.
        Returns:
            stdout: stdout log.
            stderr: stderr log.
        """

        stdout = await self._query_remote_file(
            conn,
            remote_stdout_filename,
            task_results_dir,
            missing_ok=True,
            binary_mode=False,
        )
        stderr = await self._query_remote_file(
            conn,
            remote_stderr_filename,
            task_results_dir,
            missing_ok=True,
            binary_mode=False,
        )

        return stdout, stderr

    async def query_result(
        self,
        conn: asyncssh.SSHClientConnection | None,
        remote_result_filename: str,
        remote_stdout_filename: str,
        remote_stderr_filename: str,
        task_results_dir: str,
    ) -> tuple[Result, bytes, bytes, Exception]:
        """Query and retrieve the task result including stdout and stderr logs.

        Args:
            conn: SSH connection object.
            remote_result_filename: Name of the pickled result file on the remote_machine.
            remote_stdout_filename: Name of the log file of stdout on the remote machine.
            remote_stderr_filename: Name of the log file of stderr on the remote machine.
            task_results_dir: Directory on the Covalent server where the result will be copied.
        Returns:
            result: Task result.
            stdout: stdout log.
            stderr: stderr log.
            exception: Exception raised during task execution.
        """

        result_content = await self._query_remote_file(
            conn,
            remote_result_filename,
            task_results_dir,
            missing_ok=False,
            binary_mode=True,
        )
        result, exception = pickle.loads(result_content)

        stdout, stderr = await self._query_stdout_stderr(
            conn, remote_stdout_filename, remote_stderr_filename, task_results_dir
        )

        return result, stdout, stderr, exception

    def _validate_qsub_args(self) -> None:
        """Helper function to validate following unsupported qsub arguments.

        * -C
        * -G
        * -h
        * -I
        * -J
        * -k
        * -X
        * -z

        Args:
            None
        Returns:
            None
        Raises:
            RuntimeError: If qsub_args or embedded_qsub_args contain unsupported arguments.
        """

        unsupported_args = ("C", "G", "h", "I", "J", "k", "X", "z")
        for key in self.qsub_args.keys():
            if key.startswith(unsupported_args):
                raise RuntimeError(
                    f"`{key}` arg is included in qsub_args. PBSProExecutor does not support this argument of qsub command."
                )
        for key in self.embedded_qsub_args.keys():
            if key.startswith(unsupported_args):
                raise RuntimeError(
                    f"`{key}` arg is included in embedded_qsub_args. PBSProExecutor does not support this argument of qsub command."
                )

    def _make_current_remote_workdir_path(self, dispatch_id: str, node_id: int) -> str:
        """Helper function that make path of current_remote_workdir.
        If self.skip_ssh is True and self.remote_workdir is a relative path,
        current_remote_workdir is a relative path from the home directory of the machine that the covalent server is running on.
        Also, if self.create_unique_workdir is True, current_remote_workdir is a subdirectory of remote_workdir named by dispatch_id and node_id.

        Args:
            dispatch_id: Dispatch ID of the workflow.
            node_id: Node ID of the task in the workflow.

        """
        current_remote_workdir = Path(self.remote_workdir)
        if self.skip_ssh and not current_remote_workdir.is_absolute():
            current_remote_workdir = Path().home() / current_remote_workdir
        if self.create_unique_workdir:
            current_remote_workdir = current_remote_workdir / dispatch_id / f"node_{node_id}"

        return str(current_remote_workdir)

    async def run(
        self, function: Callable, args: list, kwargs: dict, task_metadata: dict
    ) -> Result | None:
        """Run a function on a remote machine using PBS Professional.

        Args:
            function: Function to be executed.
            args: List of positional arguments to be passed to the function.
            kwargs: Dictionary of keyword arguments to be passed to the function.
            task_metadata: Dictionary of metadata associated with the task.

        Returns:
            result: Result object containing the result of the function execution.

        Raises:
            RuntimeError: If fail to make remote_workdir on remote machine or fail to submit the task.
            TaskRuntimeError: If the task is failed.
        """

        self._validate_qsub_args()

        dispatch_id = task_metadata["dispatch_id"]
        node_id = task_metadata["node_id"]
        results_dir = task_metadata["results_dir"]
        task_results_dir = Path(results_dir) / dispatch_id

        async def _check_cancel_request() -> None:
            if await self.get_cancel_requested():
                app_log.debug(f"Task {dispatch_id}-{node_id} has been cancelled don't proceed")
                raise TaskCancelledError(
                    f"Task {dispatch_id}-{node_id} requested to be cancelled."
                )

        current_remote_workdir = self._make_current_remote_workdir_path(dispatch_id, node_id)

        # Specify file names.
        result_filename = f"result-{dispatch_id}-{node_id}.pkl"
        job_script_filename = f"jobscript-{dispatch_id}-{node_id}.sh"
        py_script_filename = f"script-{dispatch_id}-{node_id}.py"
        func_filename = f"func-{dispatch_id}-{node_id}.pkl"

        result = None

        await _check_cancel_request()

        conn = await self._client_connect()

        version_info = await self.get_version_info()
        app_log.debug(f"{version_info=}")
        covalent_version = version_info["covalent"]
        py_version_func = ".".join(function.args[0].python_version.split(".")[:2])
        app_log.debug(f"Python version: {py_version_func}, Covalent version: {covalent_version}")

        await _check_cancel_request()

        # Create the remote directory
        app_log.debug(f"Creating remote work directory {current_remote_workdir} ...")
        cmd_mkdir_remote = f"mkdir -p {current_remote_workdir}"
        proc_mkdir_remote = await self._run_command(conn, cmd_mkdir_remote)

        if proc_mkdir_remote.returncode != 0:
            raise RuntimeError(
                f"Cannot make the directory {current_remote_workdir}. stderr: {str(proc_mkdir_remote.stderr).strip()}"
            )

        async with aiofiles.tempfile.NamedTemporaryFile(
            dir=self.cache_dir, mode="wb", delete=False
        ) as temp_func_file:
            # Pickle the function and write to file
            app_log.debug("Writing pickled function, args, kwargs to file...")
            await temp_func_file.write(pickle.dumps((function, args, kwargs)))
            await temp_func_file.flush()
            local_func_filename = str(Path(temp_func_file.name).parent / func_filename)
            os.rename(temp_func_file.name, local_func_filename)
            app_log.debug(f"Made pickle file of the function {local_func_filename}")

        async with aiofiles.tempfile.NamedTemporaryFile(
            dir=self.cache_dir, mode="w", delete=False
        ) as temp_py_script_file:
            # Format the function execution script and write to file
            python_exec_script = (Path(__file__).parent / "exec.py").read_text("utf-8")
            app_log.debug("Writing python run-function script to tempfile...")
            await temp_py_script_file.write(python_exec_script)
            await temp_py_script_file.flush()
            local_py_script_filename = str(
                Path(temp_py_script_file.name).parent / py_script_filename
            )
            os.rename(temp_py_script_file.name, local_py_script_filename)
            app_log.debug(f"Made Python script file {local_py_script_filename}")

        async with aiofiles.tempfile.NamedTemporaryFile(
            dir=self.cache_dir, mode="w", delete=False
        ) as temp_job_script_file:
            # Format the job script and write to file
            submit_script = self._format_submit_script(
                python_version=py_version_func,
                covalent_version=covalent_version,
                py_script_filename=py_script_filename,
                func_filename=func_filename,
                result_filename=result_filename,
                current_remote_workdir=current_remote_workdir,
            )
            app_log.debug("Writing PBS Professional job script to tempfile...")
            await temp_job_script_file.write(submit_script)
            await temp_job_script_file.flush()
            local_job_script_filename = str(
                Path(temp_job_script_file.name).parent / job_script_filename
            )
            os.rename(temp_job_script_file.name, local_job_script_filename)
            app_log.debug(f"Made job script file {local_job_script_filename}")

        await _check_cancel_request()

        # Copy files to the remote machine
        await self._upload_task(
            conn,
            local_func_filename,
            local_py_script_filename,
            local_job_script_filename,
            current_remote_workdir,
        )

        app_log.debug(f"Uploaded task files to {current_remote_workdir}")

        remote_func_filename = str(Path(current_remote_workdir, func_filename))
        remote_py_script_filename = str(Path(current_remote_workdir, py_script_filename))
        remote_job_script_filename = str(Path(current_remote_workdir, job_script_filename))

        await _check_cancel_request()

        proc_submit_task = await self.submit_task(conn, remote_job_script_filename)

        if proc_submit_task.returncode != 0:
            raise RuntimeError(
                f"Failed to submit task. stderr: {str(proc_submit_task.stderr).strip()}"
            )

        submit_task_stdout = str(proc_submit_task.stdout).strip()

        app_log.debug(f"Job submitted with stdout: {submit_task_stdout}")

        job_id = submit_task_stdout.strip()

        await self.set_job_handle(handle=job_id)

        app_log.debug(f"Polling PBS Professional with job_id: {job_id} ...")
        job_stat = await self._poll_task(conn, job_id)

        await _check_cancel_request()

        app_log.debug(f"Querying result with job_id: {job_id} ...")
        remote_result_filename = str(Path(current_remote_workdir, result_filename))
        remote_stdout_filename = job_stat.stdout_path
        remote_stderr_filename = job_stat.stderr_path

        if job_stat.job_state_category == _JobStateCategory.COMPLETED:
            result, stdout, stderr, exception = await self.query_result(
                conn,
                remote_result_filename,
                remote_stdout_filename,
                remote_stderr_filename,
                task_results_dir,
            )

            print(stdout, end="", file=self._task_stdout)
            print(stderr, end="", file=self._task_stderr)

            if exception:
                app_log.debug(f"An exception has occurred in the task {dispatch_id}-{node_id}:")
                app_log.debug(f"Exception of Task: {type(exception).__name__}: {exception}")
                print(f"{type(exception).__name__}: {exception}", end="", file=self._task_stderr)

                raise TaskRuntimeError from exception
        else:
            app_log.debug(
                f"Job FAILED with state: {job_stat.job_state}, substate: {job_stat.substate}, state_category: {job_stat.job_state_category}."
            )
            stdout, stderr = await self._query_stdout_stderr(
                conn, remote_stdout_filename, remote_stderr_filename, task_results_dir
            )

            print(stdout, end="", file=self._task_stdout)
            print(stderr, end="", file=self._task_stderr)

            raise TaskRuntimeError

        app_log.debug("Preparing for teardown")
        self._cleanup_files = _CleanupFiles(
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

        if conn is not None:
            app_log.debug("Closing SSH connection...")
            conn.close()
            await conn.wait_closed()

            app_log.debug("SSH connection closed, returning result")

        return result

    async def cancel(self, task_metadata: dict, job_id: str | None) -> bool:
        """
        Cancel the job.

        Args:
            task_metadata: Dictionary with the task's dispatch_id and node id.
            job_id: Unique ID assigned to the job by the backend.

        Returns:
            True if cancel succeeded, False if failed.
        """

        if job_id is None:
            app_log.debug("cancel was called while the task was not yet run.")
            return True

        app_log.debug(f"Cancelling job ID {job_id}...")

        conn = await self._client_connect()
        qdel_cmd = f"qdel {job_id}"
        proc = await self._run_command(conn, qdel_cmd)

        is_canceled = proc.returncode == 0

        if not is_canceled:
            stderr_str = str(proc.stderr).strip()
            app_log.debug(
                f"Failed to cancel the job with task_metadata: {task_metadata}, job_handle: {job_id}. error: {stderr_str}"
            )

        if conn is not None:
            app_log.debug("Closing SSH connection...")
            conn.close()
            await conn.wait_closed()

            app_log.debug("SSH connection closed, cancel complete")

        return is_canceled

    async def teardown(self, task_metadata: dict) -> None:
        """Perform cleanup on remote machine and Covalent server.
        If self.cleanup is False, teardown do nothing.

        Args:
            task_metadata: Dictionary of metadata associated with the task. This variable is always passed by Covalent, but is never used.

        Returns:
            None
        """
        if self.cleanup:
            app_log.debug("Performing cleanup on remote...")

            try:
                conn = await self._client_connect()
                await self._perform_cleanup(conn=conn)

                if conn is not None:
                    app_log.debug("Closing SSH connection...")
                    conn.close()
                    await conn.wait_closed()
                    app_log.debug("SSH connection closed, teardown complete")
            except Exception as err:
                app_log.warning(
                    "Cleanup on the remote machine could not successfully complete. Nonfatal error."
                )
                app_log.warning(err)

    async def _perform_cleanup(self, conn: asyncssh.SSHClientConnection | None) -> None:
        """Function to perform cleanup on remote machine and Covalent server.

        Args:
            conn: SSH connection object

        Returns:
            None
        """

        if self._cleanup_files is None:
            app_log.debug("Skipping _perform_cleanup as _cleanup_files is not set.")
            return

        local_files_to_remove = [
            self._cleanup_files.local_func_filename,
            self._cleanup_files.local_job_script_filename,
            self._cleanup_files.local_py_script_filename,
        ]

        remote_files_to_remove = [
            self._cleanup_files.remote_func_filename,
            self._cleanup_files.remote_job_script_filename,
            self._cleanup_files.remote_py_script_filename,
            self._cleanup_files.remote_result_filename,
            self._cleanup_files.remote_stdout_filename,
            self._cleanup_files.remote_stderr_filename,
        ]

        for local_file in local_files_to_remove:
            await async_os.remove(local_file)

        for remote_file in remote_files_to_remove:
            await self._run_command(conn, f"rm {remote_file}")
