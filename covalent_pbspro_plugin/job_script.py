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

"""Tools for formatting the PBS Professional job submission script."""

from __future__ import annotations

import importlib.metadata

JOB_SCRIPT_TEMPLATE = """\
#!/bin/bash -e

{embedded_qsub_args}

{source_command}

{cd_workdir_command}

{prerun_commands}

remote_py_version=$(python3 -c "print('.'.join(map(str, __import__('sys').version_info[:2])))")
if [[ $remote_py_version != "{python_version}" ]] ; then
  >&2 echo "Python version mismatch. Please install Python {python_version} in the compute environment."
  exit 3
fi

covalent_version=$(python -c "import covalent; print(covalent.__version__)")
if [ $? -ne 0 ] ; then
  >&2 echo "Covalent may not be installed in the compute environment."
  exit 4
elif [[ $covalent_version != "{covalent_version}" ]] ; then
  >&2 echo "Covalent version mismatch."
  >&2 echo "Environment (covalent==$covalent_version) does not match task (covalent=={covalent_version})."
  >&2 echo "The task might still be runnable but if failed the error might not be as informative."
  exit 5
fi

cloudpickle_version=$(python -c "import cloudpickle; print(cloudpickle.__version__)")
if [ $? -ne 0 ] ; then
  >&2 echo "Cloudpickle may not be installed in the compute environment."
  >&2 echo "Please install cloudpickle=={cloudpickle_version} in the compute environment."
  exit 6
elif [[ $cloudpickle_version != "{cloudpickle_version}" ]] ; then
  >&2 echo "Cloudpickle version mismatch."
  >&2 echo "Environment (cloudpickle==$cloudpickle_version) does not match task (cloudpickle=={cloudpickle_version})."
  >&2 echo "The task might still be runnable but if failed the error might not be as informative."
  exit 7
fi

{python_command}

{postrun_commands}

wait
"""


class JobScript:

    def __init__(
        self,
        embedded_qsub_args: dict[str, str | list[str] | tuple[str, ...]],
        bashrc_path: str | None,
        prerun_commands: list[str],
        postrun_commands: list[str],
    ) -> None:
        self.embedded_qsub_args = embedded_qsub_args
        self.bashrc_path = bashrc_path
        self.prerun_commands = prerun_commands
        self.postrun_commands = postrun_commands

    @property
    def qsub_directives(self) -> str:
        directives = []
        directive_prefix = "#PBS"
        for key, value in self.embedded_qsub_args.items():
            if isinstance(value, (list, tuple)):
                for arg_value in value:
                    embedded_qsub_arg_str = f"{directive_prefix} -{key}" + (
                        f" {arg_value}" if arg_value else ""
                    )
                    directives.append(embedded_qsub_arg_str)
            else:
                embedded_qsub_arg_str = f"{directive_prefix} -{key}" + (
                    f" {value}" if value else ""
                )
                directives.append(embedded_qsub_arg_str)

        return "\n".join(directives)

    @property
    def prerun_commands_lines(self) -> str:
        return "\n".join(self.prerun_commands)

    @property
    def source_command(self) -> str:
        return f"source {self.bashrc_path}" if self.bashrc_path else ""

    @property
    def postrun_commands_lines(self) -> str:
        return "\n".join(self.postrun_commands)

    def format(
        self,
        python_version: str,
        covalent_version: str,
        py_script_filename: str,
        func_filename: str,
        result_filename: str,
        current_remote_workdir: str,
    ) -> str:

        cd_workdir_command = f"cd {current_remote_workdir}"

        python_command = f"python3 {py_script_filename} {func_filename} {result_filename}"

        return JOB_SCRIPT_TEMPLATE.format(
            embedded_qsub_args=self.qsub_directives,
            source_command=self.source_command,
            cd_workdir_command=cd_workdir_command,
            prerun_commands=self.prerun_commands_lines,
            python_version=python_version,
            covalent_version=covalent_version,
            python_command=python_command,
            cloudpickle_version=importlib.metadata.version("cloudpickle"),
            postrun_commands=self.postrun_commands_lines,
        )
