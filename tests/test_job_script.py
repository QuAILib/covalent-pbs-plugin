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

import importlib.metadata
from pathlib import Path

import pytest

from covalent_pbspro_plugin.job_script import JobScript


def test_JobsScript_properties():
    """Test the properties of the JobScript class."""

    job_script = JobScript(
        embedded_qsub_args=dict(),
        bashrc_path=None,
        prerun_commands=[],
        postrun_commands=[],
    )

    assert job_script.qsub_directives == ""
    assert job_script.source_command == ""
    assert job_script.prerun_commands_lines == ""
    assert job_script.postrun_commands_lines == ""

    # with embedded_qsub_args, prerun_commands, postrun_commands
    job_script.embedded_qsub_args = {
        "l": ["select=ncpus=2:mem=4gb", "walltime=1:00:00"],
        "f": "",
        "N": "JobName",
    }
    job_script.prerun_commands = ["prerun1", "prerun2"]
    job_script.postrun_commands = ["postrun1", "postrun2", "postrun3"]

    assert (
        job_script.qsub_directives
        == "#PBS -l select=ncpus=2:mem=4gb\n#PBS -l walltime=1:00:00\n#PBS -f\n#PBS -N JobName"
    )
    assert job_script.prerun_commands_lines == "prerun1\nprerun2"
    assert job_script.postrun_commands_lines == "postrun1\npostrun2\npostrun3"

    # empty bashrc_path
    job_script.bashrc_path = ""
    assert job_script.source_command == ""

    # with bashrc_path
    job_script.bashrc_path = "/path/to/bashrc"
    assert job_script.source_command == "source /path/to/bashrc"


def test_job_script_format():
    """Test that the method `format` of JobScript returns correct string."""

    job_script = JobScript(
        embedded_qsub_args={
            "l": ["select=ncpus=2:mem=4gb", "walltime=1:00:00"],
            "f": "",
            "N": "JobName",
        },
        bashrc_path="/path/to/bashrc",
        prerun_commands=["prerun1", "prerun2"],
        postrun_commands=["postrun1", "postrun2", "postrun3"],
    )

    current_remote_workdir = "/home/test_user/workdir"
    python_version = ".".join(map(str, __import__("sys").version_info[:2]))
    covalent_version = importlib.metadata.version("covalent")
    cloudpickle_version = importlib.metadata.version("cloudpickle")

    dispatch_id = "378cdd10-8443-11ee-99e2-a35cb4664dfd"
    node_id = 1
    py_script_filename = f"script-{dispatch_id}-{node_id}.py"
    func_filename = f"func-{dispatch_id}-{node_id}.pkl"
    result_filename = f"result-{dispatch_id}-{node_id}.pkl"

    expected = f"""\
#!/bin/bash -e

#PBS -l select=ncpus=2:mem=4gb
#PBS -l walltime=1:00:00
#PBS -f
#PBS -N JobName

source /path/to/bashrc

cd /home/test_user/workdir

prerun1
prerun2

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

python3 {py_script_filename} {func_filename} {result_filename}

postrun1
postrun2
postrun3

wait
"""

    assert (
        job_script.format(
            python_version=python_version,
            covalent_version=covalent_version,
            py_script_filename=py_script_filename,
            func_filename=func_filename,
            result_filename=result_filename,
            current_remote_workdir=current_remote_workdir,
        )
        == expected
    )
