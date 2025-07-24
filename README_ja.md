# README

## Covalent PBS Professional Plugin

Covalent は Python を用いたワークフローツールであり、高度なコンピューティングハードウェア上でタスクを実行するために使用されています。Covalent PBS Professional Plugin は、 Covalent を [PBS Professional](https://www.altairjp.co.jp/pbs-professional/) で管理されているHPCシステムと連携させます。ワークフローをデプロイするには、ユーザーは PBS Professional ノードへのSSHアクセス、リモートファイルシステム上で書き込み可能なストレージスペース、および PBS Professional へのジョブの送信権限を保持している必要があります。

## インストール方法

### ソースコードからのインストール方法

次のステップで Covalent PBS Professional Plugin をソースコードから直接インストールすることができます。

1. 本リポジトリをローカルマシンにクローン
2. ディレクトリに移動

```shell
cd this-repository
```

3. `pip`を使ってインストール

```shell
pip install .
```

### リモートマシンの環境について

リモートシステムでは、Python のメジャーバージョンとマイナーバージョンがローカルマシンのものと一致する必要があります。これは、さまざまなオブジェクトを pickle 化(および unpickle ) する際の信頼性を保証するためです。加えて、リモートシステムの Python 環境では、ローカルマシン上と同じバージョンの [covalent](https://github.com/AgnostiqHQ/covalent) をインストールする必要があります（例: `pip install covalent==<specific version>`）。

## 使用方法

### ワークフローでプラグインを使用する方法 1

[Covalentの設定ファイル](https://docs.covalent.xyz/docs/user-documentation/how-to/customization/)(デフォルトは`~/.config/covalent/covalent.conf`)を適切に設定することで、次のようにしてHPCマシン上でワークフローを実行できます。

```python
import covalent as ct

@ct.electron(executor="pbspro")
def add(a, b):
    return a + b

@ct.lattice
def workflow(a, b):
    return add(a, b)


dispatch_id = ct.dispatch(workflow)(1, 2)
result = ct.get_result(dispatch_id)

```

### ワークフローでプラグインを使用する方法 2

Covalent の設定ファイルの設定値を使用するだけでなく、Python スクリプト内で様々なパラメータを変更したい場合は、`PBSProExecutor` クラスのカスタムインスタンスを生成することで実現可能です。

以下はいくつかの一般的に使用されるパラメータの設定例です。デフォルトでは、PBSProExecutor内で指定されていないパラメータは、設定ファイルから継承されます。

```python
import covalent as ct

executor = ct.executor.PBSProExecutor(
    username="UserName",
    address="remotemachine.address",
    port=22,
    ssh_key_file="~/.ssh/id_rsa",
    remote_workdir="$HOME/remote_workdir",
    poll_freq=30,
    cleanup=True,
    embedded_qsub_args={
        "l": ["select=mem=400mb", "walltime=1:00:00"],
    },
    qsub_args={
        "N": "job_name",
    },
    prerun_commands=[
        "source /etc/profile.d/modules.sh",
        "module load python/3.9/3.9.16",
        "module load cuda/11.8/11.8.0",
        "module load cudnn/8.8/8.8.1",
        "source ~/remote_workdir/.venv/bin/activate",
    ],
    bashrc_path="~/.bashrc",
)

@ct.electron(executor=executor)
def add(a, b):
    return a + b

@ct.lattice
def workflow(a, b):
    return add(a, b)


dispatch_id = ct.dispatch(workflow)(1, 2)
result = ct.get_result(dispatch_id)
```

### 設定値

`ct.executor.PBSProExecutor`に渡すことができる、または Covalent の設定ファイルの `[executors.pbspro]` セクションを変更することで指定できる設定オプションは多数あります。

以下は Covalent の設定ファイルこのプラグインの設定値を記述する方法の例です。

```console
[executors.pbspro]
username = "UserName"
address = "remote_machine.example.address"
port = 22
ssh_key_file = "~/.ssh/id_rsa"
bashrc_path = "$HOME/.bashrc"
prerun_commands = ["module load ABC", "source ~/remote-workdir/.venv/bin/activate"]
cleanup = true
remote_workdir = "covalent-workdir"
create_unique_workdir = false
cache_dir = "~/.cache/covalent"
poll_freq = 30
remote_cache = ".cache/covalent"
log_stdout = "stdout.log"
log_stderr = "stderr.log"

[executors.pbspro.qsub_args]
P = "project_name"
N = "job_name"

[executors.pbspro.embedded_qsub_args]
l = ["walltime=1:00:00", "select=mem=400mb"]
V = ""

```
#### `qsub` コマンドのオプションの指定方法

Covalent PBS Professional Plugin は、`qsub`コマンドを使用してジョブを PBS Professional に送信します。`qsub`のオプションの指定は、`qsub_args`または`embedded_qsub_args`というパラメータで指定することで行うことができます。これらのパラメータはそれぞれ、`qsub`コマンドに直接渡されるパラメータと、ジョブスクリプトに`#PBS`ディレクティブで記述されるパラメータを指定します。

たとえば、設定ファイルに次のように記述されている場合、

```console
[executors.pbspro.qsub_args]
P = "project_name"
N = "job_name"

[executors.pbspro.embedded_qsub_args]
l = ["walltime=1:00:00", "select=mem=400mb"]
V = ""

```

PBSProExecutorで実行されるタスクは、以下の`qsub`コマンドによってPBS Professional に送信されます

```shell
qsub -P project_name -N job_name {script_filename}
```

また、PBS Professional に送信されたスクリプトは以下のディレクティブを含みます。


```shell
#!/bin/bash

#PBS -l walltime=1:00:00
#PBS -l select=mem=400mb
#PBS -V
```

##### サポートしていない`qsub`コマンドのオプション

以下の`qsub`コマンドのオプションは Covalent PBS Professional Plugin ではサポートしていません。`qsub_args`または`embedded_qsub_args`で一つでも設定されていた場合、PBSProExecutorはエラーを送出します。

  * `-C`
  * `-G`
  * `-h`
  * `-I`
  * `-J`
  * `-k`
  * `-X`
  * `-z`

#### その他のパラメータ

使用者のニーズに応じて、Covalent の設定ファイル内で、リモートマシンのアドレスやログイン時に使用するユーザーネーム、認証で使用するssh_key_file等の様々なパラメータを指定・変更することができます。

PBSProExecutor クラスの docstring に記載されている入力パラメータの説明は以下の通りです。

```python
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
                If skip_ssh=False, the directory will be created under the user home directory on the machine where the computation is executed.
        create_unique_workdir: Whether to create a unique working (sub)directory for each task.
        cache_dir: Local cache directory used by this executor for temporary files.
        poll_freq: Frequency with which to poll a submitted job. Always is >= 30.
        remote_cache: Remote server cache directory used for temporary files.
        log_stdout: The path to the file to be used for redirecting stdout.
        log_stderr: The path to the file to be used for redirecting stderr.
        time_limit: time limit for the task.
        retries: Number of times to retry execution upon failure.
    """
```

## リリースノート

リリースノートは[Changelog](/CHANGELOG.md) に記載しています。

## 引用

出版物では次の引用を使用してください。

> W. J. Cunningham, S. K. Radha, F. Hasan, J. Kanem, S. W. Neagle, and S. Sanand.
> _Covalent._ Zenodo, 2022. https://doi.org/10.5281/zenodo.5903364

## ライセンス
Covalent は  Apache License 2.0 によってライセンスされています。ライセンスの詳細については、[LICENSE](/LICENSE) を参照するかサポートチームに連絡してください。
