"""
Shell 解析器扩展：解析 spark-submit 命令
"""

import dataclasses
from typing import List, Optional

from hanlu.special_command.base import Command

__all__ = [
    "parse_spark_submit",
    "CommandSparkSubmit",
]


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class CommandSparkSubmit(Command):
    """spark-submit 命令"""

    # Spark 参数
    arg_master: str = dataclasses.field(kw_only=True, default=None)  # 集群
    arg_deploy_mode: str = dataclasses.field(kw_only=True, default=None)  # 部署模式
    arg_class: str = dataclasses.field(kw_only=True, default=None)  # Java / Scala 任务的主类
    arg_name: str = dataclasses.field(kw_only=True, default=None)  # 作业名称
    arg_jars: str = dataclasses.field(kw_only=True, default=None)
    arg_packages: str = dataclasses.field(kw_only=True, default=None)
    arg_exclude_packages: str = dataclasses.field(kw_only=True, default=None)
    arg_repositories: str = dataclasses.field(kw_only=True, default=None)
    arg_py_files: str = dataclasses.field(kw_only=True, default=None)
    arg_files: str = dataclasses.field(kw_only=True, default=None)
    arg_conf: List[str] = dataclasses.field(kw_only=True, default=None)
    arg_properties_file: str = dataclasses.field(kw_only=True, default=None)
    arg_driver_memory: str = dataclasses.field(kw_only=True, default=None)
    arg_driver_java_options: str = dataclasses.field(kw_only=True, default=None)
    arg_driver_library_path: str = dataclasses.field(kw_only=True, default=None)
    arg_driver_class_path: str = dataclasses.field(kw_only=True, default=None)
    arg_executor_memory: str = dataclasses.field(kw_only=True, default=None)
    arg_proxy_user: str = dataclasses.field(kw_only=True, default=None)
    arg_driver_cores: str = dataclasses.field(kw_only=True, default=None)
    arg_total_executor_cores: str = dataclasses.field(kw_only=True, default=None)
    arg_executor_cores: str = dataclasses.field(kw_only=True, default=None)
    arg_queue: str = dataclasses.field(kw_only=True, default=None)
    arg_num_executors: str = dataclasses.field(kw_only=True, default=None)
    arg_archives: str = dataclasses.field(kw_only=True, default=None)
    arg_principal: str = dataclasses.field(kw_only=True, default=None)
    arg_keytab: str = dataclasses.field(kw_only=True, default=None)

    # Jar 包或 Python 脚本
    application: str = dataclasses.field(kw_only=True)

    # Application 的命令行参数
    application_arguments: List[str] = dataclasses.field(kw_only=True, default=None)


# Spark 的参数列表
SPARK_CONFIG_SET = {"--master", "--deploy-mode", "--class", "--name", "--jars", "--packages", "--exclude-packages",
                    "--repositories", "--py-files", "--files", "--conf", "--properties-file", "--driver-memory",
                    "--driver-java-options", "--driver-library-path", "--driver-class-path", "--executor-memory",
                    "--proxy-user", "--driver-cores", "--total-executor-cores", "--executor-cores",
                    "--queue", "--num-executors", "--archives", "--principal", "--keylab"}


def parse_spark_submit(tokens: List[str]) -> Optional[CommandSparkSubmit]:
    """解析 spark_submit 命令并返回 CommandSparkSubmit 对象，如果不是标准的 spark-submit 命令则返回 None"""
    # 匹配 Spark 参数
    params = {}
    i = 0
    while i + 1 < len(tokens) and tokens[i] in SPARK_CONFIG_SET:
        config_name = "arg_" + tokens[i].lstrip("-").replace("-", "_")
        config_value = tokens[i + 1]
        if config_name == "arg_conf":
            params.setdefault(config_name, [])
            params[config_name].append(config_value)
        else:
            params[config_name] = config_value
        i += 2

    if i == len(tokens):
        return None  # 没有 jar 包或 python 主程序

    params["application"] = tokens[i]
    params["application_arguments"] = tokens[i + 1:]

    return CommandSparkSubmit(
        command_name="spark-submit",
        tokens=tokens,
        **params
    )
