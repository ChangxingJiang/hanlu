"""
Shell 解析器扩展：解析 spark-submit 命令
"""

import dataclasses
from typing import List, Optional

from hanlu.special_command.base import Command

__all__ = [
    "parse_beeline",
    "CommandBeeline",
]


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class CommandBeeline(Command):
    """Beeline 命令"""

    arg_url: str = dataclasses.field(kw_only=True, default=None)  # 集群信息（-u）
    arg_username: str = dataclasses.field(kw_only=True, default=None)  # 用户名（-n）
    arg_password: str = dataclasses.field(kw_only=True, default=None)  # 密码（-p）
    arg_database: str = dataclasses.field(kw_only=True, default=None)  # 数据库名称（-d）
    arg_filename: str = dataclasses.field(kw_only=True, default=None)  # 包含 HQL 语句的文件（-f）
    arg_execute: str = dataclasses.field(kw_only=True, default=None)  # 执行包含单个 HQL 语句（-e）
    arg_incremental: str = dataclasses.field(kw_only=True, default=None)  # 增量输出模式（-i）
    arg_silent: str = dataclasses.field(kw_only=True, default=None)  # 禁止输出日志信息（-s）
    arg_verbose: str = dataclasses.field(kw_only=True, default=None)  # 详细输出日志信息（-e）
    arg_query: str = dataclasses.field(kw_only=True, default=None)  # 执行一个包含多个 HQL 的语句（-q）
    arg_color: str = dataclasses.field(kw_only=True, default=None)  # 启用或禁用彩色输出


# 参数映射关系
ARG_NAME_HASH = {
    "-u": "arg_url",
    "--url": "arg_url",
    "-n": "arg_username",
    "--username": "arg_username",
    "-p": "arg_password",
    "--password": "arg_password",
    "-d": "arg_database",
    "--database": "arg_database",
    "-f": "arg_filename",
    "--filename": "arg_filename",
    "-e": "arg_execute",
    "--execute": "arg_execute",
    "-i": "arg_incremental",
    "--incremental": "arg_incremental",
    "-s": "arg_silent",
    "--silent": "arg_silent",
    "-v": "arg_verbose",
    "--verbose": "arg_verbose",
    "-q": "arg_query",
    "--query": "arg_query",
    "-c": "arg_color",
    "--color": "arg_color",
}


def parse_beeline(tokens: List[str]) -> Optional[CommandBeeline]:
    """解析 beeline 命令并返回 CommandBeeline 对象，如果不是标准的 beeline 命令则返回 None"""
    # 匹配 Spark 参数
    params = {}
    i = 0
    while i + 1 < len(tokens) and tokens[i] in ARG_NAME_HASH:
        config_name = ARG_NAME_HASH[tokens[i]]
        config_value = tokens[i + 1]
        params[config_name] = config_value
        i += 2

    return CommandBeeline(
        command_name="beeline",
        tokens=tokens,
        **params
    )
