"""
Shell 解析器扩展：解析 spark-submit 命令
"""

import dataclasses
from typing import List

__all__ = [
    "Command"
]


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class Command:
    """Shell 命令的基类"""

    command_name: str = dataclasses.field(kw_only=True)  # 命令名称
    tokens: List[str] = dataclasses.field(kw_only=True)  # 原始 token 的列表
