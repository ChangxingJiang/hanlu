"""
MYSQL 类型节点
"""

import dataclasses
from typing import Optional

from hanlu.data_node import DInstance
from hanlu.data_node import DType

__all__ = [
    "DMySQLInstance",
]


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class DMySQLInstance(DInstance):
    """MySQL 实例"""

    host: str = dataclasses.field(kw_only=True)  # 主机地址
    port: int = dataclasses.field(kw_only=True)  # 端口号
    username: Optional[str] = dataclasses.field(kw_only=True, default=None)  # 用户名
    password: Optional[str] = dataclasses.field(kw_only=True, default=None)  # 密码

    schema_name: Optional[str] = dataclasses.field(kw_only=True, default=None)  # 数据源库名

    @staticmethod
    def create(host: str,
               port: int,
               name: str,
               username: Optional[str] = None,
               password: Optional[str] = None,
               schema_name: Optional[str] = None) -> "DMySQLInstance":
        return DMySQLInstance(
            data_type=DType.MYSQL,
            name=name,
            host=host,
            port=port,
            username=username,
            password=password,
            schema_name=schema_name,
        )
