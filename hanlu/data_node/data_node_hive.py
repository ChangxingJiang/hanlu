"""
Hive 类型节点
"""

import dataclasses
from typing import List, Optional, Tuple

from hanlu.data_node.data_node_base import DInstance
from hanlu.data_node.data_node_base import DType

__all__ = [
    "DHiveInstance",
]


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class DHiveInstance(DInstance):
    """Hive 类型的数据源实例"""

    hosts: Tuple[str] = dataclasses.field(kw_only=True)
    username: Optional[str] = dataclasses.field(kw_only=True, default=None)
    password: Optional[str] = dataclasses.field(kw_only=True, default=None)

    schema_name: Optional[str] = dataclasses.field(kw_only=True, default=None)  # 数据源库名

    @staticmethod
    def create(hosts: List[str],
               name: str,
               username: Optional[str] = None,
               password: Optional[str] = None,
               schema_name: Optional[str] = None) -> "DHiveInstance":
        return DHiveInstance(
            data_type=DType.HIVE,
            name=name,
            hosts=tuple(hosts),
            username=username,
            password=password,
            schema_name=schema_name,
        )
