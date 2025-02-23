"""
基础数据节点类
"""

import dataclasses
import enum
from typing import Optional

__all__ = [
    "DType",
    "DInstance",
    "DNode",
]


class DType(enum.IntEnum):
    """数据源对象类型的枚举类"""

    UNKNOWN = 0  # 未知数据类型
    HIVE = 1  # Hive
    MYSQL = 2  # MySQL
    KAFKA = 3  # Kafka
    HBASE = 4  # HBase
    OTS = 5  # OTS (tablestore)
    DORIS = 6  # Doris
    REDIS = 7  # Redis
    ES = 8  # ElasticSearch
    HDFS = 9  # HDFS


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class DInstance:
    """数据源实例"""

    data_type: DType = dataclasses.field(kw_only=True, hash=True, compare=True)  # 数据源类型
    name: Optional[str] = dataclasses.field(kw_only=True, default=None, hash=False, compare=False)  # 实例名称（不参与比较）

    @staticmethod
    def unknown() -> "DInstance":
        return DInstance(data_type=DType.UNKNOWN)


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class DNode:
    """数据源对象"""

    instance: Optional[DInstance] = dataclasses.field(kw_only=True, default=None)  # 数据源实例对象
    schema_name: Optional[str] = dataclasses.field(kw_only=True, default=None)  # 数据源库名
    table_name: Optional[str] = dataclasses.field(kw_only=True, default=None)  # 数据源表名
