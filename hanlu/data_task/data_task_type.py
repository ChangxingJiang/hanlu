"""
数据任务类型
"""

import enum

__all__ = [
    "DTaskType",
]


class DTaskType(enum.Enum):
    """数据任务类型"""

    UNKNOWN = 0

    # 离线任务
    HIVE_ON_SPARK = 10  # Hive on Spark
    SPARK_SQL = 11  # Spark SQL

    # 实时任务
    FLINK_JAVA = 20  # Java Flink
    FLINK_SCALA = 21  # Scala Flink
    FLINK_PYTHON = 22  # Python Flink
    PYTHON = 23  # Python
    CANAL = 24
