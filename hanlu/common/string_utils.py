"""
通用字符串工具函数
"""

from typing import List

__all__ = [
    "jdbc_url_to_hive_hosts"
]


def jdbc_url_to_hive_hosts(jdbc_url: str) -> List[str]:
    """将 JDBC Url 字符串转化为 Host 的列表

    JDBC URL 字符串样例：jdbc:hive2://xxx.xxx.xxx.xxx:2181,xxx.xxx.xxx.xxx:2181/schema_name
    """
    jdbc_url = jdbc_url.replace("jdbc:hive2://", "")
    if "/" in jdbc_url:
        jdbc_url = jdbc_url[:jdbc_url.index("/")]
    return jdbc_url.split(",")
