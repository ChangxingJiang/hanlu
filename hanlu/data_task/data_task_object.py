"""
数据任务节点类
"""

import abc
import dataclasses
from typing import List

from hanlu.data_node import DNode

__all__ = [
    "DTask",
]


@dataclasses.dataclass(slots=True)
class DTask(abc.ABC):
    """数据任务对象"""

    is_unknown: bool = dataclasses.field(kw_only=True, default=False)  # 数据任务的相关数据节点推断是否成功
    dependent_node_list: List[DNode] = dataclasses.field(kw_only=True, default_factory=lambda: [])  # 数据任务依赖的数据节点列表（上游）
    generate_node_list: List[DNode] = dataclasses.field(kw_only=True, default_factory=lambda: [])  # 数据任务生成的数据节点列表（下游）

    @classmethod
    def unknown(cls) -> "DTask":
        """创建一个推断失败的数据任务对象"""
        return cls(is_unknown=True)

    @classmethod
    def empty(cls) -> "DTask":
        """创建没有依赖和生成数据节点的空数据任务对象"""
        return cls(is_unknown=False, dependent_node_list=[], generate_node_list=[])

    def add_dependent_node(self, data_node: DNode) -> None:
        self.dependent_node_list.append(data_node)

    def add_generate_node(self, data_node: DNode) -> None:
        self.generate_node_list.append(data_node)

    def __bool__(self) -> bool:
        """如果数据任务的相关数据节点推断是否成功则返回 True，否则返回 False"""
        return self.is_unknown is False

    def __add__(self, other: "DTask") -> "DTask":
        """实现 + 运算符"""
        if not isinstance(other, DTask):
            raise NotImplemented  # 如果 other 不是 DTask 类型则不允许进行计算
        if self.is_unknown or other.is_unknown:
            return DTask.unknown()  # 如果 self 和 other 中有任意一个推断失败，则求和后的任务也推断失败
        return DTask(
            dependent_node_list=self.dependent_node_list + other.dependent_node_list,
            generate_node_list=self.generate_node_list + other.generate_node_list
        )

    def __iadd__(self, other: "DTask") -> "DTask":
        """实现 += 运算符"""
        if not isinstance(other, DTask):
            raise NotImplemented  # 如果 other 不是 DTask 类型则不允许进行计算
        if self.is_unknown or other.is_unknown:
            self.is_unknown = True  # 如果 self 和 other 中有任意一个推断失败，则求和后的任务也推断失败
        self.dependent_node_list += other.dependent_node_list
        self.generate_node_list += other.generate_node_list
        return self
