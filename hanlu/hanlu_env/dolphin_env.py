"""
海豚配置环境
"""

import abc
from typing import Optional

from hanlu.data_node import DInstance

__all__ = [
    "DolphinEnv"
]


class DolphinEnv(abc.ABC):
    """海豚环境类"""

    @abc.abstractmethod
    def get_data_instance(self, data_source_id: int) -> Optional[DInstance]:
        """根据数据源类型和数据源实例获取实例信息

        可以在海豚调度元数据 t_ds_datasource 表中根据 id 查询

        Parameters
        ----------
        data_source_id : int
            数据源 ID

        Returns
        -------
        DInstance
            数据源实例对象
        """
