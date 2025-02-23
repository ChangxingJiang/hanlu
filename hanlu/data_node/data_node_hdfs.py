"""
HDFS 节点类型
"""

import dataclasses
from typing import Optional

from hanlu.data_node import DInstance
from hanlu.data_node import DType

__all__ = [
    "DHdfsInstance",
]


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class DHdfsInstance(DInstance):
    """HDFS 实例"""

    default_fs: str = dataclasses.field(kw_only=True, hash=True, compare=True)

    # OBS 服务终端对象
    fs_obs_end_point: Optional[str] = dataclasses.field(kw_only=True, default=None, hash=True, compare=True)
    # OBS 访问密钥
    fs_obs_access_key: Optional[str] = dataclasses.field(kw_only=True, default=None, hash=True, compare=True)
    # OBS 安全密钥
    fs_obs_secret_key: Optional[str] = dataclasses.field(kw_only=True, default=None, hash=True, compare=True)
    # OBS 桶名
    fs_obs_bucket: Optional[str] = dataclasses.field(kw_only=True, default=None, hash=True, compare=True)

    @staticmethod
    def create_obs_instance(name: Optional[str],
                            fs_obs_end_point: str,
                            fs_obs_bucket: str,
                            fs_obs_access_key: Optional[str] = None,
                            fs_obs_secret_key: Optional[str] = None
                            ) -> "DHdfsInstance":
        return DHdfsInstance(
            data_type=DType.HDFS,
            name=name,
            default_fs=f"obs://{fs_obs_bucket}",
            fs_obs_end_point=fs_obs_end_point,
            fs_obs_bucket=fs_obs_bucket,
            fs_obs_access_key=fs_obs_access_key,
            fs_obs_secret_key=fs_obs_secret_key
        )
