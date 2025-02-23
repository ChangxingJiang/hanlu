"""
寒露环境类
"""
import collections
from typing import Dict, List, Optional

from hanlu.data_node import DHdfsInstance
from hanlu.data_node import DHiveInstance
from hanlu.data_node import DInstance
from hanlu.data_node import DMySQLInstance

__all__ = [
    "HanLuEnv"
]


class HanLuEnv:
    """寒露环境类"""

    def __init__(self):
        # Hive 主机到 Hive 名称的映射
        self._hive_host_to_name_hash: Dict[str, str] = {}

        # Hive 名称到主机列表的映射
        self._hive_name_to_hosts_hash: Dict[str, List[str]] = {}

        # MySQL 主机到 MySQL 名称的映射
        self._mysql_host_to_name_hash: Dict[str, str] = {}

        # HDFS 信息到 Hive 名称的映射
        self._hdfs_info_to_hive_name_hash: Dict[DHdfsInstance, Dict[str, str]] = collections.defaultdict(dict)

    def regist_hive_cluster(self, hosts: List[str], name: str,
                            hdfs_instance: Optional[DHdfsInstance] = None,
                            hdfs_root_path: Optional[str] = None) -> None:
        """注册 Hive 集群

        Parameters
        ----------
        hosts : List[str]
            Hive 主机列表
        name : str
            集群名称
        hdfs_instance : Optional[DHdfsInstance], default = None
            HDFS 实例对象
        hdfs_root_path : Optional[str], default = None
            HDFS 根路径
        """
        for host in hosts:
            self._hive_host_to_name_hash[host] = name
        self._hive_name_to_hosts_hash[name] = hosts

        if hdfs_instance is not None and hdfs_root_path is not None:
            self._hdfs_info_to_hive_name_hash[hdfs_instance][hdfs_root_path] = name

    def regist_mysql_server(self, host: str, port: int, name: str) -> None:
        """注册 MySQL 服务器

        Parameters
        ----------
        host : str
            MySQL 主机
        port : int
            端口号
        name : str
            服务器名称
        """
        self._mysql_host_to_name_hash[f"{host}:{port}"] = name

    def get_instance_by_jdbc_url(self,
                                 jdbc_url: str,
                                 user_name: Optional[str] = None,
                                 password: Optional[str] = None) -> DInstance:
        """根据 JDBC URL 获取实例对象

        Parameters
        ----------
        jdbc_url : str
            JDBC URL
        user_name : Optional[str], default = None
            用户名
        password : Optional[str], default = None
            密码

        Returns
        -------
        DInstance
            实例对象
        """
        if not jdbc_url.startswith("jdbc:"):
            return DInstance.unknown()

        jdbc_url = jdbc_url[5:]

        # Hive 集群：样例 jdbc:hive2://xxx.xxx.xxx.xxx:2181,xxx.xxx.xxx.xxx:2181,xxx.xxx.xxx.xxx:2181/test;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;
        if jdbc_url.startswith("hive2://"):
            hive_info = jdbc_url[8:].split(";")[0]
            if "/" in hive_info:
                hosts_str, schema_name = hive_info.split("/")
            else:
                hosts_str, schema_name = hive_info, None

            hosts = hosts_str.split(",")
            return DHiveInstance.create(
                hosts=hosts,
                name=self._hive_host_to_name_hash.get(hosts[0]),
                username=user_name,
                password=password,
                schema_name=schema_name,
            )

        if jdbc_url.startswith("mysql://"):
            mysql_info = jdbc_url[8:]
            if "/" in mysql_info:
                host_and_port, schema_name = mysql_info.split("/")
            else:
                host_and_port, schema_name = mysql_info, None
            host, port = host_and_port.split(":")
            return DMySQLInstance.create(
                host=host,
                port=int(port),
                name=self._mysql_host_to_name_hash.get(host_and_port),
                username=user_name,
                password=password,
                schema_name=schema_name
            )

    def get_hive_instance_by_hdfs_instance(self, hdfs_instance: DHdfsInstance, path: str) -> Optional[DHiveInstance]:
        """根据 HDFS 实例对象和路径，构造对应 Hive 的实例对象

        Parameters
        ----------
        hdfs_instance : DHdfsInstance
            HDFS 实例对象
        path : str
            HDFS 路径

        Returns
        -------
        DHdfsInstance
            Hive 实例对象
        """
        if hdfs_instance not in self._hdfs_info_to_hive_name_hash:
            return None
        for root_path, hive_name in self._hdfs_info_to_hive_name_hash[hdfs_instance].items():
            if path.startswith(root_path):
                path = path[len(root_path):]
                if path.startswith("/"):
                    path = path[1:]
                schema_name = path[:path.index("/")].replace(".db", "")
                return DHiveInstance.create(
                    hosts=self._hive_name_to_hosts_hash[hive_name],
                    name=hive_name,
                    schema_name=schema_name
                )
        return None
