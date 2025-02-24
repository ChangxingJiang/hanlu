"""
寒露分析器
"""

import abc
import json
from typing import Any, Dict, Optional

import metasequoia_sql as ms_sql
from hanlu import special_command
from hanlu.common import dolphin_utils
from hanlu.data_node import DHdfsInstance
from hanlu.data_node import DInstance
from hanlu.data_node import DNode
from hanlu.data_task import DTask
from hanlu.hanlu_env import DolphinEnv
from hanlu.hanlu_env import HanLuEnv
from metasequoia_data_linage.table_level.analysis import all_use_table
from metasequoia_shell.init_simu_system import init_simu_system
from metasequoia_shell.lexical import LexicalFSMShell
from metasequoia_shell.parser import parse
from metasequoia_shell.simu_env import SimuCommandInput
from metasequoia_shell.simu_env import SimuProcess


class LackEnvError(Exception):
    """缺失环境信息错误"""


class HanLuAnalyzer(abc.ABC):
    """寒露分析器"""

    def __init__(self, hanlu_env: Optional[HanLuEnv] = None, dolphin_env: Optional[DolphinEnv] = None):
        self.hanlu_env = hanlu_env
        self.dolphin_env = dolphin_env

    # ------------------------------ 分析海豚调度任务的血缘关系 ------------------------------
    # analyze_dolphin_task：海豚调度任务血缘关系分析方法的入口，包含内置的处理逻辑；如果需要调整内置处理逻辑，则重写此方法
    # analyze_other_dolphin_task：内置处理逻辑无法分析该任务时的补充逻辑；如果需要补充处理逻辑，则重写此方法

    def analyze_dolphin_task(self, record: Dict[str, Any]) -> DTask:
        """海豚调度任务血缘关系分析方法的入口

        Parameters
        ----------
        record : Dict[str, Any]
            海豚元数据 task_definition 的表中记录
        """
        # DEPENDENT、CONDITIONS、DATA_QUALITY 类型任务节点中包含上下游关系
        if record["task_type"] in {"DEPENDENT", "CONDITIONS", "DATA_QUALITY"}:
            return DTask.empty()

        if record["task_type"] == "SQL":
            if self.dolphin_env is None:
                raise LackEnvError("need dolphin_env")
            task_params = json.loads(record["task_params"])
            data_instance = self.dolphin_env.get_data_instance(task_params["datasource"])
            return self.analyze_sql(data_instance, dolphin_utils.run_all_inner_function(task_params["sql"]))

        if record["task_type"] in {"SPARK", "SHELL"}:
            task_params = json.loads(record["task_params"])
            return self.analyze_shell_script(dolphin_utils.run_all_inner_function(task_params["rawScript"]))

        return self.analyze_other_dolphin_task(record)

    @abc.abstractmethod
    def analyze_other_dolphin_task(self, record: Dict[str, Any]) -> DTask:
        """内置处理逻辑无法分析该任务时的补充逻辑

        Parameters
        ----------
        record : Dict[str, Any]
            海豚元数据 task_definition 的表中记录
        """

    # ------------------------------ 分析 Shell 命令的血缘关系 ------------------------------

    def analyze_shell_script(self, script: str) -> DTask:
        """分析 Shell 脚本"""
        data_task = DTask.empty()
        simu_system = init_simu_system(
            configuration=self.hanlu_env.shell_parser_configuration,
            hook_func=self.analyze_shell_command_hook,
            hook_args={"data_task": data_task}
        )
        parse(LexicalFSMShell(script)).execute(simu_system.create_process())
        return data_task

    def analyze_shell_command_hook(self,
                                   simu_process: SimuProcess,
                                   command_input: SimuCommandInput,
                                   data_task: Optional[DTask]) -> None:
        """分析 Shell 命令的回调方法"""
        res_data_task = self.analyze_shell_command(simu_process, command_input)
        data_task += res_data_task

    def analyze_shell_command(self,
                              simu_process: SimuProcess,
                              command_input: SimuCommandInput) -> DTask:
        """分析 Shell 命令"""
        # 跳过不存在有效概念的空命令
        if self.hanlu_env.is_shell_ignore_command(command_input.command_name):
            return DTask.empty()
        if command_input.command_name == "beeline":
            return self.analyze_beeline_command(simu_process, command_input)
        if command_input.command_name == "spark-submit":
            spark_submit_command = special_command.parse_spark_submit(command_input.command_params)
            return self.analyze_spark_submit_command(simu_process, spark_submit_command)

        if command_input.command_name == "/data/datax/bin/datax.py":
            file_name = command_input.command_params[0]
            file_content = simu_process.read_file(file_name)
            if file_content is None:
                print(f"【失败】DataX 配置文件构造失败: {file_content}")
                return DTask.unknown()
            return self.analyze_datax_config(file_content)

        print(f"分析命令: {command_input.command_name} {command_input.command_params}")
        return self.analyze_other_shell_command(simu_process, command_input)

    @abc.abstractmethod
    def analyze_spark_submit_command(self,
                                     simu_process: SimuProcess,
                                     command: special_command.CommandSparkSubmit) -> DTask:
        """分析 spark-submit 命令"""

    @abc.abstractmethod
    def analyze_other_shell_command(self,
                                    simu_process: SimuProcess,
                                    command_input: SimuCommandInput) -> DTask:
        """分析其他 Shell 命令"""

    # ------------------------------ 分析 SQL 的血缘关系 ------------------------------

    def analyze_sql(self, data_instance: DInstance, sql: str) -> DTask:
        """分析 SQL 语句

        Parameters
        ----------
        data_instance : DInstance
            SQL 运行的数据实例
        sql : str
            执行的 SQL 语句
        """
        try:
            data_task = DTask.empty()
            for statement in ms_sql.SQLParser.parse_statements(sql, sql_type=ms_sql.SQLType.HIVE):
                if isinstance(statement, ms_sql.node.ASTAlterTableStatement):
                    data_task.add_generate_node(DNode(
                        instance=data_instance,
                        schema_name=statement.table_name.schema_name,
                        table_name=statement.table_name.table_name
                    ))
                elif isinstance(statement, ms_sql.node.ASTInsertSelectStatement):
                    for dependent_table in all_use_table(statement):
                        data_task.add_dependent_node(DNode(
                            instance=data_instance,
                            schema_name=dependent_table.schema_name,
                            table_name=dependent_table.table_name
                        ))
                    data_task.add_generate_node(DNode(
                        instance=data_instance,
                        schema_name=statement.table_name.schema_name,
                        table_name=statement.table_name.table_name
                    ))
                elif isinstance(statement, ms_sql.node.ASTSelectStatement):
                    continue  # SELECT 语句不影响血缘关系
                elif isinstance(statement, ms_sql.node.ASTSetStatement):
                    continue  # SET 语句不影响血缘关系
                elif isinstance(statement, ms_sql.node.ASTAnalyzeTableStatement):
                    continue  # ANALYZE 语句不影响血缘关系
                elif isinstance(statement, ms_sql.node.ASTTruncateTable):
                    data_task.add_generate_node(DNode(
                        instance=data_instance,
                        schema_name=statement.table_name.schema_name,
                        table_name=statement.table_name.table_name
                    ))
                else:
                    return self.analyze_other_sql(data_instance, sql)
            return data_task
        except Exception as e:
            print(f"SQL 解析失败: {sql}")
            return self.analyze_other_sql(data_instance, sql)

    @abc.abstractmethod
    def analyze_other_sql(self, data_instance: DInstance, sql: str) -> DTask:
        """分析 SQL 语句

        Parameters
        ----------
        data_instance : DInstance
            SQL 运行的数据实例
        sql : str
            执行的 SQL 语句
        """

    # ------------------------------ 分析 beeline 命令的血缘关系 ------------------------------
    def analyze_beeline_command(self,
                                simu_process: SimuProcess,
                                command_input: SimuCommandInput) -> DTask:
        """分析 beeline 命令"""
        idx = 0
        jdbc_url: Optional[str] = None
        user_name: Optional[str] = None
        password: Optional[str] = None
        sql: Optional[str] = None
        params = command_input.command_params
        while idx < len(params):
            if params[idx] in {"-u", "--url"}:
                jdbc_url = params[idx + 1]
                idx += 2
            elif params[idx] in {"-n", "--user"}:
                user_name = params[idx + 1]
                idx += 2
            elif params[idx] in {"-p", "--password"}:
                password = params[idx + 1]
                idx += 2
            elif params[idx] in {"-e", "--execute"}:
                sql = params[idx + 1]
                idx += 2
            else:
                print(f"未知 beeline 命令: {command_input}")
                return DTask.unknown()

        if sql is None:
            print(f"没有找到 SQL 语句: {command_input}")
            return DTask.unknown()

        data_instance = self.hanlu_env.get_instance_by_jdbc_url(jdbc_url, user_name=user_name, password=password)

        return self.analyze_sql(data_instance, sql)

    def analyze_datax_config(self, config_content: str) -> DTask:
        """根据 DataX 的配置文件，分析数据流向"""
        data_task = DTask.empty()
        datax_config = json.loads(config_content.replace("\t", "\\t"))
        for content in datax_config["job"]["content"]:
            reader_name = content["reader"]["name"]
            if reader_name == "mysqlreader":
                parameter = content["reader"]["parameter"]
                username = parameter.get("username")
                password = parameter.get("password")
                connection = parameter["connection"][0]
                table_name = connection["table"][0]
                jdbc_url = connection["jdbcUrl"][0]
                data_instance = self.hanlu_env.get_instance_by_jdbc_url(jdbc_url, user_name=username, password=password)
                data_task.add_dependent_node(DNode(
                    instance=data_instance,
                    table_name=table_name
                ))
            else:
                print("【失败】暂不支持的 DataX Reader 类型: ", reader_name)
                return DTask.unknown()

            writer_name = content["writer"]["name"]
            if writer_name == "hdfswriter":
                parameter = content["writer"]["parameter"]
                if "defaultFS" not in parameter or "path" not in parameter:
                    print("【失败】DataX 格式错误 ", writer_name)
                    return DTask.unknown()

                default_fs = parameter["defaultFS"]
                path = parameter["path"]
                if default_fs.startswith("obs://"):
                    hdfs_instance = DHdfsInstance.create_obs_instance(
                        name=None,
                        fs_obs_end_point=parameter.get("hadoopConfig", {}).get("fs.obs.endpoint"),
                        fs_obs_bucket=default_fs.replace("obs://", "").strip("/")
                    )
                    self.hanlu_env.get_hive_instance_by_hdfs_instance(hdfs_instance, path)
            else:
                print("【失败】暂不支持的 DataX Writer 类型: ", reader_name)
                return DTask.unknown()

        return data_task


class HanLuDefaultAnalyzer(HanLuAnalyzer):
    """寒露默认分析器"""

    def analyze_spark_submit_command(self,
                                     simu_process: SimuProcess,
                                     command_input: SimuCommandInput) -> DTask:
        print("【失败】需重写 analyze_spark_submit_command 方法")
        return DTask.unknown()

    def analyze_other_dolphin_task(self, record: Dict[str, Any]) -> DTask:
        print("【失败】未知 Dolphin 任务")
        return DTask.unknown()

    def analyze_other_shell_command(self,
                                    simu_process: SimuProcess,
                                    command_input: SimuCommandInput) -> DTask:
        print("【失败】未知 Shell 命令")
        return DTask.unknown()

    def analyze_other_sql(self, data_instance: DInstance, sql: str) -> DTask:
        print(f"【失败】未知 sql 语句: {sql}")
        return DTask.unknown()
