"""
海豚调度相关工具函数
"""

import calendar
import datetime
import re
from typing import Any, Optional

__all__ = [
    "run_inner_function",
    "run_all_inner_function",
]


def run_inner_function(text: str) -> Optional[str]:
    """运行海豚调度的内置函数

    Parameters
    ----------
    text : str
        内置函数文本

    Returns
    -------
    Optional[str]
        内置函数的返回值，如果无法运行则返回 None
    """

    # 兼容包含前缀和不包含前缀的情况
    if text.startswith("${"):  # 如果包含 "${" 前缀则剔除
        text = text[2:]
    if text.endswith("}"):  # 如果包含 "}" 后缀则剔除
        text = text[:-1]

    now: Any = None
    for sub_text in text.split("."):
        # 提取变量名、函数名和函数参数
        if "(" in sub_text and sub_text.endswith(")"):  # 当前元素是函数
            idx = sub_text.index("(")
            name = sub_text[:idx]
            params = sub_text[idx + 1:-1].split(",")
        else:  # 非函数的形式
            name = sub_text
            params = []

        if now is None:
            # 样例：start("yyyyMMdd",-1)
            if name == "start" and len(params) == 2:
                # 解析参数
                p1 = (params[0][1:-1]
                      .replace("yyyy", "%Y")
                      .replace("MM", "%m")
                      .replace("dd", "%d")
                      .replace("HH", "%H")
                      .replace("mm", "%M")
                      .replace("ss", "%S"))
                p2 = int(params[1])

                # 执行计算逻辑
                now = (datetime.datetime.now() + datetime.timedelta(days=p2)).strftime(p1)

            # 样例：zdt
            elif name == "zdt":
                now = datetime.datetime.now()

            else:
                return "${" + text + "}"
        else:
            # 样例：zdt.addDay(-1)
            if name == "addDay" and isinstance(now, datetime.datetime):
                p1 = int(params[0])
                now += datetime.timedelta(days=p1)

            elif name == "add" and isinstance(now, datetime.datetime):
                p1 = int(params[0])
                p2 = int(params[1])
                if p1 == 1:  # 年份变化
                    new_year = now.year + p2
                    new_day = min(now.day, calendar.monthrange(new_year, now.month)[1])
                    now = now.replace(year=new_year, day=new_day)
                elif p1 == 2:  # 月份变化
                    new_month = now.month + p2
                    new_year = now.year + new_month // 12
                    new_month = new_month % 12
                    new_day = min(now.day, calendar.monthrange(new_year, new_month)[1])
                    now = now.replace(year=new_year, month=new_month, day=new_day)
                elif p1 == 3 or p1 == 4:  # 星期变化
                    now += datetime.timedelta(days=p2 * 7)
                elif p1 == 5:  # 日期变化
                    now += datetime.timedelta(days=p2 * 1)
                elif p1 == 11:  # 小时变化
                    now += datetime.timedelta(hours=p2 * 1)
                else:
                    return "${" + text + "}"

            # 样例：zdt.format("yyyyMMdd")
            elif name == "format" and isinstance(now, datetime.datetime):
                p1 = (params[0][1:-1]
                      .replace("yyyy", "%Y")
                      .replace("MM", "%m")
                      .replace("dd", "%d")
                      .replace("HH", "%H")
                      .replace("mm", "%M")
                      .replace("ss", "%S"))
                now = now.strftime(p1)

            # 样例：zdt.getTime()
            elif name == "getTime" and isinstance(now, datetime.datetime):
                now = int(now.timestamp())

            else:
                return "${" + text + "}"

    # 返回结果
    if isinstance(now, str):
        return now
    elif isinstance(now, int):
        return str(now)
    else:
        return "${" + text + "}"


DOLPHIN_INNER_FUNCTION = re.compile(r"\$\{[^}]+}")


def run_all_inner_function(script: str) -> str:
    """运行海豚调度脚本中的所有海豚内置函数"""
    return DOLPHIN_INNER_FUNCTION.sub(lambda x: run_inner_function(x.group()), script)


if __name__ == "__main__":
    # 以下 Case 在 2024.07.31 运行
    print(run_inner_function("${start(\"yyyyMMdd\",-1)}"))  # 20240730
    print(run_inner_function("${zdt.addDay(-1).format(\"yyyyMMdd\")}"))  # 20240730
    print(run_inner_function("${zdt.addDay(-2).format(\"yyyyMMdd\")}"))  # 20240729
    print(run_inner_function("${zdt.add(2,-12).format(\"yyyyMMdd\")}"))  # 20230731
    print(run_inner_function("${zdt.add(2,-1).format(\"yyyyMMdd\")}"))  # 20240630
    print(run_inner_function("${zdt.add(2,0).format(\"yyyyMMdd\")}"))  # 20240731
    print(run_inner_function("${zdt.add(2,1).format(\"yyyyMMdd\")}"))  # 20240831
    print(run_inner_function("${zdt.add(2,2).format(\"yyyyMMdd\")}"))  # 20240930
    print(run_inner_function("${zdt.add(1,1).format(\"yyyyMMdd\")}"))  # 20250731
    print(run_inner_function("${zdt.add(3,1).format(\"yyyyMMdd\")}"))  # 20240807
    print(run_inner_function("${zdt.add(3,2).format(\"yyyyMMdd\")}"))  # 20240814
    print(run_inner_function("${zdt.add(4,1).format(\"yyyyMMdd\")}"))  # 20240807
    print(run_inner_function("${zdt.add(4,2).format(\"yyyyMMdd\")}"))  # 20240814
    print(run_inner_function("${zdt.add(5,1).format(\"yyyyMMdd\")}"))  # 20240801
    print(run_inner_function("${zdt.add(5,2).format(\"yyyyMMdd\")}"))  # 20240802
    print(run_inner_function("${zdt.add(11,1).format(\"yyyyMMddHH\")}"))  # 2024073120
    print(run_inner_function("${zdt.add(11,2).format(\"yyyyMMddHH\")}"))  # 2024073121
    print(run_inner_function("${zdt.getTime()}"))  # 1722424960
