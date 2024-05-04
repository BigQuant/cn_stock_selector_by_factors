"""cn_stock_selector_by_factors package.

A股-指标选股
"""
import re
import uuid
from collections import OrderedDict

import structlog

from bigmodule import I

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "数据/A股"
# 模块显示名
friendly_name = "A股-指标选股"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/doc/aistudio-HVwrgP4J1A#h-a股-基础选股3"
# 是否自动缓存结果
cacheable = True

logger = structlog.get_logger()

FACTORS = OrderedDict([
    ("自定义", ""),
    ("量价-换手率", "cn_stock_bar1d.turn"),
    ("估值-总市值", "cn_stock_valuation.total_market_cap"),
    ("估值-流通市值", "cn_stock_valuation.float_market_cap"),
])

DEFAULT_USER_EXPR = """-- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
-- 数据&字段: 数据文档 https://bigquant.com/data/home
-- 在这里输入选股表达式，多个条件用 AND 或者 OR 组合，会根据这个输入解析表名并构建查询和计算SQL, c_rank / c_pct_rank 是排序和百分位排序, 具体见算子文档

c_rank(
    c_normalize(cn_stock_bar1d.turn) + c_normalize(cn_stock_valuation.total_market_cap)
) BETWEEN 1 AND 100

"""

VALUE_TYPES = OrderedDict([
    ("值", "{value}"),
    ("排序值(从小到大, 1, 2, ..)", "c_rank({value})"),
    ("排序百分位值(从小到大, 0~1)", "c_pct_rank({value})"),
])

SQL_TEMPLATE = '''
SELECT
    date,
    instrument
FROM {table}
QUALIFY {expr}
ORDER BY date, instrument
'''

SQL_JOIN_TEMPLATE = '''
WITH {table_id} AS (
{sql}
)
SELECT
    {base_table_id}.*
FROM {base_table_id}
JOIN {table_id} USING(date, instrument)
'''

TABLE_NAME_RE = re.compile(r'(?<!\.)\b\w+\b(?=\.\w)')


def _build_table(ds):
    if isinstance(ds, str):
        sql = ds
    else:
        type_ = ds.type
        if type_ == "json":
            sql = ds.read()["sql"]
        elif type == "text":
            sql = ds.read()
        else:
            # bdb
            return {"sql": "", "table_id": ds.id}

    import bigdb

    table_id = f"_t_{uuid.uuid4().hex}"
    parts = [x.strip().strip(";") for x in bigdb.connect().parse_query(sql)]
    parts[-1] = f"CREATE TABLE {table_id} AS {parts[-1]}"
    sql = ";\n".join(parts)
    if sql:
        sql += ";\n"

    return {
        "sql": sql,
        "table_id": table_id,
    }


def _build_join_sql(base_query, sql):
    base_table = _build_table(base_query)
    table_id = f"_t_{uuid.uuid4().hex}"
    return base_table["sql"] + SQL_JOIN_TEMPLATE.format(
        base_table_id=base_table["table_id"],
        table_id=table_id,
        sql=sql
    )


def _parse_expr(expr: str) -> dict:
    lines = []
    for line in expr.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("--") or line.startswith("#"):
            continue
        lines.append(line)
    expr = " ".join(lines)

    tables = TABLE_NAME_RE.findall(expr)

    return {"expr": expr, "tables": tables}


def run(
    base_query: I.port("基础查询", specific_type_name="DataSource") = None,
    factor: I.choice("指标/因子", list(FACTORS.keys())) = list(FACTORS.keys())[1],
    value_type: I.choice("值类型", list(VALUE_TYPES.keys())) = list(VALUE_TYPES.keys())[0],
    range_lower: I.float("区间下限，包括下限，为空表示无下限/最小值") = None,
    range_upper : I.float("区间上限，包括上限，，为空表示无上限/最大值") = None,
    user_factor_expr: I.code("自定义选股表达式，当指标/因子选择为自定义时，才启用", default=DEFAULT_USER_EXPR, auto_complete_type="sql") = None,
)->[
    I.port('输出', 'data')
]:
    import dai

    factor = FACTORS[factor]
    parsed = _parse_expr(factor or user_factor_expr)

    # user factor
    if factor:
        expr = f"{VALUE_TYPES[value_type].format(value=parsed['expr'])}"
        if range_lower is None and range_upper is None:
            raise Exception("range_lower / range_lower not set")
        if range_lower is not None and range_upper is not None:
            expr += f" BETWEEN {range_lower} AND {range_upper}"
        elif range_lower is not None:
            expr += f" >= {range_lower}"
        else:
            expr += f" <= {range_upper}"
    else:
        logger.info("build user factor ..")
        # 用户给出的是完整条件表达式
        expr = parsed['expr']

    tables = list(sorted(set(parsed["tables"])))
    for i in range(1, len(tables)):
        tables[i] = f"{tables[i]} USING(date, instrument)"

    sql = SQL_TEMPLATE.format(
        table="\n    JOIN ".join(tables),
        expr=expr,
    )

    if base_query is not None:
        sql = _build_join_sql(base_query, sql)
    return I.Outputs(data=dai.DataSource.write_json({"sql": sql}, base_ds=base_query))


def post_run(outputs):
    """后置运行函数"""
    return outputs
