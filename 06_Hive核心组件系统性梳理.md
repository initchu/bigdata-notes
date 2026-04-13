# Hive 核心组件系统性梳理

## 1. Hive 产生背景

MapReduce 编程门槛高，数据分析师不会 Java，但熟悉 SQL。Hive 应运而生，将 SQL 翻译为 MapReduce 作业，让分析师用 SQL 就能处理 HDFS 上的海量数据。

## 2. Hive 是什么

Hive 是基于 Hadoop 的数据仓库工具，核心功能：
- 将结构化数据文件映射为数据库表
- 提供 HQL（Hive Query Language，类 SQL）查询接口
- 将 HQL 翻译为 MapReduce / Tez / Spark 作业执行

**本质**：Hive 不存储数据，数据存在 HDFS；Hive 不计算，计算由 MR/Tez/Spark 完成。

## 3. 优缺点

**优点：**
- 学习成本低，SQL 即可操作海量数据
- 支持自定义函数（UDF/UDAF/UDTF）
- 统一元数据管理（Metastore）
- 适合离线数据仓库建设

**缺点：**
- 延迟高（底层是 MR，分钟级）
- 不支持行级更新/删除（ACID 支持有限）
- 不适合实时查询
- 调优复杂

## 4. Hive 架构

```
Client（CLI / JDBC / WebUI）
  │
  ├── Driver（核心）
  │     ├── SQL Parser（SQL 解析）
  │     ├── Semantic Analyzer（语义分析）
  │     ├── Logical Plan Generator（逻辑计划）
  │     ├── Optimizer（优化器）
  │     └── Physical Plan Generator（物理计划 → MR Job）
  │
  ├── Metastore（元数据存储）
  │     └── MySQL（生产环境）/ Derby（内嵌，仅测试）
  │
  └── Execution Engine
        └── MapReduce / Tez / Spark
```

**Metastore 存储的内容：**
- 数据库、表、分区的定义
- 表的 Schema（列名、类型）
- 数据在 HDFS 上的存储路径
- SerDe（序列化/反序列化）信息

## 5. Hive 数据模型

```
Database（数据库）
  └── Table（表）
        ├── 内部表（Managed Table）
        ├── 外部表（External Table）
        └── 分区表（Partitioned Table）
              └── Partition（分区）
                    └── Bucket（分桶）
```

### 内部表 vs 外部表

| 对比项 | 内部表 | 外部表 |
|--------|--------|--------|
| 数据管理 | Hive 完全管理 | 用户自己管理 |
| DROP 操作 | 删除元数据 + HDFS 数据 | 只删除元数据，HDFS 数据保留 |
| 适用场景 | 中间表、临时表 | 原始数据表、多系统共享数据 |
| 创建语法 | `CREATE TABLE` | `CREATE EXTERNAL TABLE` |

> 生产环境原始数据层（ODS）一律使用外部表，防止误删数据。

### 分区表
- 将数据按某个字段值分目录存储，查询时可跳过不相关分区（分区裁剪）
- 本质是 HDFS 上的子目录

```sql
-- 创建分区表
CREATE TABLE logs (
  user_id STRING,
  action  STRING
) PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 加载数据到分区
LOAD DATA INPATH '/data/logs/2024-01-01' INTO TABLE logs PARTITION (dt='2024-01-01');

-- 动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE logs PARTITION (dt)
SELECT user_id, action, dt FROM source_table;
```

## 6. Hive DDL & DML

### 常用 DDL

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS mydb LOCATION '/user/hive/mydb';

-- 创建表
CREATE TABLE t1 (
  id   INT,
  name STRING,
  age  INT
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE;

-- 修改表
ALTER TABLE t1 RENAME TO t2;
ALTER TABLE t1 ADD COLUMNS (score DOUBLE);

-- 删除
DROP TABLE t1;          -- 内部表：删元数据 + 数据；外部表：只删元数据
TRUNCATE TABLE t1;      -- 清空数据，只对内部表有效
```

### drop vs truncate 区别
- `DROP`：删除表定义（元数据）和数据，表不再存在
- `TRUNCATE`：只清空数据，保留表结构，且只对内部表有效

### 数据加载的 N 种方式

```sql
-- 1. LOAD DATA（推荐，直接移动文件，效率最高）
LOAD DATA LOCAL INPATH '/local/path' INTO TABLE t1;          -- 从本地
LOAD DATA INPATH '/hdfs/path' INTO TABLE t1;                 -- 从 HDFS（移动）

-- 2. INSERT INTO ... SELECT（从其他表查询插入）
INSERT INTO TABLE t1 SELECT * FROM t2;

-- 3. INSERT OVERWRITE（覆盖写入）
INSERT OVERWRITE TABLE t1 SELECT * FROM t2;

-- 4. CREATE TABLE AS SELECT（建表同时插入数据）
CREATE TABLE t1 AS SELECT * FROM t2;
```

> 为什么不推荐 `INSERT VALUES`？Hive 底层是 MR，每次 INSERT VALUES 都会启动一个 MR 作业，效率极低。

### export & import

```sql
-- 导出（包含数据 + 元数据）
EXPORT TABLE t1 TO '/export/path';

-- 导入（恢复表结构和数据）
IMPORT TABLE t1 FROM '/export/path';
```

## 7. Hive 核心函数

### 日期函数
```sql
current_date()                          -- 当前日期
current_timestamp()                     -- 当前时间戳
date_format('2024-01-15', 'yyyy-MM')    -- 日期格式化
datediff('2024-01-15', '2024-01-01')    -- 日期差（天）
date_add('2024-01-01', 7)               -- 日期加减
year/month/day/hour/minute/second()     -- 提取日期部分
```

### 字符串函数
```sql
concat(s1, s2)                          -- 字符串拼接
concat_ws(',', s1, s2)                  -- 带分隔符拼接
substr(str, pos, len)                   -- 截取子串
length(str)                             -- 字符串长度
upper/lower(str)                        -- 大小写转换
trim/ltrim/rtrim(str)                   -- 去空格
regexp_replace(str, pattern, replace)   -- 正则替换
split(str, pattern)                     -- 字符串分割为数组
```

### JSON 处理
```sql
get_json_object('{"name":"Tom","age":18}', '$.name')   -- 提取 JSON 字段
json_tuple(json_str, 'name', 'age')                     -- 一次提取多个字段（效率更高）
```

### URL 函数
```sql
parse_url('http://example.com/path?k=v', 'HOST')        -- 提取 HOST
parse_url('http://example.com/path?k=v', 'QUERY', 'k')  -- 提取查询参数
```

### 条件函数
```sql
if(condition, true_val, false_val)
case when c1 then v1 when c2 then v2 else v3 end
coalesce(v1, v2, v3)                    -- 返回第一个非 NULL 值
nvl(value, default_value)               -- NULL 替换
```

### 行列转换
```sql
-- 列转行（一行变多行）
SELECT explode(array_col) AS item FROM t1;
SELECT posexplode(array_col) AS (pos, item) FROM t1;

-- 配合 LATERAL VIEW 使用
SELECT id, item
FROM t1
LATERAL VIEW explode(tags) tmp AS item;

-- 行转列（多行变一行）
SELECT id, collect_list(value) AS vals FROM t1 GROUP BY id;   -- 保留重复
SELECT id, collect_set(value)  AS vals FROM t1 GROUP BY id;   -- 去重
```

## 8. 窗口分析函数

窗口函数在不改变行数的情况下，对每行数据计算基于"窗口"范围内的聚合值。

```sql
函数名() OVER (
  [PARTITION BY 分组字段]
  [ORDER BY 排序字段]
  [ROWS/RANGE BETWEEN 起始行 AND 结束行]
)
```

### 聚合类窗口函数
```sql
-- 累计求和
SUM(amount) OVER (PARTITION BY user_id ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- 移动平均
AVG(amount) OVER (PARTITION BY user_id ORDER BY dt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
```

### 排名函数
```sql
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)   -- 连续不重复排名：1,2,3,4
RANK()       OVER (PARTITION BY dept ORDER BY salary DESC)   -- 跳跃排名：1,2,2,4
DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC)   -- 连续排名：1,2,2,3
NTILE(4)     OVER (PARTITION BY dept ORDER BY salary DESC)   -- 分成 N 桶
```

### 偏移函数
```sql
LAG(col, n, default)  OVER (...)   -- 取当前行往前第 n 行的值
LEAD(col, n, default) OVER (...)   -- 取当前行往后第 n 行的值
FIRST_VALUE(col)      OVER (...)   -- 窗口内第一个值
LAST_VALUE(col)       OVER (...)   -- 窗口内最后一个值
```

### 分布函数
```sql
CUME_DIST()    OVER (...)   -- 累积分布（当前行值 ≤ 该值的行数 / 总行数）
PERCENT_RANK() OVER (...)   -- 百分比排名（rank-1）/（总行数-1）
```

## 9. 自定义函数（UDF）

| 类型 | 说明 | 输入/输出 |
|------|------|-----------|
| UDF | 普通自定义函数 | 一行输入 → 一行输出 |
| UDAF | 自定义聚合函数 | 多行输入 → 一行输出 |
| UDTF | 自定义表生成函数 | 一行输入 → 多行输出 |

**UDF 开发步骤（新版 GenericUDF）：**
```java
public class MyUDF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) { ... }

    @Override
    public Object evaluate(DeferredObject[] arguments) { ... }

    @Override
    public String getDisplayString(String[] children) { ... }
}
```

**注册使用：**
```sql
-- 临时注册（当前 Session 有效）
ADD JAR /path/to/udf.jar;
CREATE TEMPORARY FUNCTION my_func AS 'com.example.MyUDF';

-- 永久注册
CREATE FUNCTION my_func AS 'com.example.MyUDF' USING JAR 'hdfs:///path/to/udf.jar';
```

## 10. Hive 调优

### 4 大 BY
| 关键字 | 说明 |
|--------|------|
| ORDER BY | 全局排序，只有一个 Reducer，慎用 |
| SORT BY | 每个 Reducer 内部有序，非全局有序 |
| DISTRIBUTE BY | 控制数据分发到哪个 Reducer（类似 Partitioner） |
| CLUSTER BY | = DISTRIBUTE BY + SORT BY（同一字段），但只能升序 |

### 数据倾斜解决方案

**GROUP BY 倾斜：**
```sql
-- 开启 Map 端聚合
SET hive.map.aggr=true;
-- 开启负载均衡（两阶段聚合）
SET hive.groupby.skewindata=true;
```

**COUNT(DISTINCT) 倾斜：**
```sql
-- 改写为 GROUP BY 子查询
SELECT COUNT(*) FROM (
  SELECT user_id FROM t1 GROUP BY user_id
) tmp;
```

**JOIN 倾斜：**
```sql
-- 小表 JOIN：开启 MapJoin
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;  -- 25MB 以下自动转 MapJoin

-- 大表 JOIN 热点 Key：加随机前缀打散
SELECT /*+ SKEWJOIN(t1) */ ...
```

### 其他调优参数
```sql
-- 本地模式（小数据量，避免启动 MR 开销）
SET hive.exec.mode.local.auto=true;

-- 并行执行（无依赖的 Stage 并行跑）
SET hive.exec.parallel=true;

-- 严格模式（防止全表扫描等危险操作）
SET hive.mapred.mode=strict;

-- 合理设置 Reduce 数量
SET mapreduce.job.reduces=10;
```

## 11. 高频面试题

**Q：Hive 内部表和外部表的区别及使用场景？**
- 内部表：Hive 完全管理，DROP 时数据一并删除，适合中间表
- 外部表：数据独立于 Hive，DROP 只删元数据，适合原始数据，防止误删

**Q：Hive 分区表的作用？**
- 将数据按分区字段分目录存储，查询时通过分区裁剪跳过不相关目录，大幅减少扫描数据量

**Q：ROW_NUMBER、RANK、DENSE_RANK 的区别？**
- ROW_NUMBER：连续不重复，1,2,3,4,5
- RANK：相同值并列，之后跳跃，1,2,2,4,5
- DENSE_RANK：相同值并列，之后连续，1,2,2,3,4

**Q：Hive 如何解决小文件问题？**
- 输入端：`CombineHiveInputFormat` 合并小文件为一个 InputSplit
- 输出端：`hive.merge.mapfiles=true` 合并 Map 输出的小文件
- 定期使用 `INSERT OVERWRITE` 重写表，合并历史小文件
