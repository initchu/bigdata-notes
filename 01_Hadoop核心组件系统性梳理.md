# Hadoop 核心组件系统性梳理

## 1. Hadoop 是什么

Hadoop 是 Apache 开源的分布式计算基础框架，专为海量数据的存储与计算而生。它的核心思想来源于 Google 的三篇论文（GFS、MapReduce、BigTable），解决了单机无法处理大规模数据的根本问题。

## 2. 三大核心组件

| 组件 | 职责 |
|------|------|
| HDFS | 分布式文件系统，负责海量数据的持久化存储 |
| MapReduce | 分布式计算框架，负责海量数据的并行计算 |
| YARN | 资源管理系统，负责集群资源的统一调度与管理 |

三者关系：**HDFS 存数据，MapReduce 算数据，YARN 管资源**。

## 3. 大数据生态圈

Hadoop 是大数据生态的基石，围绕它形成了完整的生态体系：

- **数据采集层**：Flume（日志采集）、Sqoop（关系型数据库导入导出）
- **数据存储层**：HDFS、HBase（列式存储）
- **数据计算层**：MapReduce（离线批处理）、Spark（内存计算）、Flink（流计算）
- **数据查询层**：Hive（SQL on Hadoop）、Impala（实时查询）
- **资源调度层**：YARN
- **协调服务层**：ZooKeeper
- **任务调度层**：Oozie、Azkaban

## 4. 发行版选择

- **Apache 原生版**：完全开源，版本更新快，但各组件兼容性需自行维护，适合学习研究
- **CDH（Cloudera）**：企业最广泛使用，组件集成度高，稳定性好
- **HDP（Hortonworks）**：已与 Cloudera 合并
- **星环、华为 FusionInsight**：国内商业发行版

> 企业生产环境推荐 CDH，学习环境使用 Apache 原生版即可。

## 5. Hadoop 的优缺点

**优点：**
- 高可靠性：数据多副本存储，单点故障不影响整体
- 高扩展性：可线性扩展至数千节点
- 高效性：并行计算，充分利用集群资源
- 高容错性：失败任务自动重新调度

**缺点：**
- 不适合低延迟数据访问（毫秒级响应）
- 不适合大量小文件存储（NameNode 内存压力大）
- 不支持多用户并发写入及任意修改文件

## 6. 集群架构

```
Client
  │
  ├── NameNode (HDFS主节点)
  │     └── DataNode × N
  │
  ├── ResourceManager (YARN主节点)
  │     └── NodeManager × N
  │
  └── SecondaryNameNode (辅助NameNode，非HA模式)
```

**HA 高可用架构**（生产必备）：
- HDFS HA：两个 NameNode（Active/Standby），通过 ZooKeeper 实现自动故障转移，JournalNode 同步元数据
- YARN HA：两个 ResourceManager（Active/Standby），同样依赖 ZooKeeper

## 7. 高频面试题

**Q：Hadoop 1.x 和 2.x 的核心区别？**
- 1.x：JobTracker 负责资源管理和作业调度，单点瓶颈严重
- 2.x：引入 YARN，将资源管理与作业调度解耦，支持多计算框架（不再局限于 MR）

**Q：为什么 Hadoop 不适合存储大量小文件？**
- NameNode 在内存中维护文件系统元数据，每个文件/块约占 150 字节内存
- 大量小文件会撑爆 NameNode 内存，且 MapReduce 处理小文件效率极低

**Q：Hadoop 集群规划原则？**
- NameNode、ResourceManager 部署在高配机器上（内存优先）
- DataNode、NodeManager 同机部署，充分利用本地数据
- ZooKeeper 奇数节点部署（3 或 5 个）
