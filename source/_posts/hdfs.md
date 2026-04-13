---
title: HDFS 核心组件系统性梳理
date: 2024-01-02
categories: [大数据]
tags: [HDFS, 分布式存储]
---

## 1. HDFS 是什么

HDFS（Hadoop Distributed File System）是 Hadoop 的分布式文件系统，设计目标是在普通商用硬件集群上可靠地存储超大文件。它采用主从架构，将文件切分为固定大小的 Block 分散存储在多个 DataNode 上。

## 2. 设计假设与目标

- 硬件故障是常态，而非异常 → 系统需具备自动容错能力
- 流式数据访问（高吞吐优先，非低延迟）
- 支持超大文件（GB、TB 级别）
- 一次写入、多次读取的访问模式
- 移动计算比移动数据代价更低（数据本地化计算）

## 3. 架构核心组件

```
Client
  │
  ├── NameNode（主节点）
  │     ├── 管理文件系统命名空间（目录树）
  │     ├── 维护文件与 Block 的映射关系
  │     └── 记录每个 Block 存储在哪些 DataNode 上
  │
  ├── SecondaryNameNode（辅助节点，非备份）
  │     ├── 定期合并 EditLog 和 FsImage
  │     └── 减轻 NameNode 的内存压力
  │
  └── DataNode（从节点，可有多个）
        ├── 实际存储 Block 数据
        ├── 定期向 NameNode 发送心跳（默认 3s）
        └── 定期上报本机 Block 列表（默认 6h）
```

## 4. 核心概念

### Block（数据块）
- Hadoop 2.x 默认 Block 大小：**128MB**（1.x 为 64MB）
- 文件按 Block 切分，不足一个 Block 的部分不会占满整个 Block 空间
- Block 大小设置原则：寻址时间占传输时间的 1% 为最优（通常传输时间 100ms，寻址 10ms）

### 副本因子（Replication Factor）
- 默认副本数：**3**
- 副本放置策略（机架感知）：
  - 第 1 个副本：写入客户端所在节点（或随机选一个节点）
  - 第 2 个副本：与第 1 个副本不同机架的节点
  - 第 3 个副本：与第 2 个副本同机架的不同节点

### 文件系统命名空间
- 支持传统的目录层级结构
- 支持创建、删除、移动、重命名文件/目录
- 不支持硬链接和软链接（目前）

## 5. HDFS 读数据流程

```
1. Client 向 NameNode 请求读取文件
2. NameNode 返回文件对应的 Block 列表及各 Block 所在 DataNode 地址
3. Client 就近选择 DataNode，逐个 Block 读取
4. DataNode 将 Block 数据流式传输给 Client
5. Client 合并所有 Block，完成读取
```

**关键点**：Client 直接与 DataNode 通信读取数据，NameNode 不参与数据传输，避免成为瓶颈。

## 6. HDFS 写数据流程

```
1. Client 向 NameNode 请求上传文件
2. NameNode 检查权限、目录是否存在，返回允许上传
3. Client 请求上传第一个 Block，NameNode 返回 3 个 DataNode 地址（DN1、DN2、DN3）
4. Client 与 DN1 建立 Pipeline，DN1 与 DN2，DN2 与 DN3 依次建立连接
5. Client 以 Packet（64KB）为单位向 Pipeline 写入数据
6. 每个 Packet 写入后，逐级返回 ACK 确认
7. 当前 Block 写完后，重复步骤 3，直到所有 Block 写完
8. Client 通知 NameNode 文件写入完成
```

**关键点**：Pipeline 流水线写入，三副本并行写，效率高且保证一致性。

## 7. NameNode 工作机制

### FsImage 与 EditLog
- **FsImage**：文件系统元数据的持久化快照（全量）
- **EditLog**：记录每次元数据变更操作的日志（增量）
- NameNode 启动时：加载 FsImage + 回放 EditLog → 恢复内存中的元数据

### SecondaryNameNode 的作用
- 定期（默认 1 小时）将 EditLog 合并到 FsImage，生成新的 FsImage
- 防止 EditLog 无限增大，加快 NameNode 重启速度
- **注意**：SecondaryNameNode 不是 NameNode 的热备，NameNode 宕机后不能立即接管

### CheckPoint 流程
```
1. SecondaryNameNode 请求 NameNode 滚动 EditLog
2. 拷贝 FsImage 和新生成的 EditLog 到 SecondaryNameNode
3. 在 SecondaryNameNode 上合并 FsImage + EditLog → 新 FsImage
4. 将新 FsImage 推送回 NameNode
```

## 8. DataNode 工作机制

- 启动后向 NameNode 注册，上报本机 Block 列表
- 每 3 秒发送一次心跳，携带节点状态信息
- NameNode 超过 10 分钟（10 次心跳超时 × 2 + 10min）未收到心跳，认为该节点死亡
- 节点死亡后，NameNode 触发副本补全机制，将该节点上的 Block 在其他节点重新复制

## 9. 安全模式

- NameNode 启动时进入安全模式，此时只读，不允许任何写操作
- 等待 DataNode 上报 Block 信息，当满足最小副本条件（默认 99.9% 的 Block 达到最小副本数 1）后，等待 30 秒退出安全模式
- 可手动操作：`hdfs dfsadmin -safemode leave`

## 10. 优缺点

**优点：**
- 高容错：多副本自动恢复
- 高吞吐：流式读写，适合批处理
- 适合大文件：TB 级文件轻松应对
- 跨平台：运行在普通商用硬件上

**缺点：**
- 不适合低延迟访问（毫秒级）
- 不适合大量小文件（NameNode 内存瓶颈）
- 不支持并发写入（同一时刻只有一个写入者）
- 不支持随机修改（只能追加写）

## 11. 高频面试题

**Q：HDFS 的 Block 为什么默认是 128MB，不是更大或更小？**
- 太小：Block 数量多，NameNode 内存压力大，寻址开销占比高
- 太大：Map 任务处理的数据量过大，并行度下降，且一个 Block 对应一个 MapTask
- 128MB 是在寻址时间和传输效率之间的经验最优值

**Q：NameNode 和 SecondaryNameNode 的区别？**
- NameNode：主节点，管理元数据，常驻内存
- SecondaryNameNode：辅助节点，定期合并 FsImage 和 EditLog，不能替代 NameNode

**Q：HDFS 写入时某个 DataNode 宕机怎么办？**
- Pipeline 中断，Client 收到异常
- Client 重新向 NameNode 申请新的 DataNode 加入 Pipeline
- 已写入的 Packet 重新发送，保证数据完整性
- NameNode 后续触发副本补全，恢复到目标副本数
