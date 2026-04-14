---
title: YARN 核心组件系统性梳理
date: 2024-01-04
categories: [大数据]
tags: [YARN, 资源调度]
slug: YARN核心组件系统性梳理
---

## 1. YARN 产生背景

Hadoop 1.x 中，JobTracker 同时承担资源管理和作业调度两大职责，存在严重问题：
- **单点瓶颈**：所有作业都由 JobTracker 管理，集群规模受限
- **资源利用率低**：资源以 slot（Map slot / Reduce slot）为单位静态划分，无法跨类型共享
- **不支持多计算框架**：只能运行 MapReduce，无法运行 Spark 等其他框架

YARN（Yet Another Resource Negotiator）在 Hadoop 2.x 引入，将资源管理与作业调度彻底解耦。

## 2. 架构核心组件

```
Client
  │
  ├── ResourceManager（全局资源管理，主节点）
  │     ├── Scheduler（资源调度器）
  │     └── ApplicationsManager（应用管理）
  │
  └── NodeManager × N（单节点资源管理，从节点）
        └── Container（资源隔离单元）
              └── ApplicationMaster（每个应用一个）
                    ├── MapTask
                    └── ReduceTask
```

## 3. 核心组件职责

### ResourceManager（RM）
- 整个集群资源的最高仲裁者
- **Scheduler**：纯粹的资源调度，不负责应用监控和容错
- **ApplicationsManager**：接受作业提交，为每个应用启动 ApplicationMaster，监控 AM 并在失败时重启

### NodeManager（NM）
- 单个节点的资源管理者
- 向 RM 注册并定期汇报节点资源使用情况（心跳）
- 接收 RM/AM 的指令，启动/停止 Container
- 监控 Container 的资源使用，超限则 kill

### ApplicationMaster（AM）
- 每个应用（Job）独立的一个 AM
- 向 RM 申请资源（Container）
- 与 NM 协作启动/停止任务
- 负责应用内部的任务调度和容错

### Container
- YARN 的资源抽象单元，封装了 CPU、内存等资源
- 任务在 Container 中运行，实现资源隔离

## 4. YARN 工作原理（作业提交全流程）

```
1. Client 向 RM 提交应用（Job）
2. RM 为该应用分配第一个 Container，在其中启动 ApplicationMaster
3. AM 向 RM 注册，Client 可通过 RM 查询应用状态
4. AM 根据任务需求向 RM 申请 Container 资源
5. RM 的 Scheduler 根据调度策略分配 Container
6. AM 拿到 Container 后，与对应 NM 通信，启动任务（MapTask/ReduceTask）
7. 各 Container 中的任务向 AM 汇报进度和状态
8. 应用完成后，AM 向 RM 注销并释放资源
```

## 5. YARN 容错性

| 组件 | 故障处理 |
|------|----------|
| ResourceManager | HA 模式下 Standby RM 接管，依赖 ZooKeeper 选主 |
| NodeManager | RM 超时未收到心跳，标记节点失效，AM 重新申请资源 |
| ApplicationMaster | RM 检测到 AM 失败，重新启动 AM（默认最多重试 2 次） |
| Container/Task | AM 检测到任务失败，重新申请 Container 执行（默认最多重试 4 次） |

## 6. 以 YARN 为核心的生态系统

YARN 的最大价值在于成为通用资源管理平台，支持多种计算框架共享集群资源：

```
┌─────────────────────────────────────────┐
│  MapReduce  │  Spark  │  Flink  │  Tez  │
├─────────────────────────────────────────┤
│                  YARN                   │
├─────────────────────────────────────────┤
│                  HDFS                   │
└─────────────────────────────────────────┘
```

## 7. YARN 调度器

### FIFO Scheduler（先进先出）
- 按提交顺序排队执行
- 简单，但大作业会阻塞小作业
- 不适合生产环境

### Capacity Scheduler（容量调度器）
- Hadoop 默认调度器
- 将集群资源划分为多个队列，每个队列有最小容量保证
- 队列内部 FIFO
- 支持队列间资源借用（弹性）
- 适合多租户场景

**核心配置：**
```xml
<!-- capacity-scheduler.xml -->
<property>
  <name>yarn.scheduler.capacity.root.queues</name>
  <value>default,hive,spark</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.default.capacity</name>
  <value>40</value>  <!-- 40% 资源 -->
</property>
```

### Fair Scheduler（公平调度器）
- CDH 默认调度器
- 所有应用公平共享资源，动态平衡
- 支持抢占（preemption）：资源不足时可抢占其他应用的资源
- 适合多用户共享集群

### 三种调度器对比

| 特性 | FIFO | Capacity | Fair |
|------|------|----------|------|
| 多队列 | ✗ | ✓ | ✓ |
| 资源保证 | ✗ | ✓ | ✓ |
| 资源抢占 | ✗ | ✗ | ✓ |
| 适用场景 | 测试 | 企业多部门 | 多用户共享 |

## 8. 作业提交到 YARN 运行

```bash
# 打包并提交到 YARN
hadoop jar your-job.jar com.example.Driver \
  /input/path \
  /output/path

# 查看作业状态
yarn application -list
yarn application -status <applicationId>
yarn application -kill <applicationId>

# 查看日志
yarn logs -applicationId <applicationId>
```

## 9. 高频面试题

**Q：YARN 和 Hadoop 1.x 的 JobTracker 有什么区别？**
- JobTracker：资源管理 + 作业调度合二为一，单点瓶颈，只支持 MR
- YARN：ResourceManager 负责资源管理，ApplicationMaster 负责作业调度，支持任意计算框架

**Q：YARN 中 Container 是什么？**
- Container 是 YARN 的资源分配单位，封装了一定量的 CPU 和内存
- 任务在 Container 中运行，通过 cgroups 实现资源隔离
- Container 的大小由 AM 申请时指定，RM 根据调度策略决定是否分配

**Q：ApplicationMaster 挂了怎么办？**
- RM 检测到 AM 心跳超时，认为 AM 失败
- RM 重新启动一个新的 AM（在新的 Container 中）
- 新 AM 可以从之前的状态恢复（如果开启了 AM 状态持久化）

**Q：Capacity Scheduler 如何防止某个队列占用过多资源？**
- 每个队列设置 `maximum-capacity`，限制最大可用资源比例
- 即使其他队列空闲，该队列也不能超过 maximum-capacity
