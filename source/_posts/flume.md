---
title: Flume 核心组件系统性梳理
date: 2024-01-07
categories: [大数据]
tags: [Flume, 日志采集]
permalink: /Flume核心组件系统性梳理/
---

## 1. Flume 产生背景

在大数据场景中，业务系统每天产生海量日志（Nginx 访问日志、应用日志、埋点日志等），这些日志分散在各个服务器上，需要实时或准实时地收集到 HDFS、Kafka 等存储系统中。Flume 就是专门解决这一问题的日志收集系统。

**采集 vs 收集的区别：**
- **采集**：从数据源获取数据（Flume 做的事）
- **收集**：汇聚多路数据到统一存储（也是 Flume 的能力）

## 2. Flume 是什么

Flume 是 Cloudera 开发、Apache 开源的分布式、可靠、高可用的日志收集系统。核心特点：
- 基于流式数据流（Streaming Data Flow）
- 声明式配置，无需编码
- 支持多种数据源和目标
- 内置可靠性保证（At-Least-Once）

## 3. 竞品对比

| 工具 | 特点 | 适用场景 |
|------|------|----------|
| Flume | 配置简单，与 Hadoop 生态集成好 | 日志收集到 HDFS/Kafka |
| Logstash | ELK 生态，功能强大 | 日志收集到 Elasticsearch |
| Filebeat | 轻量级，资源占用少 | 替代 Logstash 做数据采集 |
| Kafka Connect | Kafka 原生，吞吐高 | 数据库/消息队列数据同步 |
| Sqoop | 专注关系型数据库 | MySQL/Oracle ↔ HDFS |

## 4. 核心组件

```
数据源
  │
  ├── Source（数据源）
  │     接收外部数据，转换为 Event
  │
  ├── Channel（通道）
  │     缓冲 Source 和 Sink 之间的数据
  │
  └── Sink（数据目标）
        从 Channel 取数据，写入目标系统
```

**Event（事件）**：Flume 的数据传输单元，由 Header（Map<String,String>）和 Body（byte[]）组成。

### Source 类型

| Source | 说明 |
|--------|------|
| NetcatSource | 监听 TCP/UDP 端口，接收文本数据 |
| ExecSource | 执行 shell 命令，收集输出（如 `tail -F`） |
| SpoolingDirSource | 监控目录，收集新增文件 |
| TailDirSource | 监控多个文件的新增内容，支持断点续传 |
| AvroSource | 接收 Avro 格式数据（用于 Agent 级联） |
| KafkaSource | 从 Kafka 消费数据 |

### Channel 类型

| Channel | 说明 | 特点 |
|---------|------|------|
| MemoryChannel | 数据存内存 | 速度快，但 Agent 宕机数据丢失 |
| FileChannel | 数据存磁盘文件 | 可靠，但速度慢 |
| KafkaChannel | 数据存 Kafka | 高吞吐，高可靠，推荐生产使用 |

### Sink 类型

| Sink | 说明 |
|------|------|
| HDFSSink | 写入 HDFS（最常用） |
| AvroSink | 发送给下游 AvroSource（Agent 级联） |
| KafkaSink | 写入 Kafka |
| LoggerSink | 打印到日志（测试用） |
| HBaseSink | 写入 HBase |

## 5. Agent 配置文件编写

每个 Flume 进程称为一个 **Agent**，通过配置文件定义数据流。

```properties
# 定义组件名称
agent.sources  = s1
agent.channels = c1
agent.sinks    = k1

# Source 配置
agent.sources.s1.type     = TAILDIR
agent.sources.s1.filegroups = f1
agent.sources.s1.filegroups.f1 = /var/log/app/.*\.log
agent.sources.s1.positionFile = /opt/flume/position/taildir_position.json
agent.sources.s1.channels = c1

# Channel 配置
agent.channels.c1.type     = file
agent.channels.c1.checkpointDir = /opt/flume/checkpoint
agent.channels.c1.dataDirs      = /opt/flume/data

# Sink 配置
agent.sinks.k1.type       = hdfs
agent.sinks.k1.hdfs.path  = /data/logs/%Y-%m-%d/%H
agent.sinks.k1.hdfs.filePrefix    = log-
agent.sinks.k1.hdfs.rollInterval  = 3600    # 每小时滚动一个文件
agent.sinks.k1.hdfs.rollSize      = 134217728  # 128MB 滚动
agent.sinks.k1.hdfs.rollCount     = 0
agent.sinks.k1.hdfs.fileType      = DataStream
agent.sinks.k1.hdfs.useLocalTimeStamp = true
agent.sinks.k1.channel    = c1
```

## 6. 实战场景

### 场景一：监控文件新增内容 → HDFS

```properties
# 使用 TailDirSource 监控日志文件，FileChannel 保证可靠性，HDFSSink 写入 HDFS
agent.sources.s1.type = TAILDIR
agent.sources.s1.filegroups.f1 = /var/log/nginx/access.log
agent.sources.s1.positionFile = /opt/flume/position/nginx.json
```

### 场景二：监控目录新增文件 → HDFS

```properties
# 使用 SpoolingDirSource，已处理的文件会被重命名（加 .COMPLETED 后缀）
agent.sources.s1.type = spooldir
agent.sources.s1.spoolDir = /data/input
agent.sources.s1.fileSuffix = .COMPLETED
agent.sources.s1.deletePolicy = never
```

### 场景三：TAILDIR 断点续传

TailDirSource 是生产环境最推荐的 Source：
- 支持监控多个目录/文件（正则匹配）
- 将读取位置（offset）持久化到 JSON 文件（positionFile）
- Agent 重启后从上次位置继续读取，不丢数据、不重复读

```properties
agent.sources.s1.type = TAILDIR
agent.sources.s1.positionFile = /opt/flume/position/taildir.json
agent.sources.s1.filegroups = f1 f2
agent.sources.s1.filegroups.f1 = /var/log/app1/.*\.log
agent.sources.s1.filegroups.f2 = /var/log/app2/.*\.log
```

## 7. 多 Agent 级联（生产场景）

生产环境通常是多层 Agent 架构：

```
业务服务器（多台）          汇聚层              存储层
┌──────────────┐
│ Agent（采集）│ ──Avro──┐
└──────────────┘         │
┌──────────────┐         ├──► Agent（汇聚）──► HDFS / Kafka
│ Agent（采集）│ ──Avro──┤
└──────────────┘         │
┌──────────────┐         │
│ Agent（采集）│ ──Avro──┘
└──────────────┘
```

**采集层 Agent（AvroSink）：**
```properties
agent.sinks.k1.type    = avro
agent.sinks.k1.hostname = aggregator-host
agent.sinks.k1.port    = 4141
```

**汇聚层 Agent（AvroSource）：**
```properties
agent.sources.s1.type = avro
agent.sources.s1.bind = 0.0.0.0
agent.sources.s1.port = 4141
```

## 8. ChannelSelector（扇出）

一个 Source 将数据发送到多个 Channel：

```
Source
  ├── Channel1 → Sink1（HDFS）
  └── Channel2 → Sink2（Kafka）
```

**Replicating（复制，默认）**：数据复制到所有 Channel
```properties
agent.sources.s1.selector.type = replicating
agent.sources.s1.channels = c1 c2
```

**Multiplexing（路由）**：根据 Event Header 中的字段值路由到不同 Channel
```properties
agent.sources.s1.selector.type = multiplexing
agent.sources.s1.selector.header = logType
agent.sources.s1.selector.mapping.access = c1
agent.sources.s1.selector.mapping.error  = c2
agent.sources.s1.selector.default        = c1
```

## 9. SinkProcessor（负载均衡/故障转移）

多个 Sink 组成 SinkGroup，通过 SinkProcessor 管理：

**Failover（故障转移）**：优先使用高优先级 Sink，故障时切换到低优先级
```properties
agent.sinkgroups = sg1
agent.sinkgroups.sg1.sinks = k1 k2
agent.sinkgroups.sg1.processor.type = failover
agent.sinkgroups.sg1.processor.priority.k1 = 10
agent.sinkgroups.sg1.processor.priority.k2 = 5
```

**Load Balance（负载均衡）**：轮询或随机分发到多个 Sink
```properties
agent.sinkgroups.sg1.processor.type = load_balance
agent.sinkgroups.sg1.processor.selector = round_robin  # 或 random
```

## 10. 高频面试题

**Q：Flume 如何保证数据不丢失？**
- Source → Channel：使用事务，数据写入 Channel 成功后才从 Source 移除
- Channel → Sink：使用事务，Sink 确认写入成功后才从 Channel 删除数据
- 使用 FileChannel 或 KafkaChannel，Agent 宕机后数据不丢失
- TailDirSource 的 positionFile 记录读取位置，重启后断点续传

**Q：TailDirSource 和 ExecSource 的区别？**
- ExecSource：执行命令（如 `tail -F`），命令进程挂掉则数据丢失，不支持断点续传
- TailDirSource：Flume 自己管理文件读取，持久化 offset，支持断点续传，生产推荐

**Q：MemoryChannel 和 FileChannel 如何选择？**
- MemoryChannel：速度快，但 Agent 宕机数据丢失，适合允许少量丢失的场景
- FileChannel：速度慢，但可靠，适合生产环境不允许丢数据的场景
- KafkaChannel：兼顾高吞吐和高可靠，是更好的生产选择

**Q：Flume 采集数据到 HDFS，如何避免产生大量小文件？**
- 合理设置滚动策略：`rollInterval`（时间）、`rollSize`（大小）、`rollCount`（条数）
- 建议按小时滚动（`rollInterval=3600`），文件大小设为 128MB（与 HDFS Block 对齐）
- 将 `rollCount` 设为 0，禁用按条数滚动
