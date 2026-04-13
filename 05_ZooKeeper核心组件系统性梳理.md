# ZooKeeper 核心组件系统性梳理

## 1. ZooKeeper 是什么

ZooKeeper 是 Apache 开源的分布式协调服务，为分布式应用提供一致性服务，包括：配置管理、命名服务、分布式锁、集群选主等。它基于 ZAB（ZooKeeper Atomic Broadcast）协议保证数据一致性。

## 2. 企业使用场景

- **Hadoop HA**：NameNode/ResourceManager 主备切换，依赖 ZK 选主
- **HBase**：RegionServer 注册、Master 选主
- **Kafka**：Broker 注册、Controller 选主、消费者 offset 管理（旧版）
- **分布式锁**：利用临时顺序节点实现公平锁
- **配置中心**：统一管理分布式系统配置，变更实时推送
- **服务注册与发现**：微服务注册中心（Dubbo 默认使用 ZK）

## 3. 角色及选举机制

### 三种角色

| 角色 | 职责 |
|------|------|
| Leader | 集群唯一，处理所有写请求，发起投票 |
| Follower | 处理读请求，参与 Leader 选举投票，将写请求转发给 Leader |
| Observer | 只处理读请求，不参与选举，用于扩展读性能 |

### Leader 选举机制（FastLeaderElection）

**选举触发时机**：集群启动 或 Leader 宕机

**选举规则**（优先级从高到低）：
1. **epoch（选举轮次）大的优先**：轮次越大说明越新
2. **ZXID（事务 ID）大的优先**：ZXID 越大说明数据越新
3. **myid（服务器 ID）大的优先**：配置时手动指定

**选举过程**：
```
1. 每个节点投票给自己（myid, zxid）
2. 广播投票给其他节点
3. 收到其他节点投票后，按规则 PK，更新自己的投票
4. 统计票数，超过半数（n/2 + 1）的节点当选 Leader
5. 其余节点成为 Follower
```

> 这也是为什么 ZK 集群节点数必须是奇数（3、5、7）：偶数节点不能提高容错能力，反而浪费资源。

## 4. 数据模型（ZNode）

ZooKeeper 的数据结构类似文件系统，是一棵树形结构，每个节点称为 **ZNode**。

```
/
├── /hadoop
│     ├── /hadoop/namenode
│     └── /hadoop/datanode
├── /kafka
│     └── /kafka/brokers
└── /dubbo
```

### ZNode 类型

| 类型 | 说明 |
|------|------|
| 持久节点（PERSISTENT） | 默认类型，客户端断开后节点依然存在 |
| 持久顺序节点（PERSISTENT_SEQUENTIAL） | 节点名自动追加递增序号 |
| 临时节点（EPHEMERAL） | 客户端断开后节点自动删除，不能有子节点 |
| 临时顺序节点（EPHEMERAL_SEQUENTIAL） | 临时 + 顺序，实现分布式锁的关键 |

### ZNode 存储内容
- **data**：节点存储的数据（最大 1MB）
- **stat**：节点状态信息（czxid、mzxid、version、dataLength 等）

## 5. 监听器（Watcher）

Watcher 是 ZooKeeper 的核心特性，实现了发布/订阅模式。

**工作原理：**
```
1. Client 在读取节点数据时注册 Watcher
2. 当节点数据变化（create/update/delete）时，ZK Server 向 Client 发送通知
3. Client 收到通知后，重新读取数据并再次注册 Watcher（Watcher 是一次性的）
```

**监听类型：**
- `getData(path, watcher)`：监听节点数据变化
- `getChildren(path, watcher)`：监听子节点列表变化
- `exists(path, watcher)`：监听节点创建/删除

**注意**：Watcher 是一次性的，触发后需重新注册。

## 6. 命令行操作

```bash
# 连接 ZK
zkCli.sh -server localhost:2181

# 创建节点
create /node "data"                    # 持久节点
create -e /node "data"                 # 临时节点
create -s /node "data"                 # 持久顺序节点
create -e -s /node "data"              # 临时顺序节点

# 读取节点
get /node                              # 获取数据
get -s /node                           # 获取数据 + stat 信息
ls /node                               # 列出子节点
ls -s /node                            # 列出子节点 + stat 信息

# 修改节点
set /node "new_data"

# 删除节点
delete /node                           # 删除（有子节点时失败）
deleteall /node                        # 递归删除

# 监听
get -w /node                           # 监听数据变化
ls -w /node                            # 监听子节点变化

# 四字命令（需开启白名单）
echo stat | nc localhost 2181          # 查看服务状态
echo ruok | nc localhost 2181          # 健康检查（返回 imok）
echo mntr | nc localhost 2181          # 监控指标
```

## 7. 集群核心概念

### ZAB 协议（ZooKeeper Atomic Broadcast）
ZK 的核心一致性协议，分两种模式：
- **崩溃恢复模式**：Leader 宕机时，选举新 Leader，同步数据
- **消息广播模式**：正常工作时，Leader 将写请求广播给所有 Follower，超过半数 ACK 后提交

### ZXID（事务 ID）
- 64 位整数，高 32 位为 epoch（选举轮次），低 32 位为事务序号
- 每次写操作 ZXID 递增，保证全局有序

### 数据一致性保证
- **顺序一致性**：同一客户端的操作按发送顺序执行
- **原子性**：写操作要么全部成功，要么全部失败
- **单一视图**：客户端无论连接哪个节点，看到的数据一致
- **可靠性**：一旦变更被应用，持久有效

## 8. 集群部署

```
# zoo.cfg 配置
tickTime=2000          # 基本时间单位（ms）
initLimit=10           # Follower 初始化连接 Leader 的超时（10 * tickTime）
syncLimit=5            # Follower 与 Leader 同步的超时（5 * tickTime）
dataDir=/opt/zookeeper/data
clientPort=2181

# 集群节点配置（server.id=host:通信端口:选举端口）
server.1=node1:2888:3888
server.2=node2:2888:3888
server.3=node3:2888:3888
```

每个节点的 `dataDir` 下需创建 `myid` 文件，内容为该节点的 id（1、2、3）。

## 9. 高频面试题

**Q：ZooKeeper 如何保证数据一致性？**
- 基于 ZAB 协议，写操作必须经过 Leader，Leader 广播给 Follower，超过半数确认后才提交
- 读操作可以在任意节点执行，但可能读到稍旧的数据（最终一致性）
- 如需强一致读，可在读前调用 `sync()` 同步最新数据

**Q：ZooKeeper 集群为什么是奇数节点？**
- 容错能力：n 个节点最多容忍 (n-1)/2 个节点故障
- 4 个节点和 3 个节点的容错能力相同（都只能容忍 1 个故障），但 4 个节点浪费资源
- 奇数节点在容错能力相同的情况下，节点数更少

**Q：临时节点的应用场景？**
- 服务注册：服务启动时创建临时节点，宕机后节点自动删除，其他服务通过 Watcher 感知
- 分布式锁：抢占临时节点，持有锁的客户端断开后锁自动释放，防止死锁

**Q：ZooKeeper 的 Watcher 机制有什么缺点？**
- Watcher 是一次性的，每次触发后需重新注册，存在事件丢失的可能
- 通知只告知变化发生，不携带变化内容，需客户端主动拉取
- 大量 Watcher 注册会增加 ZK Server 的内存压力
