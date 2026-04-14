---
title: MapReduce 核心组件系统性梳理
date: 2024-01-03
categories: [大数据]
tags: [MapReduce, 分布式计算]
permalink: /MapReduce核心组件系统性梳理/
---

## 1. MapReduce 是什么

MapReduce 是 Hadoop 的分布式计算框架，将复杂的并行计算抽象为两个阶段：**Map（映射）** 和 **Reduce（归约）**。开发者只需实现这两个函数的业务逻辑，框架自动处理任务分发、容错、数据传输等底层细节。

## 2. 核心思想

**分而治之**：将大数据集切分为若干小数据集，分配给多个 Map 任务并行处理，再将中间结果汇总给 Reduce 任务合并计算。

```
输入数据
  │
  ├── InputSplit 切片
  │
  ├── Map 阶段（并行）
  │     每个 MapTask 处理一个 InputSplit
  │     输出 <K, V> 键值对
  │
  ├── Shuffle 阶段（框架自动完成）
  │     分区 → 排序 → 合并 → 传输
  │
  └── Reduce 阶段（并行）
        每个 ReduceTask 处理一组相同 Key 的数据
        输出最终结果到 HDFS
```

## 3. 优缺点

**优点：**
- 编程模型简单，屏蔽分布式细节
- 高容错：任务失败自动重试
- 适合离线批处理大规模数据
- 良好的扩展性

**缺点：**
- 延迟高，不适合实时计算
- 中间结果落磁盘，IO 开销大
- 不适合迭代计算（机器学习场景）
- 不适合流式计算

## 4. 核心进程

- **MRAppMaster**：每个 MR 作业对应一个，负责作业的任务调度和容错
- **MapTask**：执行 Map 阶段逻辑
- **ReduceTask**：执行 Reduce 阶段逻辑

## 5. 编程规范

### 三大组件

```java
// Mapper：处理输入数据，输出中间 <K, V>
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // key: 行偏移量, value: 行内容
        String[] words = value.toString().split(" ");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}

// Reducer：对相同 Key 的 Value 列表进行汇总
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

// Driver：组装作业配置并提交
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
```

## 6. Hadoop 序列化

Java 原生序列化（Serializable）太重，Hadoop 实现了自己的序列化机制 **Writable**，更紧凑、更快速。

| Java 类型 | Hadoop Writable 类型 |
|-----------|----------------------|
| String    | Text                 |
| int       | IntWritable          |
| long      | LongWritable         |
| float     | FloatWritable        |
| double    | DoubleWritable       |
| boolean   | BooleanWritable      |
| byte[]    | BytesWritable        |

**自定义序列化类**需实现 `Writable` 接口，重写 `write()` 和 `readFields()` 方法，且字段读写顺序必须一致。

## 7. InputFormat 与 InputSplit

### InputSplit vs Block
- **Block**：HDFS 物理存储单元（默认 128MB）
- **InputSplit**：MapReduce 逻辑切片，一个 InputSplit 对应一个 MapTask
- 默认情况下 InputSplit 大小 = Block 大小，但可以通过参数调整

### 常用 InputFormat

| InputFormat | 说明 |
|-------------|------|
| TextInputFormat | 默认，按行读取，Key 为行偏移量，Value 为行内容 |
| KeyValueTextInputFormat | 按分隔符将每行分为 Key 和 Value |
| NLineInputFormat | 按固定行数切片，每个 MapTask 处理 N 行 |
| DBInputFormat | 从关系型数据库读取数据 |

## 8. Shuffle 全流程（核心！）

Shuffle 是 MapReduce 性能的关键，发生在 Map 输出到 Reduce 输入之间。

```
Map 输出
  │
  ├── 写入环形缓冲区（默认 100MB）
  │
  ├── 缓冲区达到 80% 时溢写（spill）到磁盘
  │     ├── 按 Partitioner 分区
  │     ├── 区内按 Key 排序（快速排序）
  │     └── 可选：Combiner 本地预聚合
  │
  ├── 多个溢写文件归并排序（merge）为一个有序文件
  │
  ├── ReduceTask 通过 HTTP 拉取对应分区数据（copy）
  │
  ├── 内存 + 磁盘归并排序
  │
  └── 输入 Reduce 函数
```

### Combiner（本地预聚合）
- 在 MapTask 本地执行一次 Reduce 逻辑，减少网络传输数据量
- 前提：Combiner 的使用不能影响最终结果（如求和可用，求平均不可直接用）
- 本质是一个运行在 Map 端的 Reducer

## 9. Partitioner（分区）

- 决定 Map 输出的 KV 被哪个 ReduceTask 处理
- 默认：`HashPartitioner`，`key.hashCode() % numReduceTasks`
- 自定义分区：继承 `Partitioner`，重写 `getPartition()` 方法
- **分区数 = ReduceTask 数**，两者必须匹配

## 10. 排序

MapReduce 中排序无处不在：
- **Map 端**：溢写时对每个分区内的数据按 Key 排序
- **Reduce 端**：拉取数据后归并排序

**全局排序**：设置 ReduceTask 数量为 1（性能差，慎用）
**分区排序**：多个 ReduceTask，每个分区内有序（TotalOrderPartitioner）

## 11. MapTask 工作原理

```
Read → Map → Collect → Spill → Merge
```
1. Read：通过 InputFormat 读取数据，生成 <K, V>
2. Map：调用用户 map() 方法处理
3. Collect：将输出写入环形缓冲区，同时进行分区
4. Spill：缓冲区满 80% 时溢写磁盘，排序 + Combiner
5. Merge：所有溢写文件归并为一个有序文件

## 12. ReduceTask 工作原理

```
Copy → Merge → Sort → Reduce
```
1. Copy：从各 MapTask 拉取对应分区数据
2. Merge：边拉取边归并，防止内存溢出
3. Sort：最终归并排序，保证全局有序
4. Reduce：调用用户 reduce() 方法，输出结果

## 13. 经典场景题

### ReduceJoin vs MapJoin

| 对比项 | ReduceJoin | MapJoin |
|--------|------------|---------|
| 原理 | 在 Reduce 端完成 Join | 在 Map 端完成 Join |
| 适用场景 | 两张大表 Join | 一大一小表 Join |
| 缺点 | Shuffle 数据量大，易数据倾斜 | 小表需全量加载到内存 |
| 实现方式 | 标记数据来源，Reduce 端合并 | DistributedCache 分发小表 |

### GroupBy 实现
- Map 端输出 <分组字段, 其他字段>
- Reduce 端对相同 Key 的 Value 列表进行聚合

### Distinct 实现
- Map 端输出 <去重字段, NullWritable>
- Reduce 端每个 Key 只输出一次

## 14. 高频面试题

**Q：MapReduce 的 Shuffle 过程详细描述？**
见第 8 节，重点掌握：环形缓冲区 → 溢写 → 归并 → 拉取 → 归并 → Reduce。

**Q：Combiner 和 Reducer 的区别？**
- Combiner 运行在 Map 端，是本地的 Reducer，目的是减少网络传输
- Reducer 运行在 Reduce 端，处理全局数据，输出最终结果
- Combiner 不能改变最终结果的语义

**Q：MapReduce 如何实现全局排序？**
- 方案一：ReduceTask 设为 1，但并行度为 0，性能极差
- 方案二：使用 TotalOrderPartitioner，先采样确定分区边界，保证各分区间有序

**Q：数据倾斜如何解决？**
- 加随机前缀打散热点 Key，两阶段聚合
- 使用 MapJoin 避免 Reduce 端 Join
- 自定义 Partitioner 均匀分配数据
