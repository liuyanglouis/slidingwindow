# SlidingWindow大数据组件库

这是一个包含四个大数据核心组件的Java库，实现了常见的分布式系统模式。

## 📦 项目概述

本库提供了四个关键的大数据组件实现：

1. **滑动窗口限流器** - 高并发流量控制
2. **轻量级DAG调度器** - 任务依赖管理和并行执行
3. **类Flink Checkpoint系统** - Exactly-Once语义的故障恢复
4. **分布式ID生成器** - Snowflake算法的优化实现

## 🏗️ 项目结构

```
slidingwindow/
├── pom.xml                    # Maven配置文件
├── README.md                  # 项目说明文档
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── liuyanglouis/
│   │               └── slidingwindow/
│   │                   ├── ratelimiter/          # 滑动窗口限流器
│   │                   │   └── SlidingWindowRateLimiter.java
│   │                   ├── scheduler/            # DAG调度器
│   │                   │   └── LightweightDAGScheduler.java
│   │                   ├── checkpoint/           # Checkpoint系统
│   │                   │   └── CompleteCheckpointSystem.java
│   │                   └── idgenerator/          # 分布式ID生成器
│   │                       └── OptimizedDistributedIdGenerator.java
│   └── test/
│       └── java/
│           └── com/
│               └── liuyanglouis/
│                   └── slidingwindow/
│                       ├── ratelimiter/
│                       │   └── SlidingWindowRateLimiterTest.java
│                       └── scheduler/
│                           └── LightweightDAGSchedulerTest.java
```

## 📚 组件详情

### 1. 滑动窗口限流器 (`SlidingWindowRateLimiter`)

**功能**：基于滑动窗口的流量控制，支持精确的请求限流。

**特点**：
- 线程安全：使用 `ReentrantLock` 保护临界区
- 精确滑动窗口：存储每个请求的时间戳
- 无短暂无限放行：QPS突增时不会出现短暂的无限放行窗口
- 内存高效：自动清理过期请求，队列不会无限增长

**使用示例**：
```java
SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(1000, 10); // 1秒窗口，最多10个请求
if (limiter.tryAcquire()) {
    // 允许请求，执行操作
} else {
    // 请求被限制
}
```

### 2. 轻量级DAG调度器 (`LightweightDAGScheduler`)

**功能**：基于DAG（有向无环图）的任务调度和执行。

**特点**：
- 支持拓扑排序执行DAG任务
- 无依赖的任务可并行执行
- 节点失败时自动取消下游节点
- 循环依赖检测
- 线程安全，支持并发任务提交

**使用示例**：
```java
Map<String, List<String>> dag = new HashMap<>();
dag.put("A", Arrays.asList());
dag.put("B", Arrays.asList("A"));
dag.put("C", Arrays.asList("B"));

LightweightDAGScheduler scheduler = new LightweightDAGScheduler();
scheduler.execute(dag, executor);
```

### 3. 类Flink Checkpoint系统 (`CompleteCheckpointSystem`)

**功能**：支持Exactly-Once语义的流处理故障恢复系统。

**特点**：
- Barrier对齐机制：算子等待所有输入的barrier到达后才进行快照
- 数据缓冲：对齐期间缓冲barrier之后的数据
- 状态快照：算子状态的一致性快照
- 超时回滚：算子快照超时导致整个checkpoint失败
- 故障恢复：从最近的完整checkpoint恢复状态

**核心组件**：
- `CheckpointCoordinator`：管理barrier对齐和checkpoint生命周期
- `CheckpointableOperator`：支持状态快照和恢复的算子接口

### 4. 分布式ID生成器 (`OptimizedDistributedIdGenerator`)

**功能**：基于Snowflake算法的64位分布式ID生成器。

**特点**：
- 64位ID结构（时间戳41位 + 数据中心ID5位 + 机器ID5位 + 序列号12位）
- 降低锁竞争：使用CAS操作替代synchronized
- 时钟回拨检测：发现时钟回拨时抛出异常
- 序列号预取：提前预取多个序列号减少竞争
- 时间戳缓存：减少系统调用

**使用示例**：
```java
OptimizedDistributedIdGenerator generator = new OptimizedDistributedIdGenerator(1, 1); // datacenterId=1, workerId=1
long id = generator.nextId(); // 生成64位ID
```

## 🚀 快速开始

### 1. 克隆项目
```bash
git clone https://github.com/liuyanglouis/slidingwindow.git
cd slidingwindow
```

### 2. 编译项目
```bash
mvn clean compile
```

### 3. 运行测试
```bash
mvn test
```

### 4. 打包
```bash
mvn package
```

## 🔧 构建要求

- Java 11+
- Maven 3.6+

## 📊 性能特点

### 滑动窗口限流器
- 时间复杂度：`tryAcquire()` 平均 O(1)，最坏 O(n)
- 空间复杂度：O(n)，n为窗口内最大请求数

### DAG调度器
- 支持高并发任务提交
- 自动任务依赖解析

### Checkpoint系统
- 支持Exactly-Once语义
- 可配置的超时和重试机制

### ID生成器
- 高性能ID生成（支持高QPS）
- 低锁竞争设计

## 🔍 测试

项目包含两个组件的完整测试：
1. `SlidingWindowRateLimiterTest`：测试滑动窗口限流器的基本功能、多线程并发和QPS突增
2. `LightweightDAGSchedulerTest`：测试DAG调度器的拓扑排序、并行执行、失败处理和循环依赖检测

## 📝 许可证

本项目代码可自由使用和修改。

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 📧 联系

如有问题，请通过GitHub Issues联系。