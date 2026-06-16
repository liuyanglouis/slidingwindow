import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 轻量级DAG调度器
 *
 * 设计特点：
 * 1. 支持拓扑排序执行DAG任务
 * 2. 无依赖的任务可并行执行
 * 3. 节点失败时自动取消下游节点
 * 4. 线程安全，支持并发任务提交
 *
 * 实现原理：
 * 1. 使用入度/出度图表示依赖关系
 * 2. 使用队列管理可执行任务
 * 3. 使用 CompletableFuture 管理任务状态和依赖
 * 4. 使用原子变量跟踪执行状态
 */
public class LightweightDAGScheduler {

    /**
     * 任务接口
     */
    @FunctionalInterface
    public interface Task {
        void execute() throws Exception;
    }

    /**
     * DAG节点信息
     */
    private static class NodeInfo {
        final String id;
        final Task task;
        final List<String> downstreamNodes; // 下游节点ID
        final CompletableFuture<Void> future;
        volatile boolean completed = false;
        volatile boolean failed = false;
        volatile Throwable failureCause;

        NodeInfo(String id, Task task) {
            this.id = id;
            this.task = task;
            this.downstreamNodes = new ArrayList<>();
            this.future = new CompletableFuture<>();
        }
    }

    /**
     * 执行DAG任务
     *
     * @param dag DAG定义，Map<节点ID, 依赖节点ID列表>
     * @param tasks 节点任务映射，Map<节点ID, 任务>
     * @param executor 任务执行器
     * @throws Exception 如果任何节点失败，抛出异常
     */
    public static void execute(
            Map<String, List<String>> dag,
            Map<String, Task> tasks,
            Executor executor) throws Exception {

        if (dag == null || tasks == null || executor == null) {
            throw new IllegalArgumentException("参数不能为空");
        }

        // 验证DAG定义和任务映射的一致性
        if (!dag.keySet().equals(tasks.keySet())) {
            throw new IllegalArgumentException("DAG节点和任务节点不一致");
        }

        // 构建节点信息
        Map<String, NodeInfo> nodes = new HashMap<>();
        Map<String, List<String>> dependencies = new HashMap<>();
        Map<String, AtomicInteger> indegree = new HashMap<>();

        // 初始化数据结构
        for (String nodeId : dag.keySet()) {
            Task task = tasks.get(nodeId);
            if (task == null) {
                throw new IllegalArgumentException("节点 " + nodeId + " 没有对应的任务");
            }
            nodes.put(nodeId, new NodeInfo(nodeId, task));
            dependencies.put(nodeId, dag.get(nodeId));
            indegree.put(nodeId, new AtomicInteger(0));
        }

        // 构建依赖关系和出度图
        for (Map.Entry<String, NodeInfo> entry : nodes.entrySet()) {
            String nodeId = entry.getKey();
            NodeInfo node = entry.getValue();

            // 处理当前节点的依赖
            List<String> deps = dependencies.get(nodeId);
            if (deps != null) {
                for (String depId : deps) {
                    NodeInfo depNode = nodes.get(depId);
                    if (depNode == null) {
                        throw new IllegalArgumentException("依赖节点 " + depId + " 不存在");
                    }
                    depNode.downstreamNodes.add(nodeId);
                    indegree.get(nodeId).incrementAndGet();
                }
            }
        }

        // 检查是否有循环依赖（拓扑排序）
        checkForCycles(nodes, indegree);

        // 执行调度
        executeDAG(nodes, indegree, executor);
    }

    /**
     * 检查DAG是否有循环依赖
     */
    private static void checkForCycles(
            Map<String, NodeInfo> nodes,
            Map<String, AtomicInteger> indegree) {

        Map<String, Integer> indegreeCopy = new HashMap<>();
        for (Map.Entry<String, AtomicInteger> entry : indegree.entrySet()) {
            indegreeCopy.put(entry.getKey(), entry.getValue().get());
        }

        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : indegreeCopy.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        int visitedCount = 0;
        while (!queue.isEmpty()) {
            String nodeId = queue.poll();
            visitedCount++;

            NodeInfo node = nodes.get(nodeId);
            for (String downstreamId : node.downstreamNodes) {
                int newIndegree = indegreeCopy.get(downstreamId) - 1;
                indegreeCopy.put(downstreamId, newIndegree);
                if (newIndegree == 0) {
                    queue.offer(downstreamId);
                }
            }
        }

        if (visitedCount != nodes.size()) {
            throw new IllegalArgumentException("DAG中存在循环依赖");
        }
    }

    /**
     * 执行DAG调度
     */
    private static void executeDAG(
            Map<String, NodeInfo> nodes,
            Map<String, AtomicInteger> indegree,
            Executor executor) throws Exception {

        // 用于同步的锁和条件
        ReentrantLock lock = new ReentrantLock();
        Condition allDone = lock.newCondition();

        // 跟踪执行状态
        AtomicInteger pendingNodes = new AtomicInteger(nodes.size());
        CompletableFuture<Void> overallFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> nodeFutures = new ArrayList<>();

        // 检查是否有节点已经失败
        AtomicBoolean hasFailure = new AtomicBoolean(false);
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

        // 查找初始可执行节点（入度为0）
        List<String> initialNodes = new ArrayList<>();
        for (Map.Entry<String, AtomicInteger> entry : indegree.entrySet()) {
            if (entry.getValue().get() == 0) {
                initialNodes.add(entry.getKey());
            }
        }

        // 如果没有初始节点，说明DAG无效
        if (initialNodes.isEmpty() && !nodes.isEmpty()) {
            throw new IllegalArgumentException("DAG中没有无依赖的起始节点");
        }

        // 执行初始节点
        for (String nodeId : initialNodes) {
            submitNode(nodeId, nodes, indegree, executor, pendingNodes,
                       overallFuture, hasFailure, failures, lock, allDone);
        }

        // 等待所有任务完成
        lock.lock();
        try {
            while (pendingNodes.get() > 0 && !hasFailure.get()) {
                allDone.await();
            }
        } finally {
            lock.unlock();
        }

        // 检查执行结果
        if (hasFailure.get()) {
            Throwable firstFailure = failures.isEmpty() ?
                new RuntimeException("任务执行失败") : failures.get(0);

            // 取消所有未完成的任务
            for (NodeInfo node : nodes.values()) {
                if (!node.completed && !node.failed) {
                    node.future.cancel(true);
                }
            }

            if (firstFailure instanceof Exception) {
                throw (Exception) firstFailure;
            } else if (firstFailure instanceof Error) {
                throw (Error) firstFailure;
            } else {
                throw new RuntimeException(firstFailure);
            }
        }
    }

    /**
     * 提交单个节点执行
     */
    private static void submitNode(
            String nodeId,
            Map<String, NodeInfo> nodes,
            Map<String, AtomicInteger> indegree,
            Executor executor,
            AtomicInteger pendingNodes,
            CompletableFuture<Void> overallFuture,
            AtomicBoolean hasFailure,
            List<Throwable> failures,
            ReentrantLock lock,
            Condition allDone) {

        NodeInfo node = nodes.get(nodeId);

        CompletableFuture.runAsync(() -> {
            try {
                // 检查是否已经被取消
                if (node.future.isCancelled()) {
                    return;
                }

                // 执行任务
                node.task.execute();
                node.completed = true;

                // 任务执行成功，触发下游节点
                triggerDownstreamNodes(nodeId, nodes, indegree, executor,
                                     pendingNodes, overallFuture, hasFailure,
                                     failures, lock, allDone);

            } catch (Throwable t) {
                // 任务执行失败
                node.failed = true;
                node.failureCause = t;
                handleNodeFailure(nodeId, nodes, indegree, hasFailure, failures,
                                lock, allDone);
            } finally {
                // 减少待处理节点计数
                int remaining = pendingNodes.decrementAndGet();
                node.future.complete(null);

                // 通知等待线程
                if (remaining == 0 || hasFailure.get()) {
                    lock.lock();
                    try {
                        allDone.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            }
        }, executor);
    }

    /**
     * 触发下游节点执行
     */
    private static void triggerDownstreamNodes(
            String completedNodeId,
            Map<String, NodeInfo> nodes,
            Map<String, AtomicInteger> indegree,
            Executor executor,
            AtomicInteger pendingNodes,
            CompletableFuture<Void> overallFuture,
            AtomicBoolean hasFailure,
            List<Throwable> failures,
            ReentrantLock lock,
            Condition allDone) {

        NodeInfo completedNode = nodes.get(completedNodeId);

        for (String downstreamId : completedNode.downstreamNodes) {
            AtomicInteger downstreamIndegree = indegree.get(downstreamId);

            // 减少下游节点的入度
            int newIndegree = downstreamIndegree.decrementAndGet();

            // 如果入度为0，可以执行该节点
            if (newIndegree == 0) {
                submitNode(downstreamId, nodes, indegree, executor,
                          pendingNodes, overallFuture, hasFailure,
                          failures, lock, allDone);
            }
        }
    }

    /**
     * 处理节点失败
     */
    private static void handleNodeFailure(
            String failedNodeId,
            Map<String, NodeInfo> nodes,
            Map<String, AtomicInteger> indegree,
            AtomicBoolean hasFailure,
            List<Throwable> failures,
            ReentrantLock lock,
            Condition allDone) {

        // 标记失败状态
        hasFailure.set(true);
        NodeInfo failedNode = nodes.get(failedNodeId);
        failures.add(failedNode.failureCause);

        // 取消所有下游节点
        cancelDownstreamNodes(failedNodeId, nodes, indegree);

        // 通知等待线程
        lock.lock();
        try {
            allDone.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 取消下游节点
     */
    private static void cancelDownstreamNodes(
            String nodeId,
            Map<String, NodeInfo> nodes,
            Map<String, AtomicInteger> indegree) {

        NodeInfo node = nodes.get(nodeId);
        if (node == null) return;

        // 使用队列进行广度优先搜索
        Queue<String> queue = new LinkedList<>();
        queue.offer(nodeId);

        while (!queue.isEmpty()) {
            String currentId = queue.poll();
            NodeInfo currentNode = nodes.get(currentId);

            // 取消当前节点的任务（如果未完成）
            if (!currentNode.completed && !currentNode.failed) {
                currentNode.future.cancel(true);
                currentNode.failed = true;
                currentNode.failureCause = new CancellationException(
                    "由于上游节点 " + nodeId + " 失败而被取消");
            }

            // 添加下游节点到队列
            for (String downstreamId : currentNode.downstreamNodes) {
                queue.offer(downstreamId);
            }
        }
    }

    /**
     * 简化的execute方法，适用于节点任务相同的情况
     */
    public static void execute(
            Map<String, List<String>> dag,
            Executor executor) throws Exception {

        // 创建默认任务（什么也不做）
        Map<String, Task> tasks = new HashMap<>();
        for (String nodeId : dag.keySet()) {
            tasks.put(nodeId, () -> {
                // 默认任务实现
                System.out.println("执行节点: " + nodeId);
            });
        }

        execute(dag, tasks, executor);
    }
}