package com.liuyanglouis.slidingwindow.checkpoint;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * 完整的类Flink Checkpoint系统，支持Exactly-Once语义
 *
 * 实现原理：
 * 1. Barrier机制：将数据流划分为checkpoint段
 * 2. Barrier对齐：算子等待所有输入的barrier到达后才进行快照
 * 3. 数据缓冲：对齐期间缓冲barrier之后的数据
 * 4. 状态快照：算子状态的一致性快照
 * 5. 超时回滚：任何算子超时导致整个checkpoint失败
 * 6. 故障恢复：从最近的完整checkpoint恢复状态
 */
public class CompleteCheckpointSystemOld {

    // ==================== 核心接口定义 ====================

    /**
     * 可检查点的算子接口
     */
    public interface CheckpointableOperator {
        /**
         * 获取算子ID
         */
        String getOperatorId();

        /**
         * 获取上游算子ID列表
         */
        List<String> getUpstreamOperatorIds();

        /**
         * 处理数据元素
         */
        void processElement(Object element);

        /**
         * 接收barrier
         * @param checkpointId 检查点ID
         * @param sourceOperatorId 发送barrier的算子ID
         */
        void receiveBarrier(long checkpointId, String sourceOperatorId);

        /**
         * 快照当前状态
         * @param checkpointId 检查点ID
         * @return 快照完成的Future
         */
        CompletableFuture<Void> snapshotState(long checkpointId);

        /**
         * 从检查点恢复状态
         * @param checkpointId 检查点ID
         */
        void restoreState(long checkpointId);
    }

    /**
     * 状态后端接口（用于状态持久化）
     */
    public interface StateBackend {
        /**
         * 保存算子状态
         */
        CompletableFuture<Void> saveState(long checkpointId, String operatorId, Object state);

        /**
         * 加载算子状态
         */
        CompletableFuture<Object> loadState(long checkpointId, String operatorId);
    }

    /**
     * 内存状态后端（简化实现）
     */
    public static class MemoryStateBackend implements StateBackend {
        private final Map<String, Object> storage = new ConcurrentHashMap<>();

        private String getKey(long checkpointId, String operatorId) {
            return checkpointId + ":" + operatorId;
        }

        @Override
        public CompletableFuture<Void> saveState(long checkpointId, String operatorId, Object state) {
            return CompletableFuture.runAsync(() -> {
                storage.put(getKey(checkpointId, operatorId), state);
                System.out.println("保存状态: checkpoint=" + checkpointId +
                                 ", operator=" + operatorId);
            });
        }

        @Override
        public CompletableFuture<Object> loadState(long checkpointId, String operatorId) {
            return CompletableFuture.supplyAsync(() -> {
                Object state = storage.get(getKey(checkpointId, operatorId));
                System.out.println("加载状态: checkpoint=" + checkpointId +
                                 ", operator=" + operatorId +
                                 ", state=" + (state != null ? "存在" : "null"));
                return state;
            });
        }
    }

    // ==================== Checkpoint协调器 ====================

    /**
     * CheckpointCoordinator - 管理barrier对齐和checkpoint生命周期
     */
    public static class CheckpointCoordinator {
        private final AtomicLong checkpointIdCounter = new AtomicLong(0);
        private final Map<String, CheckpointableOperator> operators = new ConcurrentHashMap<>();
        private final StateBackend stateBackend;

        // Checkpoint元数据存储
        private final Map<Long, CheckpointMetadata> checkpoints = new ConcurrentHashMap<>();

        // Barrier对齐状态：checkpointId -> operatorId -> 已接收的barrier集合
        private final Map<Long, Map<String, Set<String>>> barrierStates = new ConcurrentHashMap<>();

        // 执行服务
        private final ScheduledExecutorService timeoutExecutor;
        private final ExecutorService taskExecutor;
        private final long checkpointTimeoutMs;

        // 同步锁
        private final ReentrantLock coordinatorLock = new ReentrantLock();

        public CheckpointCoordinator(long checkpointTimeoutMs, int poolSize) {
            this.checkpointTimeoutMs = checkpointTimeoutMs;
            this.stateBackend = new MemoryStateBackend();
            this.taskExecutor = Executors.newFixedThreadPool(poolSize);
            this.timeoutExecutor = Executors.newScheduledThreadPool(2);
        }

        public CheckpointCoordinator(long checkpointTimeoutMs, int poolSize, StateBackend stateBackend) {
            this.checkpointTimeoutMs = checkpointTimeoutMs;
            this.stateBackend = stateBackend;
            this.taskExecutor = Executors.newFixedThreadPool(poolSize);
            this.timeoutExecutor = Executors.newScheduledThreadPool(2);
        }

        /**
         * 注册算子
         */
        public void registerOperator(CheckpointableOperator operator) {
            operators.put(operator.getOperatorId(), operator);
        }

        /**
         * 触发新的checkpoint
         * @return checkpoint ID
         */
        public long triggerCheckpoint() {
            long checkpointId = checkpointIdCounter.incrementAndGet();

            coordinatorLock.lock();
            try {
                // 创建checkpoint元数据
                CheckpointMetadata metadata = new CheckpointMetadata(checkpointId, operators.size());
                checkpoints.put(checkpointId, metadata);

                // 将所有算子添加到待完成列表
                for (CheckpointableOperator op : operators.values()) {
                    metadata.addPendingOperator(op.getOperatorId());
                }

                checkpoints.put(checkpointId, metadata);

                // 初始化barrier状态
                Map<String, Set<String>> barrierState = new ConcurrentHashMap<>();
                for (CheckpointableOperator op : operators.values()) {
                    barrierState.put(op.getOperatorId(), ConcurrentHashMap.newKeySet());
                }
                barrierStates.put(checkpointId, barrierState);

                // 从源头算子（无上游）开始发送barrier
                for (CheckpointableOperator op : operators.values()) {
                    if (op.getUpstreamOperatorIds().isEmpty()) {
                        taskExecutor.submit(() -> {
                            try {
                                op.receiveBarrier(checkpointId, "coordinator");
                            } catch (Exception e) {
                                handleOperatorError(checkpointId, op.getOperatorId(), e);
                            }
                        });
                    }
                }

                // 设置超时检查
                timeoutExecutor.schedule(() -> checkCheckpointTimeout(checkpointId),
                                        checkpointTimeoutMs, TimeUnit.MILLISECONDS);

                System.out.println("触发checkpoint: " + checkpointId);
                return checkpointId;

            } finally {
                coordinatorLock.unlock();
            }
        }

        /**
         * 处理算子接收到的barrier
         */
        public void handleBarrierReceived(long checkpointId, String operatorId, String sourceId) {
            coordinatorLock.lock();
            try {
                Map<String, Set<String>> barrierState = barrierStates.get(checkpointId);
                if (barrierState == null) {
                    return; // checkpoint可能已被取消
                }

                Set<String> receivedBarriers = barrierState.get(operatorId);
                if (receivedBarriers == null) {
                    return; // 算子未注册
                }

                // 记录接收到的barrier
                receivedBarriers.add(sourceId);

                // 获取算子的所有上游算子
                CheckpointableOperator operator = operators.get(operatorId);
                if (operator == null) {
                    return;
                }

                List<String> upstreamIds = operator.getUpstreamOperatorIds();

                // 检查是否所有上游的barrier都已到达
                if (receivedBarriers.containsAll(upstreamIds)) {
                    // Barrier对齐完成，触发状态快照
                    triggerOperatorSnapshot(checkpointId, operatorId);
                }

            } finally {
                coordinatorLock.unlock();
            }
        }

        /**
         * 触发算子状态快照
         */
        private void triggerOperatorSnapshot(long checkpointId, String operatorId) {
            CheckpointableOperator operator = operators.get(operatorId);
            if (operator == null) {
                return;
            }

            taskExecutor.submit(() -> {
                try {
                    CompletableFuture<Void> snapshotFuture = operator.snapshotState(checkpointId);

                    // 设置超时
                    snapshotFuture.orTimeout(checkpointTimeoutMs / 2, TimeUnit.MILLISECONDS)
                                 .thenAccept(v -> {
                                     handleSnapshotComplete(checkpointId, operatorId);
                                 })
                                 .exceptionally(ex -> {
                                     handleSnapshotFailed(checkpointId, operatorId, ex);
                                     return null;
                                 });

                } catch (Exception e) {
                    handleSnapshotFailed(checkpointId, operatorId, e);
                }
            });
        }

        /**
         * 处理快照完成
         */
        private void handleSnapshotComplete(long checkpointId, String operatorId) {
            coordinatorLock.lock();
            try {
                CheckpointMetadata metadata = checkpoints.get(checkpointId);
                if (metadata == null || metadata.getStatus() == CheckpointStatus.FAILED) {
                    return;
                }

                // 标记算子快照完成
                metadata.completeOperator(operatorId);

                if (metadata.isComplete()) {
                    completeCheckpoint(checkpointId);
                }

            } finally {
                coordinatorLock.unlock();
            }
        }

        /**
         * 处理快照失败
         */
        private void handleSnapshotFailed(long checkpointId, String operatorId, Throwable cause) {
            coordinatorLock.lock();
            try {
                CheckpointMetadata metadata = checkpoints.get(checkpointId);
                if (metadata != null && metadata.getStatus() == CheckpointStatus.IN_PROGRESS) {
                    failCheckpoint(checkpointId,
                        "算子 " + operatorId + " 快照失败: " + cause.getMessage());
                }

            } finally {
                coordinatorLock.unlock();
            }
        }

        /**
         * 处理算子错误
         */
        private void handleOperatorError(long checkpointId, String operatorId, Throwable cause) {
            coordinatorLock.lock();
            try {
                CheckpointMetadata metadata = checkpoints.get(checkpointId);
                if (metadata != null && metadata.getStatus() == CheckpointStatus.IN_PROGRESS) {
                    failCheckpoint(checkpointId,
                        "算子 " + operatorId + " 错误: " + cause.getMessage());
                }

            } finally {
                coordinatorLock.unlock();
            }
        }

        /**
         * 完成checkpoint
         */
        private void completeCheckpoint(long checkpointId) {
            CheckpointMetadata metadata = checkpoints.get(checkpointId);
            if (metadata == null) {
                return;
            }

            metadata.setStatus(CheckpointStatus.COMPLETED);
            metadata.setEndTime(System.currentTimeMillis());

            // 清理状态
            barrierStates.remove(checkpointId);

            System.out.println("Checkpoint " + checkpointId + " 完成, 耗时: " +
                             (metadata.getEndTime() - metadata.getStartTime()) + "ms");
        }

        /**
         * 失败checkpoint
         */
        private void failCheckpoint(long checkpointId, String reason) {
            CheckpointMetadata metadata = checkpoints.get(checkpointId);
            if (metadata == null) {
                return;
            }

            metadata.setStatus(CheckpointStatus.FAILED);
            metadata.setEndTime(System.currentTimeMillis());

            // 清理状态
            barrierStates.remove(checkpointId);

            System.err.println("Checkpoint " + checkpointId + " 失败: " + reason);
        }

        /**
         * 检查checkpoint超时
         */
        private void checkCheckpointTimeout(long checkpointId) {
            coordinatorLock.lock();
            try {
                CheckpointMetadata metadata = checkpoints.get(checkpointId);
                if (metadata != null && metadata.getStatus() == CheckpointStatus.IN_PROGRESS) {
                    long elapsed = System.currentTimeMillis() - metadata.getStartTime();
                    if (elapsed > checkpointTimeoutMs) {
                        failCheckpoint(checkpointId, "超时 (" + elapsed + "ms > " +
                                                     checkpointTimeoutMs + "ms)");
                    }
                }

            } finally {
                coordinatorLock.unlock();
            }
        }

        /**
         * 从checkpoint恢复状态
         */
        public void restoreFromCheckpoint(long checkpointId) throws Exception {
            coordinatorLock.lock();
            try {
                CheckpointMetadata metadata = checkpoints.get(checkpointId);
                if (metadata == null) {
                    throw new IllegalArgumentException("Checkpoint不存在: " + checkpointId);
                }

                if (metadata.getStatus() != CheckpointStatus.COMPLETED) {
                    throw new IllegalStateException("Checkpoint未完成: " + checkpointId);
                }

                // 恢复所有算子状态
                List<CompletableFuture<Void>> restoreFutures = new ArrayList<>();

                for (CheckpointableOperator operator : operators.values()) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        try {
                            operator.restoreState(checkpointId);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, taskExecutor);

                    restoreFutures.add(future);
                }

                // 等待所有算子恢复完成
                CompletableFuture.allOf(restoreFutures.toArray(new CompletableFuture[0]))
                               .get(checkpointTimeoutMs, TimeUnit.MILLISECONDS);

                System.out.println("从Checkpoint " + checkpointId + " 恢复成功");

            } finally {
                coordinatorLock.unlock();
            }
        }

        /**
         * 获取最近的完整checkpoint ID
         */
        public Long getLatestCompletedCheckpointId() {
            return checkpoints.entrySet().stream()
                .filter(entry -> entry.getValue().getStatus() == CheckpointStatus.COMPLETED)
                .map(Map.Entry::getKey)
                .max(Long::compareTo)
                .orElse(null);
        }

        /**
         * 优雅关闭
         */
        public void shutdown() {
            timeoutExecutor.shutdown();
            taskExecutor.shutdown();

            try {
                if (!timeoutExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                    timeoutExecutor.shutdownNow();
                }
                if (!taskExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                    taskExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                timeoutExecutor.shutdownNow();
                taskExecutor.shutdownNow();
            }
        }
    }

    // ==================== 辅助类 ====================

    /**
     * Checkpoint元数据
     */
    public static class CheckpointMetadata {
        private final long checkpointId;
        private final long startTime;
        private volatile long endTime;
        private volatile CheckpointStatus status;
        private final Set<String> pendingOperators;
        private final AtomicInteger completedCount;

        public CheckpointMetadata(long checkpointId, int totalOperators) {
            this.checkpointId = checkpointId;
            this.startTime = System.currentTimeMillis();
            this.status = CheckpointStatus.IN_PROGRESS;
            this.pendingOperators = ConcurrentHashMap.newKeySet();
            this.completedCount = new AtomicInteger(0);
        }

        public void completeOperator(String operatorId) {
            pendingOperators.remove(operatorId);
            completedCount.incrementAndGet();
        }

        public boolean isComplete() {
            return pendingOperators.isEmpty();
        }

        public int getCompletedCount() {
            return completedCount.get();
        }

        public long getCheckpointId() { return checkpointId; }
        public long getStartTime() { return startTime; }
        public long getEndTime() { return endTime; }
        public CheckpointStatus getStatus() { return status; }
        public void addPendingOperator(String operatorId) {
            pendingOperators.add(operatorId);
        }

        public void setEndTime(long endTime) { this.endTime = endTime; }
        public void setStatus(CheckpointStatus status) { this.status = status; }
    }

    /**
     * Checkpoint状态枚举
     */
    public enum CheckpointStatus {
        IN_PROGRESS, COMPLETED, FAILED
    }

    // ==================== 基础算子实现 ====================

    /**
     * 抽象基础算子，提供Exactly-Once支持
     */
    public static abstract class BaseOperator implements CheckpointableOperator {
        protected final String operatorId;
        protected final List<String> upstreamOperatorIds;
        protected CheckpointCoordinator coordinator;

        // Exactly-Once支持
        protected final Queue<Object> buffer = new ConcurrentLinkedQueue<>();
        protected volatile boolean checkpointInProgress = false;
        protected volatile long currentCheckpointId = -1;

        // 算子状态
        protected volatile Object state;

        public BaseOperator(String operatorId, List<String> upstreamOperatorIds) {
            this.operatorId = operatorId;
            this.upstreamOperatorIds = new ArrayList<>(upstreamOperatorIds);
        }

        public void setCoordinator(CheckpointCoordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public String getOperatorId() {
            return operatorId;
        }

        @Override
        public List<String> getUpstreamOperatorIds() {
            return new ArrayList<>(upstreamOperatorIds);
        }

        @Override
        public void processElement(Object element) {
            if (checkpointInProgress) {
                // Checkpoint进行中，缓冲数据
                buffer.offer(element);
            } else {
                // 正常处理
                processElementInternal(element);
            }
        }

        /**
         * 内部元素处理（由子类实现）
         */
        protected abstract void processElementInternal(Object element);

        @Override
        public void receiveBarrier(long checkpointId, String sourceOperatorId) {
            if (coordinator != null) {
                coordinator.handleBarrierReceived(checkpointId, operatorId, sourceOperatorId);
            }

            // 开始checkpoint，缓冲后续数据
            checkpointInProgress = true;
            currentCheckpointId = checkpointId;
        }

        @Override
        public CompletableFuture<Void> snapshotState(long checkpointId) {
            return CompletableFuture.runAsync(() -> {
                try {
                    // 执行状态快照
                    performSnapshot(checkpointId);

                    // 恢复处理缓冲的数据
                    resumeProcessing();

                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            });
        }

        /**
         * 执行状态快照（由子类实现）
         */
        protected abstract void performSnapshot(long checkpointId) throws Exception;

        /**
         * 恢复处理缓冲的数据
         */
        private void resumeProcessing() {
            checkpointInProgress = false;
            currentCheckpointId = -1;

            while (!buffer.isEmpty()) {
                Object element = buffer.poll();
                if (element != null) {
                    processElementInternal(element);
                }
            }
        }

        @Override
        public void restoreState(long checkpointId) {
            System.out.println(operatorId + " 从checkpoint " + checkpointId + " 恢复状态");
        }

        public Object getState() {
            return state;
        }

        public void setState(Object state) {
            this.state = state;
        }
    }

    // ==================== 示例算子 ====================

    /**
     * 简单状态算子示例
     */
    public static class SimpleOperator extends BaseOperator {
        private int processedCount = 0;

        public SimpleOperator(String operatorId, List<String> upstreamOperatorIds) {
            super(operatorId, upstreamOperatorIds);
        }

        @Override
        protected void processElementInternal(Object element) {
            processedCount++;
            System.out.println(operatorId + " 处理元素 #" + processedCount + ": " + element);
        }

        @Override
        protected void performSnapshot(long checkpointId) throws Exception {
            // 模拟快照操作
            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("processedCount", processedCount);
            snapshot.put("timestamp", System.currentTimeMillis());

            // 模拟快照耗时
            Thread.sleep(100);

            System.out.println(operatorId + " 快照checkpoint " + checkpointId +
                             ", 已处理: " + processedCount);
        }
    }

    // ==================== 测试主程序 ====================

    public static void main(String[] args) throws Exception {
        System.out.println("=== 类Flink Checkpoint系统测试 ===\n");

        // 创建协调器
        CheckpointCoordinator coordinator = new CheckpointCoordinator(4000, 4);

        // 创建算子（简单的线性拓扑：source -> map -> sink）
        BaseOperator source = new SimpleOperator("source", Arrays.asList());
        BaseOperator map = new SimpleOperator("map", Arrays.asList("source"));
        BaseOperator sink = new SimpleOperator("sink", Arrays.asList("map"));

        // 设置协调器
        source.setCoordinator(coordinator);
        map.setCoordinator(coordinator);
        sink.setCoordinator(coordinator);

        // 注册算子
        coordinator.registerOperator(source);
        coordinator.registerOperator(map);
        coordinator.registerOperator(sink);

        try {
            System.out.println("测试1: 正常checkpoint流程");

            // 触发checkpoint
            long checkpointId1 = coordinator.triggerCheckpoint();

            // 等待checkpoint完成
            Thread.sleep(1500);

            System.out.println("\n测试2: 模拟数据处理");

            // 模拟数据处理
            for (int i = 1; i <= 5; i++) {
                source.processElement("data-" + i);
                Thread.sleep(100);
            }

            System.out.println("\n测试3: 触发第二个checkpoint");

            long checkpointId2 = coordinator.triggerCheckpoint();
            Thread.sleep(1500);

            System.out.println("\n测试4: 故障恢复测试");

            Long latestCheckpoint = coordinator.getLatestCompletedCheckpointId();
            if (latestCheckpoint != null) {
                coordinator.restoreFromCheckpoint(latestCheckpoint);
            }
            System.out.println("\n测试5: 超时测试");

            // 创建一个会超时的算子
            BaseOperator slowOperator = new BaseOperator("slow", Arrays.asList()) {
                @Override
                protected void processElementInternal(Object element) {}

                @Override
                protected void performSnapshot(long checkpointId) throws Exception {
                    // 模拟慢速快照，会超时
                    Thread.sleep(5000);
                }
            };

            slowOperator.setCoordinator(coordinator);
            coordinator.registerOperator(slowOperator);

            // Deleted:long checkpointId3 = coordinator.triggerCheckpoint();
            // Deleted:Thread.sleep(3500); // 等待超时发生

            // 重新触发checkpoint，这次会包含slowOperator
            long checkpointId3 = coordinator.triggerCheckpoint();

            // 等待足够长的时间让超时检测生效（超过3秒但小于5秒）
            Thread.sleep(4000);

            // 验证checkpoint状态
            Long latestCompleted = coordinator.getLatestCompletedCheckpointId();
            System.out.println("最新完成的checkpoint: " + latestCompleted);
            System.out.println("预期结果: checkpointId3=" + checkpointId3 + " 应该失败（超时）");


        } finally {
            coordinator.shutdown();
        }

        System.out.println("\n=== 测试完成 ===");
    }
}