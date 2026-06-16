package com.liuyanglouis.slidingwindow.scheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LightweightDAGSchedulerTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== 轻量级DAG调度器测试 ===\n");

        // 测试1：基本功能测试
        testBasicFunctionality();

        // 测试2：并行执行测试
        testParallelExecution();

        // 测试3：失败处理测试
        testFailureHandling();

        // 测试4：循环依赖检测测试
        testCycleDetection();

        // 测试5：复杂DAG测试
        testComplexDAG();

        System.out.println("\n=== 所有测试完成 ===");
    }

    /**
     * 测试1：基本功能测试
     * 验证DAG能够按照拓扑顺序正确执行
     */
    private static void testBasicFunctionality() throws Exception {
        System.out.println("--- 测试1：基本功能测试 ---");

        // 创建简单的线性DAG: A -> B -> C
        Map<String, List<String>> dag = new HashMap<>();
        dag.put("A", Arrays.asList());
        dag.put("B", Arrays.asList("A"));
        dag.put("C", Arrays.asList("B"));

        // 创建任务执行顺序记录器
        List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

        // 创建任务映射
        Map<String, LightweightDAGScheduler.Task> tasks = new HashMap<>();
        tasks.put("A", () -> {
            executionOrder.add("A");
            System.out.println("任务A执行");
        });
        tasks.put("B", () -> {
            executionOrder.add("B");
            System.out.println("任务B执行");
        });
        tasks.put("C", () -> {
            executionOrder.add("C");
            System.out.println("任务C执行");
        });

        // 创建执行器
        ExecutorService executor = Executors.newFixedThreadPool(3);

        try {
            // 执行DAG
            LightweightDAGScheduler.execute(dag, tasks, executor);

            // 验证执行顺序
            System.out.println("执行顺序: " + executionOrder);

            // 检查是否正确执行
            if (executionOrder.size() == 3 &&
                executionOrder.get(0).equals("A") &&
                executionOrder.get(1).equals("B") &&
                executionOrder.get(2).equals("C")) {
                System.out.println("✓ 基本功能测试通过：DAG按拓扑顺序执行");
            } else {
                System.out.println("✗ 基本功能测试失败：执行顺序不正确");
            }

        } finally {
            executor.shutdown();
        }
    }

    /**
     * 测试2：并行执行测试
     * 验证无依赖的任务可以并行执行
     */
    private static void testParallelExecution() throws Exception {
        System.out.println("\n--- 测试2：并行执行测试 ---");

        // 创建可并行执行的DAG
        // A和B无依赖，可以并行执行
        // C依赖A，D依赖B
        Map<String, List<String>> dag = new HashMap<>();
        dag.put("A", Arrays.asList());
        dag.put("B", Arrays.asList());
        dag.put("C", Arrays.asList("A"));
        dag.put("D", Arrays.asList("B"));

        // 使用计数器跟踪并行执行
        AtomicInteger concurrentTasks = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        // 创建任务映射
        Map<String, LightweightDAGScheduler.Task> tasks = new HashMap<>();
        tasks.put("A", () -> {
            int current = concurrentTasks.incrementAndGet();
            maxConcurrent.updateAndGet(v -> Math.max(v, current));
            Thread.sleep(100); // 模拟工作
            concurrentTasks.decrementAndGet();
        });
        tasks.put("B", () -> {
            int current = concurrentTasks.incrementAndGet();
            maxConcurrent.updateAndGet(v -> Math.max(v, current));
            Thread.sleep(100);
            concurrentTasks.decrementAndGet();
        });
        tasks.put("C", () -> {
            int current = concurrentTasks.incrementAndGet();
            maxConcurrent.updateAndGet(v -> Math.max(v, current));
            Thread.sleep(50);
            concurrentTasks.decrementAndGet();
        });
        tasks.put("D", () -> {
            int current = concurrentTasks.incrementAndGet();
            maxConcurrent.updateAndGet(v -> Math.max(v, current));
            Thread.sleep(50);
            concurrentTasks.decrementAndGet();
        });

        // 创建执行器
        ExecutorService executor = Executors.newFixedThreadPool(4);

        try {
            // 执行DAG
            LightweightDAGScheduler.execute(dag, tasks, executor);

            System.out.println("最大并发任务数: " + maxConcurrent.get());

            // 验证并行执行（应该至少有2个任务并行执行）
            if (maxConcurrent.get() >= 2) {
                System.out.println("✓ 并行执行测试通过：无依赖任务可以并行执行");
            } else {
                System.out.println("✗ 并行执行测试失败：没有检测到并行执行");
            }

        } finally {
            executor.shutdown();
        }
    }

    /**
     * 测试3：失败处理测试
     * 验证节点失败时下游节点被正确取消
     */
    private static void testFailureHandling() {
        System.out.println("\n--- 测试3：失败处理测试 ---");

        // 创建DAG: A -> B -> C -> D
        Map<String, List<String>> dag = new HashMap<>();
        dag.put("A", Arrays.asList());
        dag.put("B", Arrays.asList("A"));
        dag.put("C", Arrays.asList("B"));
        dag.put("D", Arrays.asList("C"));

        // 创建任务执行记录器
        List<String> executedTasks = Collections.synchronizedList(new ArrayList<>());

        // 创建任务映射，B任务会失败
        Map<String, LightweightDAGScheduler.Task> tasks = new HashMap<>();
        tasks.put("A", () -> {
            executedTasks.add("A");
            System.out.println("任务A执行成功");
        });
        tasks.put("B", () -> {
            executedTasks.add("B");
            throw new RuntimeException("任务B故意失败");
        });
        tasks.put("C", () -> {
            executedTasks.add("C");
            System.out.println("任务C执行成功");
        });
        tasks.put("D", () -> {
            executedTasks.add("D");
            System.out.println("任务D执行成功");
        });

        // 创建执行器
        ExecutorService executor = Executors.newFixedThreadPool(4);

        try {
            // 执行DAG，应该抛出异常
            LightweightDAGScheduler.execute(dag, tasks, executor);

            // 如果执行到这里，说明没有抛出异常，测试失败
            System.out.println("✗ 失败处理测试失败：没有抛出异常");

        } catch (Exception e) {
            System.out.println("捕获到异常: " + e.getMessage());
            System.out.println("已执行的任务: " + executedTasks);

            // 验证只有A和B执行了，C和D应该被取消
            if (executedTasks.contains("A") &&
                executedTasks.contains("B") &&
                !executedTasks.contains("C") &&
                !executedTasks.contains("D")) {
                System.out.println("✓ 失败处理测试通过：下游节点被正确取消");
            } else {
                System.out.println("✗ 失败处理测试失败：下游节点取消不正确");
            }
        } finally {
            executor.shutdown();
        }
    }

    /**
     * 测试4：循环依赖检测测试
     * 验证调度器能检测并拒绝循环依赖
     */
    private static void testCycleDetection() {
        System.out.println("\n--- 测试4：循环依赖检测测试 ---");

        // 创建有循环依赖的DAG: A -> B -> C -> A
        Map<String, List<String>> dag = new HashMap<>();
        dag.put("A", Arrays.asList("C")); // A依赖C
        dag.put("B", Arrays.asList("A")); // B依赖A
        dag.put("C", Arrays.asList("B")); // C依赖B

        // 创建简单任务
        Map<String, LightweightDAGScheduler.Task> tasks = new HashMap<>();
        tasks.put("A", () -> System.out.println("任务A"));
        tasks.put("B", () -> System.out.println("任务B"));
        tasks.put("C", () -> System.out.println("任务C"));

        // 创建执行器
        ExecutorService executor = Executors.newFixedThreadPool(3);

        try {
            // 执行DAG，应该抛出异常
            LightweightDAGScheduler.execute(dag, tasks, executor);

            System.out.println("✗ 循环依赖检测测试失败：没有抛出异常");

        } catch (IllegalArgumentException e) {
            System.out.println("捕获到预期异常: " + e.getMessage());
            System.out.println("✓ 循环依赖检测测试通过：正确检测到循环依赖");
        } catch (Exception e) {
            System.out.println("捕获到意外异常: " + e.getClass().getName() + ": " + e.getMessage());
            System.out.println("✗ 循环依赖检测测试失败：抛出错误的异常类型");
        } finally {
            executor.shutdown();
        }
    }

    /**
     * 测试5：复杂DAG测试
     * 验证复杂依赖关系的DAG能正确执行
     */
    private static void testComplexDAG() throws Exception {
        System.out.println("\n--- 测试5：复杂DAG测试 ---");

        // 创建复杂DAG
        // A -> C, A -> D
        // B -> D, B -> E
        // C -> F
        // D -> F
        // E -> F
        Map<String, List<String>> dag = new HashMap<>();
        dag.put("A", Arrays.asList());
        dag.put("B", Arrays.asList());
        dag.put("C", Arrays.asList("A"));
        dag.put("D", Arrays.asList("A", "B"));
        dag.put("E", Arrays.asList("B"));
        dag.put("F", Arrays.asList("C", "D", "E"));

        // 创建执行顺序记录器
        List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

        // 创建任务映射
        Map<String, LightweightDAGScheduler.Task> tasks = new HashMap<>();
        for (String nodeId : dag.keySet()) {
            final String id = nodeId;
            tasks.put(nodeId, () -> {
                synchronized (executionOrder) {
                    executionOrder.add(id);
                }
                System.out.println("任务" + id + "执行");
                Thread.sleep(50); // 模拟工作
            });
        }

        // 创建执行器
        ExecutorService executor = Executors.newFixedThreadPool(6);

        try {
            // 执行DAG
            LightweightDAGScheduler.execute(dag, tasks, executor);

            System.out.println("执行顺序: " + executionOrder);

            // 验证依赖关系
            int aIndex = executionOrder.indexOf("A");
            int bIndex = executionOrder.indexOf("B");
            int cIndex = executionOrder.indexOf("C");
            int dIndex = executionOrder.indexOf("D");
            int eIndex = executionOrder.indexOf("E");
            int fIndex = executionOrder.indexOf("F");

            boolean correctOrder = true;

            // 验证依赖关系：C在A之后
            if (cIndex <= aIndex) {
                System.out.println("错误：C应该在A之后执行");
                correctOrder = false;
            }

            // 验证依赖关系：D在A和B之后
            if (dIndex <= Math.max(aIndex, bIndex)) {
                System.out.println("错误：D应该在A和B之后执行");
                correctOrder = false;
            }

            // 验证依赖关系：E在B之后
            if (eIndex <= bIndex) {
                System.out.println("错误：E应该在B之后执行");
                correctOrder = false;
            }

            // 验证依赖关系：F在C、D、E之后
            if (fIndex <= Math.max(Math.max(cIndex, dIndex), eIndex)) {
                System.out.println("错误：F应该在C、D、E之后执行");
                correctOrder = false;
            }

            if (correctOrder) {
                System.out.println("✓ 复杂DAG测试通过：所有依赖关系正确满足");
            } else {
                System.out.println("✗ 复杂DAG测试失败：依赖关系不正确");
            }

        } finally {
            executor.shutdown();
        }
    }

    /**
     * 测试6：性能测试 - 大量节点
     */
    private static void testPerformance() throws Exception {
        System.out.println("\n--- 测试6：性能测试 ---");

        // 创建一个有100个节点的线性DAG
        Map<String, List<String>> dag = new HashMap<>();
        Map<String, LightweightDAGScheduler.Task> tasks = new HashMap<>();

        // 创建线性链：node1 -> node2 -> node3 -> ... -> node100
        for (int i = 1; i <= 100; i++) {
            String nodeId = "node" + i;
            List<String> dependencies = new ArrayList<>();

            if (i > 1) {
                dependencies.add("node" + (i - 1));
            }

            dag.put(nodeId, dependencies);

            // 创建简单任务
            final int taskNum = i;
            tasks.put(nodeId, () -> {
                // 简单任务，只是记录执行
                System.out.print(".");
                if (taskNum % 50 == 0) System.out.println();
            });
        }

        // 创建执行器
        ExecutorService executor = Executors.newFixedThreadPool(10);

        try {
            long startTime = System.currentTimeMillis();

            // 执行DAG
            LightweightDAGScheduler.execute(dag, tasks, executor);

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            System.out.println("\n执行时间: " + duration + "ms");
            System.out.println("✓ 性能测试完成");

        } finally {
            executor.shutdown();
        }
    }
}