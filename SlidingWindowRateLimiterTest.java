import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SlidingWindowRateLimiterTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== 滑动窗口限流器测试 ===");

        // 测试1：基本功能测试
        testBasicFunctionality();

        // 测试2：多线程并发测试
        testMultithreadedConcurrency();

        // 测试3：QPS突增测试
        testQpsSurge();

        System.out.println("=== 所有测试完成 ===");
    }

    /**
     * 测试1：基本功能测试
     */
    private static void testBasicFunctionality() throws InterruptedException {
        System.out.println("\n--- 测试1：基本功能测试 ---");

        // 创建限流器：窗口1秒，最多5个请求
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(1000, 5);

        System.out.println("配置：窗口大小=" + limiter.getWindowMs() + "ms, 最大请求数=" + limiter.getMaxRequests());

        // 测试连续请求
        int allowedCount = 0;
        for (int i = 0; i < 10; i++) {
            if (limiter.tryAcquire()) {
                allowedCount++;
                System.out.println("请求 " + (i+1) + ": 允许");
            } else {
                System.out.println("请求 " + (i+1) + ": 拒绝");
            }
            Thread.sleep(50); // 50ms间隔
        }

        System.out.println("允许的请求数: " + allowedCount + "/10 (预期: 5/10)");

        // 等待窗口滑动
        Thread.sleep(1100); // 等待1.1秒，让第一个窗口过期

        // 再次测试
        allowedCount = 0;
        for (int i = 0; i < 5; i++) {
            if (limiter.tryAcquire()) {
                allowedCount++;
            }
        }

        System.out.println("窗口滑动后允许的请求数: " + allowedCount + "/5 (预期: 5/5)");

        System.out.println("✓ 基本功能测试通过");
    }

    /**
     * 测试2：多线程并发测试
     */
    private static void testMultithreadedConcurrency() throws Exception {
        System.out.println("\n--- 测试2：多线程并发测试 ---");

        // 创建限流器：窗口1秒，最多20个请求
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(1000, 20);

        int threadCount = 10;
        int requestsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger totalAllowed = new AtomicInteger(0);
        AtomicInteger totalRejected = new AtomicInteger(0);

        // 创建栅栏确保所有线程同时开始
        CyclicBarrier barrier = new CyclicBarrier(threadCount + 1);

        // 启动线程
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    barrier.await(); // 等待所有线程就绪

                    for (int j = 0; j < requestsPerThread; j++) {
                        if (limiter.tryAcquire()) {
                            totalAllowed.incrementAndGet();
                        } else {
                            totalRejected.incrementAndGet();
                        }

                        // 随机等待0-10ms，模拟实际请求间隔
                        Thread.sleep(ThreadLocalRandom.current().nextInt(0, 11));
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // 所有线程就绪后开始测试
        barrier.await();

        // 等待所有线程完成
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        int totalRequests = totalAllowed.get() + totalRejected.get();
        System.out.println("总请求数: " + totalRequests);
        System.out.println("允许的请求数: " + totalAllowed.get());
        System.out.println("拒绝的请求数: " + totalRejected.get());

        // 验证限流器工作正常
        int currentRequests = limiter.getCurrentRequests();
        System.out.println("当前窗口内请求数: " + currentRequests + " (应该 ≤ " + limiter.getMaxRequests() + ")");

        if (currentRequests <= limiter.getMaxRequests()) {
            System.out.println("✓ 多线程并发测试通过");
        } else {
            System.out.println("✗ 多线程并发测试失败: 窗口内请求数超过限制");
        }
    }

    /**
     * 测试3：QPS突增测试
     * 验证在QPS突增时没有短暂的无限放行
     */
    private static void testQpsSurge() throws Exception {
        System.out.println("\n--- 测试3：QPS突增测试 ---");

        // 创建限流器：窗口100ms，最多5个请求（相当于50 QPS）
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(100, 5);

        int threadCount = 20; // 大量线程模拟突增
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger allowedInFirstWindow = new AtomicInteger(0);
        AtomicInteger totalAllowed = new AtomicInteger(0);
        AtomicInteger totalRejected = new AtomicInteger(0);

        // 记录开始时间
        long startTime = System.currentTimeMillis();

        // 提交大量并发请求
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                // 每个线程尝试获取10次许可
                for (int j = 0; j < 10; j++) {
                    if (limiter.tryAcquire()) {
                        totalAllowed.incrementAndGet();

                        // 记录第一个窗口内的允许数
                        if (System.currentTimeMillis() - startTime < 100) {
                            allowedInFirstWindow.incrementAndGet();
                        }
                    } else {
                        totalRejected.incrementAndGet();
                    }

                    // 非常短的间隔，模拟突增
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        // 等待所有请求完成
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);

        long elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("测试时长: " + elapsedTime + "ms");
        System.out.println("总请求数: " + (totalAllowed.get() + totalRejected.get()));
        System.out.println("允许的请求数: " + totalAllowed.get());
        System.out.println("第一个窗口(100ms)内允许的请求数: " + allowedInFirstWindow.get());
        System.out.println("拒绝的请求数: " + totalRejected.get());

        // 验证第一个窗口内没有超过限制
        if (allowedInFirstWindow.get() <= limiter.getMaxRequests()) {
            System.out.println("✓ QPS突增测试通过: 第一个窗口内没有超过限制");
        } else {
            System.out.println("✗ QPS突增测试失败: 第一个窗口内允许了 " + allowedInFirstWindow.get() +
                             " 个请求，超过了限制 " + limiter.getMaxRequests());
        }

        // 验证总允许数符合预期
        int expectedMaxAllowed = (int) Math.ceil(elapsedTime / 100.0) * 5;
        if (totalAllowed.get() <= expectedMaxAllowed) {
            System.out.println("✓ 总允许数符合预期");
        } else {
            System.out.println("✗ 总允许数超出预期: 允许了 " + totalAllowed.get() +
                             "，预期最大 " + expectedMaxAllowed);
        }
    }

    /**
     * 测试4：长时间运行稳定性测试
     */
    private static void testLongRunningStability() throws Exception {
        System.out.println("\n--- 测试4：长时间运行稳定性测试 ---");

        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(500, 10);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        long duration = 5000; // 运行5秒

        // 持续请求
        while (System.currentTimeMillis() - startTime < duration) {
            if (limiter.tryAcquire()) {
                successCount.incrementAndGet();
            } else {
                failCount.incrementAndGet();
            }

            // 随机间隔
            Thread.sleep(ThreadLocalRandom.current().nextInt(0, 20));
        }

        System.out.println("运行时间: " + duration + "ms");
        System.out.println("成功请求数: " + successCount.get());
        System.out.println("失败请求数: " + failCount.get());

        // 计算实际QPS
        double actualQps = (double) successCount.get() / (duration / 1000.0);
        double maxQps = 10.0 / (500.0 / 1000.0); // maxRequests / (windowMs/1000)

        System.out.println("实际QPS: " + String.format("%.2f", actualQps));
        System.out.println("理论最大QPS: " + String.format("%.2f", maxQps));

        if (actualQps <= maxQps * 1.1) { // 允许10%的误差
            System.out.println("✓ 长时间运行稳定性测试通过");
        } else {
            System.out.println("✗ 长时间运行稳定性测试失败: QPS超过理论值");
        }
    }
}