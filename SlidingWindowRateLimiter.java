import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 滑动窗口限流器
 *
 * 设计特点：
 * 1. 线程安全：使用 ReentrantLock 保护临界区
 * 2. 精确滑动窗口：存储每个请求的时间戳
 * 3. 无短暂无限放行：清理过期请求和检查新请求在同一锁内完成
 * 4. 内存高效：自动清理过期请求，队列不会无限增长
 *
 * 实现原理：
 * - 使用队列存储请求时间戳
 * - 每次 tryAcquire() 时：
 *   1. 加锁
 *   2. 清理窗口开始时间之前的过期请求
 *   3. 检查当前请求数是否超过限制
 *   4. 如果未超过，添加当前时间戳并返回 true
 * - 所有操作在锁内完成，避免竞态条件
 */
public class SlidingWindowRateLimiter {

    // 窗口大小（毫秒）
    private final long windowMs;

    // 窗口内最大请求数
    private final int maxRequests;

    // 存储请求时间戳的队列
    private final LinkedList<Long> requestTimestamps;

    // 用于保护临界区的可重入锁
    private final ReentrantLock lock;

    /**
     * 构造函数
     *
     * @param windowMs 窗口大小（毫秒），必须大于0
     * @param maxRequests 窗口内最大请求数，必须大于0
     * @throws IllegalArgumentException 如果参数不合法
     */
    public SlidingWindowRateLimiter(long windowMs, int maxRequests) {
        if (windowMs <= 0) {
            throw new IllegalArgumentException("窗口大小必须大于0");
        }
        if (maxRequests <= 0) {
            throw new IllegalArgumentException("最大请求数必须大于0");
        }

        this.windowMs = windowMs;
        this.maxRequests = maxRequests;
        this.requestTimestamps = new LinkedList<>();
        this.lock = new ReentrantLock();
    }

    /**
     * 尝试获取许可
     *
     * @return true 如果允许请求，false 如果不允许
     */
    public boolean tryAcquire() {
        long currentTime = System.currentTimeMillis();
        long windowStart = currentTime - windowMs;

        lock.lock();
        try {
            // 清理过期请求
            while (!requestTimestamps.isEmpty() && requestTimestamps.peekFirst() < windowStart) {
                requestTimestamps.pollFirst();
            }

            // 检查是否允许新请求
            if (requestTimestamps.size() < maxRequests) {
                requestTimestamps.addLast(currentTime);
                return true;
            }

            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 带超时的尝试获取许可
     *
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return true 如果允许请求，false 如果超时或不允许
     * @throws InterruptedException 如果线程被中断
     */
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        long waitTime = Math.min(10, unit.toMillis(timeout) / 10);

        while (System.currentTimeMillis() < deadline) {
            if (tryAcquire()) {
                return true;
            }

            // 避免忙等待，短暂休眠
            Thread.sleep(Math.min(waitTime, deadline - System.currentTimeMillis()));
        }

        return false;
    }

    /**
     * 获取当前窗口内的请求数
     *
     * @return 当前窗口内的请求数
     */
    public int getCurrentRequests() {
        long currentTime = System.currentTimeMillis();
        long windowStart = currentTime - windowMs;

        lock.lock();
        try {
            // 清理过期请求
            while (!requestTimestamps.isEmpty() && requestTimestamps.peekFirst() < windowStart) {
                requestTimestamps.pollFirst();
            }

            return requestTimestamps.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重置限流器（清空所有请求记录）
     */
    public void reset() {
        lock.lock();
        try {
            requestTimestamps.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取窗口大小（毫秒）
     */
    public long getWindowMs() {
        return windowMs;
    }

    /**
     * 获取最大请求数
     */
    public int getMaxRequests() {
        return maxRequests;
    }

    /**
     * 获取限流器的预估QPS（每秒请求数）
     *
     * @return 预估的QPS
     */
    public double getEstimatedQps() {
        int currentRequests = getCurrentRequests();
        return (double) currentRequests * 1000 / windowMs;
    }
}