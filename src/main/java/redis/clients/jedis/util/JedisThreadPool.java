package redis.clients.jedis.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Protocol;

/**
 * This class is used to build a thread pool for Jedis.
 */
public class JedisThreadPool {

    private static final Logger log = LoggerFactory.getLogger(JedisThreadPool.class);

    private static final RejectedExecutionHandler defaultRejectHandler = new AbortPolicy();

    public static PoolBuilder poolBuilder() {
        return new PoolBuilder();
    }

  /**
   * The following are the default parameters for the multi node pipeline executor
   * Since Redis query is usually a slower IO operation (requires more threads),
   * so we set DEFAULT_CORE_POOL_SIZE to be the same as the core
   */
  private static final long DEFAULT_KEEPALIVE_TIME_MS = 60000L;
  private static final int DEFAULT_BLOCKING_QUEUE_SIZE = Protocol.CLUSTER_HASHSLOTS;
  private static final int DEFAULT_CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors();
  private static final int DEFAULT_MAXIMUM_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
  private static ExecutorService executorService = null;

  public static ExecutorService getThreadPool() {
    return executorService;
  }

  /**
   * Provide an interface for users to set executors themselves.
   * @param executor the executor
   */
  public static void setThreadPool(ExecutorService executor) {
    if (executorService != executor && executorService != null) {
      executorService.shutdown();
    }
    executorService = executor;
  }

  /**
   * Create thread pool with default configurations.
   * @return thread pool
   */
  public static ExecutorService createDefaultThreadPool() {
    return JedisThreadPool.poolBuilder()
      .setCoreSize(DEFAULT_CORE_POOL_SIZE)
      .setMaxSize(DEFAULT_MAXIMUM_POOL_SIZE)
      .setKeepAliveMillSecs(DEFAULT_KEEPALIVE_TIME_MS)
      .setThreadNamePrefix("jedis-multi-node-pipeline")
      .setWorkQueue(new ArrayBlockingQueue<>(DEFAULT_BLOCKING_QUEUE_SIZE)).build();
  }

  /**
   * This is a shortcut for {@link JedisThreadPool#createDefaultThreadPool()} and
   * {@link JedisThreadPool#setThreadPool(java.util.concurrent.ExecutorService)}.
   */
  public static void createAndUseDefaultThreadPool() {
    setThreadPool(createDefaultThreadPool());
  }

    /**
     * Custom thread factory or use default
     * @param threadNamePrefix the thread name prefix
     * @param daemon daemon
     * @return ThreadFactory
     */
    private static ThreadFactory createThreadFactory(String threadNamePrefix, boolean daemon) {
        if (threadNamePrefix != null) {
            return new JedisThreadFactoryBuilder().setNamePrefix(threadNamePrefix).setDaemon(daemon)
                .setUncaughtExceptionHandler((Thread t, Throwable e) -> {
                    log.error(String.format("Thread %s threw exception %s", t.getName(), e.getMessage()));
            }).build();
        }

        return Executors.defaultThreadFactory();
    }

    /**
     * This class is used to build a thread pool.
     */
    public static class PoolBuilder {
        private int coreSize = 0;
        private int maxSize = Integer.MAX_VALUE;
        private long keepAliveMillSecs = 10;
        private ThreadFactory threadFactory;
        private String threadNamePrefix;
        private boolean daemon;
        private RejectedExecutionHandler rejectHandler;
        private BlockingQueue<Runnable> workQueue;

        public PoolBuilder setCoreSize(int coreSize) {
            this.coreSize = coreSize;
            return this;
        }

        public PoolBuilder setMaxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public PoolBuilder setKeepAliveMillSecs(long keepAliveMillSecs) {
            this.keepAliveMillSecs = keepAliveMillSecs;
            return this;
        }

        public PoolBuilder setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
            return this;
        }

        public PoolBuilder setDaemon(boolean daemon) {
            this.daemon = daemon;
            return this;
        }

        public PoolBuilder setThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public PoolBuilder setRejectHandler(RejectedExecutionHandler rejectHandler) {
            this.rejectHandler = rejectHandler;
            return this;
        }

        public PoolBuilder setWorkQueue(BlockingQueue<Runnable> workQueue) {
            this.workQueue = workQueue;
            return this;
        }

        public ExecutorService build() {
            if (threadFactory == null) {
                threadFactory = createThreadFactory(threadNamePrefix, daemon);
            }

            if (workQueue == null) {
                throw new IllegalArgumentException("workQueue can't be null");
            }

            if (rejectHandler == null) {
                rejectHandler = defaultRejectHandler;
            }

            ExecutorService executorService = new ThreadPoolExecutor(coreSize, maxSize, keepAliveMillSecs,
                TimeUnit.MILLISECONDS, workQueue, threadFactory, rejectHandler);

            return executorService;
        }
    }
}
