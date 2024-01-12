package com.himadieiev.redpulsar.jedis.locks

import com.himadieiev.redpulsar.core.locks.ListeningCountDownLatch
import com.himadieiev.redpulsar.core.locks.RedLock
import com.himadieiev.redpulsar.core.locks.Semaphore
import com.himadieiev.redpulsar.core.locks.SimpleLock
import com.himadieiev.redpulsar.jedis.locks.backends.JedisCountDownLatchBackend
import com.himadieiev.redpulsar.jedis.locks.backends.JedisLocksBackend
import redis.clients.jedis.UnifiedJedis
import java.time.Duration

/**
 * A factory for creating lock instances.
 */
class LockFactory {
    companion object {
        /**
         * Create a new [SimpleLock] instance.
         * @param client [UnifiedJedis] the Jedis client instance to use for lock.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @return [SimpleLock] the lock instance.
         */
        @JvmStatic
        fun createSimpleLock(
            client: UnifiedJedis,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
        ): SimpleLock {
            return SimpleLock(JedisLocksBackend(client), retryDelay, retryCount)
        }

        /**
         * Create a new [RedLock] instance.
         * @param clients [List]<[UnifiedJedis]> the Jedis client instances to use for lock.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @return [RedLock] the lock instance.
         */
        @JvmStatic
        fun createRedLock(
            clients: List<UnifiedJedis>,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
        ): RedLock {
            val backends = clients.map { JedisLocksBackend(it) }
            return RedLock(backends, retryCount, retryDelay)
        }

        /**
         * Create a new [Semaphore] instance.
         * @param clients [List]<[UnifiedJedis]> the Jedis client instances to use for lock.
         * @param maxLeases [Int] the maximum number of leases.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @return [Semaphore] the lock instance.
         */
        @JvmStatic
        fun createSemaphore(
            clients: List<UnifiedJedis>,
            maxLeases: Int,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
        ): Semaphore {
            val backends = clients.map { JedisLocksBackend(it) }
            return Semaphore(backends, maxLeases, retryCount, retryDelay)
        }

        /**
         * Create a new [ListeningCountDownLatch] instance.
         * @param clients [List]<[UnifiedJedis]> the Jedis client instances to use for lock.
         * @param name [String] the name of the latch.
         * @param count [Int] the number of permits.
         * @param maxDuration [Duration] the maximum duration of the latch.
         * @param retryCount [Int] the number of retries.
         * @param retryDelay [Duration] the delay between retries.
         * @return [ListeningCountDownLatch] the latch instance.
         */
        @JvmStatic
        fun createCountDownLatch(
            clients: List<UnifiedJedis>,
            name: String,
            count: Int,
            maxDuration: Duration = Duration.ofMinutes(5),
            retryCount: Int = 3,
            retryDelay: Duration = Duration.ofMillis(100),
        ): ListeningCountDownLatch {
            val backends = clients.map { JedisCountDownLatchBackend(it) }
            return ListeningCountDownLatch(name, count, backends, maxDuration, retryCount, retryDelay)
        }
    }
}
