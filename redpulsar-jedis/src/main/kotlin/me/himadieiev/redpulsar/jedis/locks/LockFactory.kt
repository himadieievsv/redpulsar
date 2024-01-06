package me.himadieiev.redpulsar.jedis.locks

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import me.himadieiev.redpulsar.core.locks.ListeningCountDownLatch
import me.himadieiev.redpulsar.core.locks.RedLock
import me.himadieiev.redpulsar.core.locks.Semaphore
import me.himadieiev.redpulsar.core.locks.SimpleLock
import me.himadieiev.redpulsar.jedis.locks.backends.JedisCountDownLatchBackend
import me.himadieiev.redpulsar.jedis.locks.backends.JedisLocksBackend
import redis.clients.jedis.UnifiedJedis
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

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
            retryDelay: Duration = 100.milliseconds,
            retryCount: Int = 3,
        ): SimpleLock {
            return SimpleLock(JedisLocksBackend(client), retryDelay, retryCount)
        }

        /**
         * Create a new [RedLock] instance.
         * @param clients [List]<[UnifiedJedis]> the Jedis client instances to use for lock.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @param scope [CoroutineScope] the coroutine scope to use for lock.
         * @return [RedLock] the lock instance.
         */
        @JvmStatic
        fun createRedLock(
            clients: List<UnifiedJedis>,
            retryDelay: Duration = 100.milliseconds,
            retryCount: Int = 3,
            scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
        ): RedLock {
            val backends = clients.map { JedisLocksBackend(it) }
            return RedLock(backends, retryCount, retryDelay, scope)
        }

        /**
         * Create a new [Semaphore] instance.
         * @param clients [List]<[UnifiedJedis]> the Jedis client instances to use for lock.
         * @param maxLeases [Int] the maximum number of leases.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @param scope [CoroutineScope] the coroutine scope to use for lock.
         * @return [Semaphore] the lock instance.
         */
        @JvmStatic
        fun createSemaphore(
            clients: List<UnifiedJedis>,
            maxLeases: Int,
            retryDelay: Duration = 100.milliseconds,
            retryCount: Int = 3,
            scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
        ): Semaphore {
            val backends = clients.map { JedisLocksBackend(it) }
            return Semaphore(backends, maxLeases, retryCount, retryDelay, scope)
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
            maxDuration: Duration = 5.minutes,
            retryCount: Int = 3,
            retryDelay: Duration = 100.milliseconds,
        ): ListeningCountDownLatch {
            val backends = clients.map { JedisCountDownLatchBackend(it) }
            return ListeningCountDownLatch(name, count, backends, maxDuration, retryCount, retryDelay)
        }
    }
}
