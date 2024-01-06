package me.himadieiev.redpulsar.lettuce.locks

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import me.himadieiev.redpulsar.core.locks.ListeningCountDownLatch
import me.himadieiev.redpulsar.core.locks.RedLock
import me.himadieiev.redpulsar.core.locks.Semaphore
import me.himadieiev.redpulsar.core.locks.SimpleLock
import me.himadieiev.redpulsar.lettuce.LettucePooled
import me.himadieiev.redpulsar.lettuce.LettucePubSubPooled
import me.himadieiev.redpulsar.lettuce.locks.backends.LettuceCountDownLatchBackend
import me.himadieiev.redpulsar.lettuce.locks.backends.LettuceLocksBackend
import java.time.Duration

/**
 * A factory for creating lock instances.
 */
interface LockFactory {
    companion object {
        /**
         * Create a new [SimpleLock] instance.
         * @param client [LettucePooled] the Jedis client instance to use for lock.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @return [SimpleLock] the lock instance.
         */
        fun createSimpleLock(
            client: LettucePooled<String, String>,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
        ): SimpleLock {
            return SimpleLock(LettuceLocksBackend(client), retryDelay, retryCount)
        }

        /**
         * Create a new [RedLock] instance.
         * @param clients [List]<[LettucePooled]> the Jedis client instances to use for lock.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @param scope [CoroutineScope] the coroutine scope to use for lock.
         * @return [RedLock] the lock instance.
         */
        fun createRedLock(
            clients: List<LettucePooled<String, String>>,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
            scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
        ): RedLock {
            val backends = clients.map { LettuceLocksBackend(it) }
            return RedLock(backends, retryCount, retryDelay, scope)
        }

        /**
         * Create a new [Semaphore] instance.
         * @param clients [List]<[LettucePooled]> the Jedis client instances to use for lock.
         * @param maxLeases [Int] the maximum number of leases.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @param scope [CoroutineScope] the coroutine scope to use for lock.
         * @return [Semaphore] the lock instance.
         */
        fun createSemaphore(
            clients: List<LettucePooled<String, String>>,
            maxLeases: Int,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
            scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
        ): Semaphore {
            val backends = clients.map { LettuceLocksBackend(it) }
            return Semaphore(backends, maxLeases, retryCount, retryDelay, scope)
        }

        /**
         * Create a new [ListeningCountDownLatch] instance.
         * @param clients [List]<[LettucePubSubPooled]> the Jedis client instances to use for lock.
         * @param name [String] the name of the latch.
         * @param count [Int] the number of permits.
         * @param maxDuration [Duration] the maximum duration of the latch.
         * @param retryCount [Int] the number of retries.
         * @param retryDelay [Duration] the delay between retries.
         * @return [ListeningCountDownLatch] the latch instance.
         */
        fun createCountDownLatch(
            clients: List<LettucePubSubPooled<String, String>>,
            name: String,
            count: Int,
            maxDuration: Duration = Duration.ofMinutes(10),
            retryCount: Int = 3,
            retryDelay: Duration = Duration.ofMillis(100),
        ): ListeningCountDownLatch {
            val backends = clients.map { LettuceCountDownLatchBackend(it) }
            return ListeningCountDownLatch(name, count, backends, maxDuration, retryCount, retryDelay)
        }
    }
}
