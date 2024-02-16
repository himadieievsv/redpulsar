package com.himadieiev.redpulsar.lettuce.locks

import com.himadieiev.redpulsar.core.locks.ListeningCountDownLatch
import com.himadieiev.redpulsar.core.locks.Mutex
import com.himadieiev.redpulsar.core.locks.Semaphore
import com.himadieiev.redpulsar.core.locks.SimplifiedMutex
import com.himadieiev.redpulsar.lettuce.LettucePubSubPooled
import com.himadieiev.redpulsar.lettuce.abstracts.LettuceUnified
import com.himadieiev.redpulsar.lettuce.locks.backends.LettuceCountDownLatchBackend
import com.himadieiev.redpulsar.lettuce.locks.backends.LettuceLocksBackend
import java.time.Duration

/**
 * A factory for creating lock instances.
 */
class LockFactory {
    companion object {
        /**
         * Create a new [SimplifiedMutex] instance.
         * @param client [LettuceUnified] the Jedis client instance to use for lock.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @return [SimplifiedMutex] the lock instance.
         */
        @JvmStatic
        fun createSimplifiedMutex(
            client: LettuceUnified<String, String>,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
        ): SimplifiedMutex {
            return SimplifiedMutex(LettuceLocksBackend(client), retryDelay, retryCount)
        }

        /**
         * Create a new [Mutex] instance.
         * @param clients [List]<[LettuceUnified]> the Jedis client instances to use for lock.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @return [Mutex] the lock instance.
         */
        @JvmStatic
        fun createMutex(
            clients: List<LettuceUnified<String, String>>,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
        ): Mutex {
            val backends = clients.map { LettuceLocksBackend(it) }
            return Mutex(backends, retryCount, retryDelay)
        }

        /**
         * Create a new [Semaphore] instance.
         * @param clients [List]<[LettuceUnified]> the Jedis client instances to use for lock.
         * @param maxLeases [Int] the maximum number of leases.
         * @param retryDelay [Duration] the delay between retries.
         * @param retryCount [Int] the number of retries.
         * @return [Semaphore] the lock instance.
         */
        @JvmStatic
        fun createSemaphore(
            clients: List<LettuceUnified<String, String>>,
            maxLeases: Int,
            retryDelay: Duration = Duration.ofMillis(100),
            retryCount: Int = 3,
        ): Semaphore {
            val backends = clients.map { LettuceLocksBackend(it) }
            return Semaphore(backends, maxLeases, retryCount, retryDelay)
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
        @JvmStatic
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
