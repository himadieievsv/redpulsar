package io.redpulsar.locks

import io.redpulsar.locks.core.AbstractMultyInstanceLock
import redis.clients.jedis.UnifiedJedis
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * A distributed lock implementation based on the Redlock algorithm.
 * [RedLock] depends on multiple single Redis instances or clusters to be available.
 * It uses a quorum to determine if the lock was acquired.
 * Details: https://redis.io/docs/manual/patterns/distributed-locks/
 */
class RedLock(
    instances: List<UnifiedJedis>,
    private val retryDelay: Duration = 200.milliseconds,
    private val retryCount: Int = 3,
) : AbstractMultyInstanceLock(instances) {
    init {
        require(retryDelay > 0.milliseconds) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        require(ttl > 2.milliseconds) { "Timeout must be greater that min clock drift." }
        return try {
            multyLock(resourceName, ttl, 2.milliseconds, retryCount, retryDelay)
        } catch (e: CancellationException) {
            logger.error(e) { "Coroutines unexpectedly terminated." }
            false
        }
    }
}
