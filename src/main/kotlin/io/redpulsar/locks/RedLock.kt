package io.redpulsar.locks

import io.redpulsar.locks.core.AbstractMultyInstanceLock
import redis.clients.jedis.UnifiedJedis
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * A distributed lock for single or multiple Redis instances / clusters.
 * It uses Redlock algorithm to determine if the lock was acquired.
 * See details in [AbstractMultyInstanceLock].
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
