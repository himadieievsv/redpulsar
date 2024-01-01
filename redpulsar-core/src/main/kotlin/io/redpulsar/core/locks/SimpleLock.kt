package io.redpulsar.core.locks

import io.redpulsar.core.locks.abstracts.AbstractLock
import io.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * A distributed lock implementation that using only single Redis Cluster or Redis instance.
 */
class SimpleLock(
    private val backend: LocksBackend,
    private val retryDelay: Duration = 200.milliseconds,
    private val retryCount: Int = 3,
) : AbstractLock() {
    init {
        require(retryDelay > 0.milliseconds) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        require(ttl > 2.milliseconds) { "Timeout is too small." }
        var retries = retryCount
        do {
            if (lockInstance(backend, resourceName, ttl)) {
                return true
            }
            runBlocking {
                delay(retryDelay.inWholeMilliseconds)
            }
        } while (--retries > 0)
        return false
    }

    override fun unlock(resourceName: String) {
        unlockInstance(backend, resourceName)
    }
}
