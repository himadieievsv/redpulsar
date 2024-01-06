package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.AbstractLock
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.time.Duration

/**
 * A distributed lock implementation that using only single Redis Cluster or Redis instance.
 */
class SimpleLock(
    private val backend: LocksBackend,
    private val retryDelay: Duration = Duration.ofMillis(100),
    private val retryCount: Int = 3,
) : AbstractLock() {
    init {
        require(retryDelay.toMillis() > 0) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    /**
     * Lock the resource with given name on a single Redis instance/cluster.
     * @return true if lock was acquired, false otherwise.
     */
    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        require(ttl.toMillis() > 2) { "Timeout is too small." }
        var retries = retryCount
        do {
            if (lockInstance(backend, resourceName, ttl)) {
                return true
            }
            runBlocking {
                delay(retryDelay.toMillis())
            }
        } while (--retries > 0)
        return false
    }

    /**
     * Unlock the resource with given name on a single Redis instance/cluster.
     */
    override fun unlock(resourceName: String) {
        unlockInstance(backend, resourceName)
    }
}
