package me.himadieiev.redpulsar.core.locks

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import me.himadieiev.redpulsar.core.locks.abstracts.AbstractLock
import me.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
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

    /**
     * Lock the resource with given name on a single Redis instance/cluster.
     * @return true if lock was acquired, false otherwise.
     */
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

    /**
     * Unlock the resource with given name on a single Redis instance/cluster.
     */
    override fun unlock(resourceName: String) {
        unlockInstance(backend, resourceName)
    }
}
