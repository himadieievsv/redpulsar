package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.AbstractLock
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.time.Duration

/**
 * A distributed lock implementation that using only single Redis Cluster or Redis instance.
 *
 * @param backend [LocksBackend] instance.
 * @param retryDelay [Duration] the delay between retries.
 * @param retryCount [Int] the number of retries to acquire lock.
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
     * @param resourceName [String] the name of the resource for which lock key was created.
     * @param ttl [Duration] the time to live of the lock. Smaller ttl will require better clock synchronization.
     * @return true if lock was acquired, false otherwise.
     */
    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        require(ttl.toMillis() > 2) { "Timeout is too small." }
        var retries = retryCount
        do {
            if (lockInstance(backend, resourceName, ttl) != null) {
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
     * @param resourceName [String] the name of the resource for which lock key was created.
     * @return true if lock was released, false otherwise.
     */
    override fun unlock(resourceName: String): Boolean {
        return unlockInstance(backend, resourceName) != null
    }
}
