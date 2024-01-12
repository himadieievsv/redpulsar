package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.AbstractMultiInstanceLock
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import java.time.Duration

/**
 * An implementation for Semaphore lock in distributed systems.
 * Semaphore lock is a lock that allows multiple clients to acquire the lock until lock limit is reached.
 *
 * @param backends [List] of [LocksBackend] instances.
 * @param maxLeases [Int] the maximum number of leases that can be acquired.
 * @param retryCount [Int] the number of retries to acquire lock.
 * @param retryDelay [Duration] the delay between retries.
 * @param scope [CoroutineScope] the scope for coroutines.
 */
class Semaphore(
    backends: List<LocksBackend>,
    private val maxLeases: Int,
    retryCount: Int = 3,
    retryDelay: Duration = Duration.ofMillis(100),
    scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
) : AbstractMultiInstanceLock(backends, scope, retryCount, retryDelay) {
    private val globalKeyPrefix = "semaphore"
    private val leasersKey = "leasers"

    init {
        require(maxLeases > 0) { "Max leases should be positive number" }
        require(retryDelay.toMillis() > 0) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    /**
     * Lock the resource with given name on a single Redis instance/cluster.
     * Multiple locks can be acquired.
     * @return true if lock was acquired, false otherwise.
     */
    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        // ttl is longer that other locks because process of acquiring lock is longer.
        require(ttl.toMillis() > 3 * backendSize()) { "Timeout is too small." }
        return super.lock(resourceName, ttl)
    }

    override fun lockInstance(
        backend: LocksBackend,
        resourceName: String,
        ttl: Duration,
    ): String? {
        val leasersKey = buildKey(leasersKey, resourceName)
        val leaserValidityKey = buildKey(resourceName, clientId)
        return backend.setSemaphoreLock(leasersKey, leaserValidityKey, clientId, maxLeases, ttl)
    }

    override fun unlockInstance(
        backend: LocksBackend,
        resourceName: String,
    ): String? {
        val leasersKey = buildKey(leasersKey, resourceName)
        val leaserValidityKey = buildKey(resourceName, clientId)
        val removeSemaphoreLock = backend.removeSemaphoreLock(leasersKey, leaserValidityKey, clientId)
        // clean up expired other leasers
        cleanUp(backend, resourceName)
        return removeSemaphoreLock
    }

    private fun cleanUp(
        backend: LocksBackend,
        resourceName: String,
    ) {
        val leasersKey = buildKey(leasersKey, resourceName)
        val leaserValidityKeyPrefix = buildKey(resourceName)
        backend.cleanUpExpiredSemaphoreLocks(leasersKey, leaserValidityKeyPrefix)
    }

    @Suppress("NOTHING_TO_INLINE")
    private inline fun buildKey(vararg parts: String) = globalKeyPrefix + ":" + parts.joinToString(":")
}
