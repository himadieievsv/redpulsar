package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.AbstractMultyInstanceLock
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import java.time.Duration

/**
 * An implementation for Semaphore lock in distributed systems.
 * Semaphore lock is a lock that allows multiple clients to acquire the lock until lock limit is reached.
 */
class Semaphore(
    backends: List<LocksBackend>,
    private val maxLeases: Int,
    private val retryCount: Int = 3,
    private val retryDelay: Duration = Duration.ofMillis(100),
    scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
) : AbstractMultyInstanceLock(backends, scope) {
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
        require(ttl.toMillis() > 10) { "Timeout is too small." }
        return multyLock(resourceName, ttl, Duration.ofMillis(10), retryCount, retryDelay)
    }

    override fun lockInstance(
        backend: LocksBackend,
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        val leasersKey = buildKey(leasersKey, resourceName)
        val leaserValidityKey = buildKey(resourceName, clientId)
        return backend.setSemaphoreLock(leasersKey, leaserValidityKey, clientId, maxLeases, ttl) != null
    }

    override fun unlockInstance(
        backend: LocksBackend,
        resourceName: String,
    ) {
        val leasersKey = buildKey(leasersKey, resourceName)
        val leaserValidityKey = buildKey(resourceName, clientId)
        backend.removeSemaphoreLock(leasersKey, leaserValidityKey, clientId)
        // clean up expired other leasers
        cleanUp(backend, resourceName)
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
