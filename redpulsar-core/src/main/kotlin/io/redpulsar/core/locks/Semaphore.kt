package io.redpulsar.core.locks

import io.redpulsar.core.locks.abstracts.AbstractMultyInstanceLock
import io.redpulsar.core.locks.abstracts.LocksBackend
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * An implementation for Semaphore lock in distributed systems.
 * Semaphore lock is a lock that allows multiple clients to acquire the lock until limit reached.
 */
class Semaphore(
    backends: List<LocksBackend>,
    private val maxLeases: Int,
    private val retryDelay: Duration = 200.milliseconds,
    private val retryCount: Int = 3,
    scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
) : AbstractMultyInstanceLock(backends, scope) {
    private val globalKeyPrefix = "semaphore"
    private val leasersKey = "leasers"

    init {
        require(maxLeases > 0) { "Max leases should be positive number" }
        require(retryDelay > 0.milliseconds) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        val minTtl = 10.milliseconds
        // ttl is longer that other locks because process of acquiring lock is longer.
        require(ttl > minTtl) { "Timeout is too small." }
        return multyLock(resourceName, ttl, minTtl, retryCount, retryDelay)
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

    private inline fun buildKey(vararg parts: String) = globalKeyPrefix + ":" + parts.joinToString(":")
}
