package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.AbstractMultyInstanceLock
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import java.time.Duration

/**
 * A distributed lock for single or multiple Redis instances / clusters.
 * It uses Redlock algorithm to determine if the lock was acquired.
 * See details in [AbstractMultyInstanceLock].
 */
class RedLock(
    backends: List<LocksBackend>,
    private val retryCount: Int = 3,
    private val retryDelay: Duration = Duration.ofMillis(100),
    scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
) : AbstractMultyInstanceLock(backends, scope) {
    init {
        require(retryDelay.toMillis() > 0) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    /**
     * Lock the resource with given name on multiple Redis instances/clusters.
     * @return true if lock was acquired, false otherwise.
     */
    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        require(ttl.toMillis() > 2) { "Timeout must be greater that min clock drift." }
        return multyLock(resourceName, ttl, Duration.ofMillis(2), retryCount, retryDelay)
    }
}
