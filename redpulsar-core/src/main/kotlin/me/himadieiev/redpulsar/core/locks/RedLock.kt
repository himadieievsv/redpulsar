package me.himadieiev.redpulsar.core.locks

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import me.himadieiev.redpulsar.core.locks.abstracts.AbstractMultyInstanceLock
import me.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * A distributed lock for single or multiple Redis instances / clusters.
 * It uses Redlock algorithm to determine if the lock was acquired.
 * See details in [AbstractMultyInstanceLock].
 */
class RedLock(
    backends: List<LocksBackend>,
    private val retryCount: Int = 3,
    private val retryDelay: Duration = 100.milliseconds,
    scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
) : AbstractMultyInstanceLock(backends, scope) {
    init {
        require(retryDelay > 0.milliseconds) { "Retry delay must be positive" }
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
        require(ttl > 2.milliseconds) { "Timeout must be greater that min clock drift." }
        return multyLock(resourceName, ttl, 2.milliseconds, retryCount, retryDelay)
    }
}
