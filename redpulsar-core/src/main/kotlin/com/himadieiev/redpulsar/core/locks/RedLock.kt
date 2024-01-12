package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.AbstractMultiInstanceLock
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import java.time.Duration

/**
 * A distributed lock for single or multiple Redis instances / clusters.
 * It uses RedLock algorithm to determine if the lock was acquired.
 * See details in [AbstractMultiInstanceLock].
 *
 * @param backends [List] of [LocksBackend] instances.
 * @param retryCount [Int] the number of retries to acquire lock.
 * @param retryDelay [Duration] the delay between retries.
 * @param scope [CoroutineScope] the scope for coroutines.
 */
class RedLock(
    backends: List<LocksBackend>,
    retryCount: Int = 3,
    retryDelay: Duration = Duration.ofMillis(100),
    scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
) : AbstractMultiInstanceLock(backends, scope, retryCount, retryDelay) {
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
        require(ttl.toMillis() > 3 * backendSize()) { "Timeout must be greater that min clock drift." }
        return super.lock(resourceName, ttl)
    }
}
