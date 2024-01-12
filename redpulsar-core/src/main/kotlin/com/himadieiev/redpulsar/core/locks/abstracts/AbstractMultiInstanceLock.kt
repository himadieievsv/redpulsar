package com.himadieiev.redpulsar.core.locks.abstracts

import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.core.locks.excecutors.executeWithRetry
import kotlinx.coroutines.CoroutineScope
import java.time.Duration

/**
 * A distributed lock implementation based on the RedLock algorithm.
 * Algorithm depends on single or multiple Redis instances / clusters.
 * It uses a quorum to determine if the lock was acquired.
 * Details: https://redis.io/docs/manual/patterns/distributed-locks/
 */
abstract class AbstractMultiInstanceLock(
    private val backends: List<LocksBackend>,
    private val scope: CoroutineScope,
    private val retryCount: Int = 3,
    private val retryDelay: Duration = Duration.ofMillis(100),
) : AbstractLock() {
    init {
        require(backends.isNotEmpty()) { "Redis instances must not be empty" }
    }

    /**
     * Lock the resource with given name on multiple Redis instances/clusters.
     * @param resourceName [String] the name of the resource for which lock key was created.
     * @param ttl [Duration] the time to live of the lock. Smaller ttl will require better clock synchronization.
     * @return true if lock was acquired, false otherwise.
     */
    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        return backends.executeWithRetry(
            scope = scope,
            timeout = ttl,
            defaultDrift = Duration.ofMillis(3L * backends.size),
            retryCount = retryCount,
            retryDelay = retryDelay,
            cleanUp = { backend ->
                unlockInstance(backend, resourceName)
            },
            callee = { backend ->
                lockInstance(backend, resourceName, ttl)
            },
        ).isNotEmpty()
    }

    /**
     * Unlocks a resource with a given name.
     * @param resourceName [String] the name of the resource for which lock key was created.
     * @return [Boolean] true if the lock was released, false otherwise.
     */
    override fun unlock(resourceName: String): Boolean {
        return backends.executeWithRetry(
            scope = scope,
            timeout = Duration.ofSeconds(1),
            defaultDrift = Duration.ofMillis(3L * backends.size),
            retryCount = retryCount,
            retryDelay = retryDelay,
            callee = { backend ->
                unlockInstance(backend, resourceName)
            },
        ).isNotEmpty()
    }

    protected fun backendSize(): Int = backends.size
}
