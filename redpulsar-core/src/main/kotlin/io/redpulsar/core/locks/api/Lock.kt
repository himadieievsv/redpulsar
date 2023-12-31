package io.redpulsar.core.locks.api

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A distributed lock api. It is a simple interface that allows to lock and unlock a resource.
 */
interface Lock {
    /**
     * Locks a resource with a given name.
     * @param resourceName [String] the name of the resource to use for create lock key.
     * @param ttl [Duration] the time to live of the lock. Smaller ttl will require better clock synchronization
     * between Redis instances.
     * @return [Boolean] true if the lock was acquired, false otherwise.
     */
    fun lock(
        resourceName: String,
        ttl: Duration = 10.seconds,
    ): Boolean

    /**
     * Unlocks a resource with a given name.
     * @param resourceName [String] the name of the resource for which lock key was created.
     */
    fun unlock(resourceName: String)
}
