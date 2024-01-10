package com.himadieiev.redpulsar.core.locks.abstracts

import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.core.locks.api.Lock
import mu.KotlinLogging
import java.time.Duration
import java.util.UUID

/**
 * Common functions for broad range of different locks.
 */
abstract class AbstractLock : Lock {
    protected val logger = KotlinLogging.logger {}
    protected val clientId: String = UUID.randomUUID().toString()

    /**
     * Locks the resource on the given Redis instance.
     */
    protected open fun lockInstance(
        backend: LocksBackend,
        resourceName: String,
        ttl: Duration,
    ): String? {
        return backend.setLock(resourceName, clientId, ttl)
    }

    /**
     * Unlocks the resource on the given Redis instance.
     */
    protected open fun unlockInstance(
        backend: LocksBackend,
        resourceName: String,
    ): String? {
        return backend.removeLock(resourceName, clientId)
    }
}
