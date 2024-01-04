package me.himadieiev.redpulsar.core.locks.abstracts

import me.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import me.himadieiev.redpulsar.core.locks.api.Lock
import mu.KotlinLogging
import java.util.UUID
import kotlin.time.Duration

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
    ): Boolean {
        return backend.setLock(resourceName, clientId, ttl) != null
    }

    /**
     * Unlocks the resource on the given Redis instance.
     */
    protected open fun unlockInstance(
        backend: LocksBackend,
        resourceName: String,
    ) {
        backend.removeLock(resourceName, clientId)
    }
}
