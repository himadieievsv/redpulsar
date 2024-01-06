package com.himadieiev.redpulsar.core.locks.abstracts.backends

import com.himadieiev.redpulsar.core.locks.abstracts.Backend
import java.time.Duration

/**
 * An abstraction for underlying storage for distributed locks.
 */
abstract class LocksBackend : Backend() {
    abstract fun setLock(
        resourceName: String,
        clientId: String,
        ttl: Duration,
    ): String?

    abstract fun removeLock(
        resourceName: String,
        clientId: String,
    ): String?

    abstract fun setSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
        maxLeases: Int,
        ttl: Duration,
    ): String?

    abstract fun removeSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
    ): String?

    abstract fun cleanUpExpiredSemaphoreLocks(
        leasersKey: String,
        leaserValidityKeyPrefix: String,
    ): String?
}
