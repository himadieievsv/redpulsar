package io.redpulsar.core.locks.abstracts

import kotlin.time.Duration

/**
 * An abstraction for underlying storage for distributed locks.
 */
abstract class LocksBackend {
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

    protected fun convertToString(result: Any?): String? =
        when (result) {
            is String -> result
            is Any -> result.toString()
            else -> (null)
        }
}
