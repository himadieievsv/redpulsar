package io.redpulsar.core.locks.abstracts

import kotlin.time.Duration

abstract class Backend {
    internal abstract fun setLock(resourceName: String, clientId: String, ttl: Duration): String?
    internal abstract fun removeLock(resourceName: String, clientId: String): String?
    internal abstract fun setSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
        maxLeases: Int,
        ttl: Duration,
    ): String?

    internal abstract fun removeSemaphoreLock(leasersKey: String, leaserValidityKey: String, clientId: String): String?
    internal abstract fun cleanUpExpiredSemaphoreLocks(leasersKey: String, leaserValidityKeyPrefix: String): String?
}
