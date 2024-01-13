package com.himadieiev.redpulsar.core.locks.abstracts.backends

import com.himadieiev.redpulsar.core.locks.abstracts.Backend
import java.time.Duration

/**
 * An abstraction for underlying storage for distributed locks.
 */
abstract class LocksBackend : Backend() {
    /**
     * Set lock on the resource with given name.
     * @param resourceName [String] the name of the resource.
     * @param clientId [String] the id of the client.
     * @param ttl [Duration] the maximum time to wait.
     * @return [String] the result of operation. If result is null, then operation failed.
     */
    abstract fun setLock(
        resourceName: String,
        clientId: String,
        ttl: Duration,
    ): String?

    /**
     * Remove lock on the resource with given name.
     * @param resourceName [String] the name of the resource.
     * @param clientId [String] the id of the client.
     * @return [String] the result of operation. If result is null, then operation failed.
     */
    abstract fun removeLock(
        resourceName: String,
        clientId: String,
    ): String?

    /**
     * Set semaphore lock on the resource with given name.
     * @param leasersKey [String] the name of the leasers key.
     * @param leaserValidityKey [String] the name of the leaser validity key.
     * @param clientId [String] the id of the client.
     * @param maxLeases [Int] the maximum number of leases.
     * @param ttl [Duration] the maximum time to wait.
     * @return [String] the result of operation. If result is null, then operation failed.
     */
    abstract fun setSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
        maxLeases: Int,
        ttl: Duration,
    ): String?

    /**
     * Remove semaphore lock on the resource with given name.
     * @param leasersKey [String] the name of the leasers key.
     * @param leaserValidityKey [String] the name of the leaser validity key.
     * @param clientId [String] the id of the client.
     * @return [String] the result of operation. If result is null, then operation failed.
     */
    abstract fun removeSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
    ): String?

    /**
     * Clean up expired locks.
     * @param leasersKey [String] the name of the leasers key.
     * @param leaserValidityKeyPrefix [String] the name of the leaser validity key prefix.
     * @return [String] the result of operation. If result is null, then operation failed.
     */
    abstract fun cleanUpExpiredSemaphoreLocks(
        leasersKey: String,
        leaserValidityKeyPrefix: String,
    ): String?
}
