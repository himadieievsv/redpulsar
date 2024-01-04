package io.redpulsar.core.locks.api

import kotlin.time.Duration

/**
 * A distributed locking mechanics, intended for synchronization of multiple workloads distributed across cloud.
 * Allows one or more workloads to wait until a set of other workload are get ready or finished.
 * Example: we need to wait until downstream service is ready to accept requests.
 */
interface CountDownLatch {
    /**
     * Decrements the count of the latch.
     * @return [CallResult] SUCCESS or FAILED. SUCCESS means that the count was decremented on majority
     * of underling instances. And FAILED means that majority of underling instances are down or other network issue.
     */
    fun countDown(): CallResult

    /**
     * Block exception in current thread until the count of the latch is zero.
     * @return [CallResult] SUCCESS or FAILED. SUCCESS means that the count was decremented on majority
     * of underling instances. And FAILED means that majority of underling instances are down or other network issue.
     */
    fun await(): CallResult

    /**
     * Block exception in current thread until the count of the latch is zero with timeout.
     * @param timeout [Duration] the maximum time to wait.
     * @return [CallResult] SUCCESS or FAILED. SUCCESS means that the count was decremented on majority
     * of underling instances. And FAILED means that timeout is reached or majority of underling instances are down
     * or other network issue.
     */
    fun await(timeout: Duration): CallResult

    /**
     * Returns the current latch count.
     * @return [Int] the current count.
     */
    fun getCount(): Int
}

enum class CallResult {
    SUCCESS,
    FAILED,
}
