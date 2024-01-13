package com.himadieiev.redpulsar.core.locks.abstracts.backends

import com.himadieiev.redpulsar.core.locks.abstracts.Backend
import java.time.Duration

/**
 * An abstraction for underlying storage for distributed count down latch.
 */
abstract class CountDownLatchBackend : Backend() {
    /**
     * Ensures that count is idempotent.
     * e.g. calling this method with the same arguments multiple times should not total counts more than once.
     * Also, this method responsible for publishing message to channel if count is reached.
     * Message body that is published to channel should be "open".
     * @param latchKeyName [String] the name of the latch.
     * @param channelName [String] the name of the channel.
     * @param clientId [String] the id of the client.
     * @param count [Int] the number of current counter state.
     * @param initialCount [Int] the number of times [countDown] must be invoked before threads can pass through [await].
     * @param ttl [Duration] the maximum time to wait.
     * @return [String] the result of operation. If result is null, then operation failed.
     */
    abstract fun count(
        latchKeyName: String,
        channelName: String,
        clientId: String,
        count: Int,
        initialCount: Int,
        ttl: Duration,
    ): String?

    /**
     * Undo count() operation.
     * @param latchKeyName [String] the name of the latch.
     * @param clientId [String] the id of the client.
     * @param count [Int] the number of current counter state.
     * @return [Long] the result of operation. If result is null, then operation failed.
     */
    abstract fun undoCount(
        latchKeyName: String,
        clientId: String,
        count: Int,
    ): Long?

    /**
     * Check current count of the latch.
     * @param latchKeyName [String] the name of the latch.
     * @return [Long] the result of operation. If result is null, then operation failed.
     */
    abstract fun checkCount(latchKeyName: String): Long?

    /**
     * Receive notification about count down latch is opened now.
     * @param channelName [String] the name of the channel.
     * @return [String] the message body. If result is null, then operation failed.
     */
    abstract suspend fun listen(channelName: String): String?
}
