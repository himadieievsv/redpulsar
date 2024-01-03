package io.redpulsar.core.locks.abstracts.backends

import io.redpulsar.core.locks.abstracts.Backend
import kotlinx.coroutines.flow.Flow
import kotlin.time.Duration

/**
 * An abstraction for underlying storage for distributed count down latch.
 */
abstract class CountDownLatchBackend : Backend() {
    /**
     * Ensures that count is idempotent.
     * e.g. calling this method with the same arguments multiple times should not total counts more than once.
     * Also, this method responsible for publishing message to channel if count is reached.
     * Message body that is published to channel should be "open".
     */
    abstract fun count(
        latchKeyName: String,
        channelName: String,
        clientId: String,
        count: Int,
        initialCount: Int,
        ttl: Duration,
    ): String?

    abstract fun undoCount(
        latchKeyName: String,
        clientId: String,
        count: Int,
    ): Long?

    abstract fun checkCount(latchKeyName: String): Long?

    /** Receive notification about count down latch is opened now. This is supposed to be a blocking call*/
    abstract fun listen(channelName: String): Flow<String>
}
