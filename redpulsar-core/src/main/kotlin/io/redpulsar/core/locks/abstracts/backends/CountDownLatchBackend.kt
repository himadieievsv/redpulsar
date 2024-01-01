package io.redpulsar.core.locks.abstracts.backends

import io.redpulsar.core.locks.abstracts.Backend
import kotlin.time.Duration

/**
 * An abstraction for underlying storage for distributed count down latch.
 */
abstract class CountDownLatchBackend : Backend() {
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
    ): String?

    abstract fun checkCount(latchKeyName: String): Int?

    /** Receive notification about count down latch is opened now. This is supposed to be a blocking call*/
    abstract fun <T> listen(
        channelName: String,
        messageConsumer: (message: String) -> T,
    ): T?
}
