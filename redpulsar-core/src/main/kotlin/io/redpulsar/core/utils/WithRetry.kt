package io.redpulsar.core.utils

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

inline fun <R> withRetry(
    retryCount: Int = 3,
    retryDelay: Duration = 100.milliseconds,
    block: () -> List<R>,
): List<R> {
    var retries = retryCount
    var exponentialDelay = retryDelay.inWholeMilliseconds
    do {
        val result = block()
        if (result.isNotEmpty()) {
            return result
        }
        runBlocking {
            delay(exponentialDelay)
            exponentialDelay *= 2
        }
    } while (--retries > 0)
    return emptyList()
}
