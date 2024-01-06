package com.himadieiev.redpulsar.core.utils

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.time.Duration

/**
 * If provided closure returns empty list, it will be retried [retryCount] times
 * with exponentially growing [retryDelay] between each attempt.
 */
inline fun <R> withRetry(
    retryCount: Int = 3,
    retryDelay: Duration = Duration.ofMillis(100),
    block: () -> List<R>,
): List<R> {
    var retries = retryCount
    var exponentialDelay = retryDelay.toMillis()
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
