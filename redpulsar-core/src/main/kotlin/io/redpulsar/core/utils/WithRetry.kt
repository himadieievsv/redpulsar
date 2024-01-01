package io.redpulsar.core.utils

inline fun <R> withRetry(
    maxRetries: Int = 3,
    block: () -> R?,
): R? {
    var retries = maxRetries
    do {
        val result = block()
        if (result != null) {
            return result
        }
    } while (--retries > 0)
    return null
}
