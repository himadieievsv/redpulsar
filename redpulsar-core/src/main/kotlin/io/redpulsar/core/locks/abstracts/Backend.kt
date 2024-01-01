package io.redpulsar.core.locks.abstracts

/**
 * Utility abstraction for backends.
 */
abstract class Backend {
    protected fun convertToString(result: Any?): String? =
        when (result) {
            is String -> result
            is Any -> result.toString()
            else -> (null)
        }
}
