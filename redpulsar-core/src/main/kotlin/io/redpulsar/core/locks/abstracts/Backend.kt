package io.redpulsar.core.locks.abstracts

import io.redpulsar.core.utils.failsafe
import kotlinx.coroutines.flow.Flow
import java.util.NoSuchElementException

/**
 * Utility abstraction for backends.
 *
 * Typically, backend methods should not throw exceptions and use [failsafe].
 * However, if methods will return [Flow], such methods should be wrapped in [failsafe]
 * as collecting elements can throw [NoSuchElementException].
 */
abstract class Backend {
    protected fun convertToString(result: Any?): String? =
        when (result) {
            is String -> result
            is Any -> result.toString()
            else -> (null)
        }
}
