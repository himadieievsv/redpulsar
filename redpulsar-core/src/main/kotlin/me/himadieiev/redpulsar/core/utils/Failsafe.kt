package me.himadieiev.redpulsar.core.utils

import mu.KotlinLogging

/**
 * Catch and supress all types of exceptions.
 * If [block] failed to run with exception, default value will be returned.
 */
inline fun <R> failsafe(
    defaultRerunValue: R,
    block: () -> R,
): R {
    return try {
        block()
    } catch (e: Exception) {
        val logger = KotlinLogging.logger {}
        logger.error(e) { "Failsafe suppressed exception." }
        defaultRerunValue
    }
}
