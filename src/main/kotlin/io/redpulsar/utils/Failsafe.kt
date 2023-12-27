package io.redpulsar.utils

import mu.KotlinLogging

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

fun failsafe(block: () -> Unit) {
    failsafe(Unit, block)
}
