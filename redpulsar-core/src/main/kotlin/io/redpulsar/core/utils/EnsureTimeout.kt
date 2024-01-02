package io.redpulsar.core.utils

import mu.KotlinLogging

fun <T> ensureTimeout(
    timeoutMillis: Long,
    block: () -> T,
): T? {
    val logger = KotlinLogging.logger {}
    return try {
        val currentThread = Thread.currentThread()
        val timeoutControlThread =
            Thread {
                try {
                    Thread.sleep(timeoutMillis)
                    currentThread.interrupt()
                } catch (e: InterruptedException) {
                    logger.debug { "Timeout control thread interrupted." }
                }
            }
        timeoutControlThread.start()
        val result = block()
        timeoutControlThread.interrupt()
        result
    } catch (e: InterruptedException) {
        logger.warn { "Exiting due to timeout: $timeoutMillis." }
        null
    }
}
