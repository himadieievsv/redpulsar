package io.redpulsar.core.utils

import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.system.measureTimeMillis

class WithTimeoutInThreadTest {
    private val logger = KotlinLogging.logger {}

    // Acceptable clock error
    private val epsilon = 50L

    @Test
    fun `block ends faster that timeout`() {
        val timeDiff =
            measureTimeMillis {
                val result =
                    withTimeoutInThread(1000) {
                        Thread.sleep(100)
                        "OK"
                    }
                assertEquals("OK", result)
            }
        logger.info { "Time diff: $timeDiff" }
        val clockDrift = timeDiff - 100
        assertTrue(clockDrift < epsilon)
    }

    @Test
    fun `block ends after timeout`() {
        val timeDiff =
            measureTimeMillis {
                val result =
                    withTimeoutInThread(100) {
                        Thread.sleep(1000)
                        "OK"
                    }
                assertNull(result)
            }
        logger.info { "Time diff: $timeDiff" }
        assertTrue(timeDiff < 100 + epsilon)
    }
}
