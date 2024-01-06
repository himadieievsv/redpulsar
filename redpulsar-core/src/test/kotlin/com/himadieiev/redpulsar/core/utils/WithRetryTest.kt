package com.himadieiev.redpulsar.core.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import kotlin.system.measureTimeMillis

@Tag(TestTags.UNIT)
class WithRetryTest {
    @Test
    fun `doesn't retry`() {
        var counter = 0
        val returnVal =
            withRetry(3, Duration.ofMillis(1)) {
                counter++
                listOf("OK")
            }

        assertEquals(1, counter)
        assertEquals(listOf("OK"), returnVal)
    }

    @ParameterizedTest(name = "retry count with ttl - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 10, 11, 12, 20, 40])
    fun `check retry count`(withCount: Int) {
        var counter = 0
        val returnVal =
            withRetry(withCount, Duration.ofNanos(1000)) {
                counter++
                emptyList<Int>()
            }
        if (withCount > 0) {
            assertEquals(withCount, counter)
        } else {
            assertEquals(1, counter)
        }
        assertEquals(emptyList<Int>(), returnVal)
    }

    @Test
    fun `retry with negative delay is ignored`() {
        var counter = 0
        val returnVal =
            withRetry(3, Duration.ofNanos(0)) {
                counter++
                emptyList<Int>()
            }
        assertEquals(3, counter)
        assertEquals(emptyList<Int>(), returnVal)
    }

    @Test
    fun `check exponential delay`() {
        var counter = 0
        val time =
            measureTimeMillis {
                val returnVal =
                    withRetry(4, Duration.ofMillis(50)) {
                        counter++
                        emptyList<Int>()
                    }
                assertEquals(emptyList<Int>(), returnVal)
            }

        assertEquals(4, counter)
        // 50 + 100 + 200 + 400 = 750, 45 - is allowed clock error
        assertTrue(time in 750 - 45..750 + 45)
    }
}
