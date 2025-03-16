package com.himadieiev.redpulsar.core.utils

import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

@Tag(TestTags.UNIT)
class FailsafeTest {
    @Test
    fun `returned value correctly`() {
        val returnValue =
            failsafe(0) {
                "OK"
            }
        assertEquals("OK", returnValue)
    }

    @Test
    fun `supress top level exception`() {
        assertDoesNotThrow {
            failsafe(0) {
                throw Exception("test exception")
            }
        }
    }

    @Test
    fun `throwable is not captured`() {
        assertThrows<Throwable> {
            failsafe(0) {
                throw mockk<Throwable>()
            }
        }
    }
}
