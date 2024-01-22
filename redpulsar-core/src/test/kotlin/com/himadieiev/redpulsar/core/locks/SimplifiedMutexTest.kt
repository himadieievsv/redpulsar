package com.himadieiev.redpulsar.core.locks

import TestTags
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration

@Tag(TestTags.UNIT)
class SimplifiedMutexTest {
    private lateinit var backend: LocksBackend

    @BeforeEach
    fun setUp() {
        backend = mockk<LocksBackend>()
    }

    @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
    @ValueSource(ints = [1, 2, 5, 7, 10])
    fun `lock acquired`(ttl: Long) {
        every { backend.setLock(eq("test"), any(), eq(Duration.ofSeconds(ttl))) } returns "OK"

        val simplifiedMutex = SimplifiedMutex(backend)
        val permit = simplifiedMutex.lock("test", Duration.ofSeconds(ttl))

        assertTrue(permit)
        verify(exactly = 1) {
            backend.setLock(eq("test"), any(), any())
        }
    }

    @Test
    fun `lock already taken or instance is down`() {
        // every { redis.set(eq("test"), any(), any()) } returns null
        every { backend.setLock(eq("test"), any(), eq(Duration.ofSeconds(1))) } returns null

        val simplifiedMutex = SimplifiedMutex(backend, retryDelay = Duration.ofMillis(20), retryCount = 3)
        val permit = simplifiedMutex.lock("test", Duration.ofSeconds(1))

        assertFalse(permit)

        verify(exactly = 3) { backend.setLock(eq("test"), any(), any()) }
    }

    @Test
    fun `unlock resource`() {
        every { backend.removeLock(eq("test"), any()) } returns "OK"

        val simplifiedMutex = SimplifiedMutex(backend)
        simplifiedMutex.unlock("test")

        verify(exactly = 1) {
            backend.removeLock(eq("test"), any())
        }
        verify(exactly = 0) {
            backend.setLock(any(), any(), any())
        }
    }

    @ParameterizedTest(name = "Validated with retry count - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
    fun `validate retry count`(retryCount: Int) {
        if (retryCount > 0) {
            assertDoesNotThrow { SimplifiedMutex(backend, retryCount = retryCount) }
        } else {
            assertThrows<IllegalArgumentException> { SimplifiedMutex(backend, retryCount = retryCount) }
        }
    }

    @ParameterizedTest(name = "Validated with retry delay - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
    fun `validate retry delay`(retryDelay: Long) {
        if (retryDelay > 0) {
            assertDoesNotThrow { SimplifiedMutex(backend, retryDelay = Duration.ofMillis(retryDelay)) }
        } else {
            assertThrows<IllegalArgumentException> { SimplifiedMutex(backend, retryDelay = Duration.ofMillis(retryDelay)) }
        }
    }

    @ParameterizedTest(name = "lock acquired with ttl - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
    fun `validate ttl`(ttl: Long) {
        every { backend.setLock(eq("test"), any(), any()) } returns "OK"

        val simplifiedMutex = SimplifiedMutex(backend)
        if (ttl > 2) {
            assertDoesNotThrow { simplifiedMutex.lock("test", Duration.ofMillis(ttl)) }
        } else {
            assertThrows<IllegalArgumentException> { simplifiedMutex.lock("test", Duration.ofMillis(ttl)) }
        }
    }
}
