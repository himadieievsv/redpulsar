package me.himadieiev.redpulsar.core.locks

import TestTags
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import me.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.UNIT)
class SimpleLockTest {
    private lateinit var backend: LocksBackend

    @BeforeEach
    fun setUp() {
        backend = mockk<LocksBackend>()
    }

    @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
    @ValueSource(ints = [1, 2, 5, 7, 10])
    fun `lock acquired`(ttl: Int) {
        every { backend.setLock(eq("test"), any(), any()) } returns "OK"

        val simpleLock = SimpleLock(backend)
        val permit = simpleLock.lock("test", ttl.seconds)

        assertTrue(permit)
        verify(exactly = 1) {
            backend.setLock(eq("test"), any(), any())
        }
    }

    @Test
    fun `lock already taken or instance is down`() {
        // every { redis.set(eq("test"), any(), any()) } returns null
        every { backend.setLock(eq("test"), any(), any()) } returns null

        val simpleLock = SimpleLock(backend, retryDelay = 20.milliseconds, retryCount = 3)
        val permit = simpleLock.lock("test", 1.seconds)

        assertFalse(permit)

        verify(exactly = 3) { backend.setLock(eq("test"), any(), any()) }
    }

    @Test
    fun `unlock resource`() {
        every { backend.removeLock(eq("test"), any()) } returns "OK"

        val simpleLock = SimpleLock(backend)
        simpleLock.unlock("test")

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
            assertDoesNotThrow { SimpleLock(backend, retryCount = retryCount) }
        } else {
            assertThrows<IllegalArgumentException> { SimpleLock(backend, retryCount = retryCount) }
        }
    }

    @ParameterizedTest(name = "Validated with retry delay - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
    fun `validate retry delay`(retryDelay: Int) {
        if (retryDelay > 0) {
            assertDoesNotThrow { SimpleLock(backend, retryDelay = retryDelay.milliseconds) }
        } else {
            assertThrows<IllegalArgumentException> { SimpleLock(backend, retryDelay = retryDelay.milliseconds) }
        }
    }

    @ParameterizedTest(name = "lock acquired with ttl - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
    fun `validate ttl`(ttl: Int) {
        every { backend.setLock(eq("test"), any(), any()) } returns "OK"

        val simpleLock = SimpleLock(backend)
        if (ttl > 2) {
            assertDoesNotThrow { simpleLock.lock("test", ttl.milliseconds) }
        } else {
            assertThrows<IllegalArgumentException> { simpleLock.lock("test", ttl.milliseconds) }
        }
    }
}
