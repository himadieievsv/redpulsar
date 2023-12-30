package io.redpulsar.core.locks

import TestTags
import equalsTo
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
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.params.SetParams
import java.io.IOException
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.UNIT)
class SimpleLockTest {
    private lateinit var redis: UnifiedJedis

    @BeforeEach
    fun setUp() {
        redis = mockk<UnifiedJedis>()
    }

    @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
    @ValueSource(ints = [1, 2, 5, 7, 10])
    fun `lock acquired`(ttl: Int) {
        every { redis.set(eq("test"), any(), any()) } returns "OK"

        val simpleLock = SimpleLock(redis)
        val permit = simpleLock.lock("test", ttl.seconds)

        assertTrue(permit)
        verify(exactly = 1) {
            redis.set(
                eq("test"),
                any<String>(),
                match<SetParams> {
                    it.equalsTo(SetParams().nx().px(ttl.seconds.inWholeMilliseconds))
                },
            )
        }
    }

    @Test
    fun `instance is down`() {
        every { redis.set(eq("test"), any(), any()) } throws IOException()

        val simpleLock = SimpleLock(redis)
        val permit = simpleLock.lock("test")

        assertFalse(permit)

        verify(exactly = 3) {
            redis.set(eq("test"), any<String>(), any<SetParams>())
        }
    }

    @Test
    fun `lock already taken`() {
        every { redis.set(eq("test"), any(), any()) } returns null

        val simpleLock = SimpleLock(redis)
        val permit = simpleLock.lock("test", 1.seconds)

        assertFalse(permit)

        verify(exactly = 3) {
            redis.set(eq("test"), any<String>(), any<SetParams>())
        }
    }

    @Test
    fun `unlock resource`() {
        val redis = mockk<UnifiedJedis>()
        every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"

        val simpleLock = SimpleLock(redis)
        simpleLock.unlock("test")

        verify(exactly = 1) {
            redis.eval(
                any<String>(),
                eq(listOf("test")),
                any<List<String>>(),
            )
        }
        verify(exactly = 0) {
            redis.set(any<String>(), any(), any())
        }
    }

    @ParameterizedTest(name = "Validated with retry count - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
    fun `validate retry count`(retryCount: Int) {
        if (retryCount > 0) {
            assertDoesNotThrow { SimpleLock(redis, retryCount = retryCount) }
        } else {
            assertThrows<IllegalArgumentException> { SimpleLock(redis, retryCount = retryCount) }
        }
    }

    @ParameterizedTest(name = "Validated with retry delly - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
    fun `validate retry delly`(retryDelly: Int) {
        if (retryDelly > 0) {
            assertDoesNotThrow { SimpleLock(redis, retryDelay = retryDelly.milliseconds) }
        } else {
            assertThrows<IllegalArgumentException> { SimpleLock(redis, retryDelay = retryDelly.milliseconds) }
        }
    }

    @ParameterizedTest(name = "lock acquired with ttl - {0}")
    @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
    fun `validate ttl`(ttl: Int) {
        every { redis.set(eq("test"), any(), any()) } returns "OK"

        val simpleLock = SimpleLock(redis)
        if (ttl > 2) {
            assertDoesNotThrow { simpleLock.lock("test", ttl.milliseconds) }
        } else {
            assertThrows<IllegalArgumentException> { simpleLock.lock("test", ttl.milliseconds) }
        }
    }
}
