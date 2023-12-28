package io.redpulsar.locks.core

import equalsTo
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.redpulsar.locks.RedLock
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.params.SetParams
import java.io.IOException
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.UNIT)
class RedLockTest {
    @Nested
    inner class SingleRedisInstance {
        private lateinit var redis: UnifiedJedis

        @BeforeEach
        fun setUp() {
            redis = mockk<UnifiedJedis>()
        }

        @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
        @ValueSource(ints = [1, 2, 5, 7, 10])
        fun `lock acquired`(ttl: Int) {
            every { redis.set(eq("test"), any(), any()) } returns "OK"

            val redLock = RedLock(listOf(redis))
            val permit = redLock.lock("test", ttl.seconds)

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

            val redLock = RedLock(listOf(redis))
            val permit = redLock.lock("test")

            assertFalse(permit)

            verify(exactly = 3) {
                redis.set(eq("test"), any<String>(), any<SetParams>())
            }
        }

        @Test
        fun `lock already taken`() {
            every { redis.set(eq("test"), any(), any()) } returns null

            val redLock = RedLock(listOf(redis))
            val permit = redLock.lock("test", 1.seconds)

            assertFalse(permit)

            verify(exactly = 3) {
                redis.set(eq("test"), any<String>(), any<SetParams>())
            }
        }

        @Test
        fun `unlock resource`() {
            val redis = mockk<UnifiedJedis>()
            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"

            val redLock = RedLock(listOf(redis))
            // It cant be guarantied that the lock was actually acquired
            redLock.unlock("test")

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
    }

    @Nested
    inner class MultipleRedisInstance {
        private lateinit var redis1: UnifiedJedis
        private lateinit var redis2: UnifiedJedis
        private lateinit var redis3: UnifiedJedis

        @BeforeEach
        fun setUp() {
            redis1 = mockk<UnifiedJedis>()
            redis2 = mockk<UnifiedJedis>()
            redis3 = mockk<UnifiedJedis>()
        }

        @Test
        fun `all instances are in quorum`() {
            every { redis1.set(eq("test"), any(), any()) } returns "OK"
            every { redis2.set(eq("test"), any(), any()) } returns "OK"
            every { redis3.set(eq("test"), any(), any()) } returns "OK"

            val redLock = RedLock(listOf(redis1, redis2, redis3))
            val permit = redLock.lock("test")

            assertTrue(permit)
            verify(exactly = 1) {
                redis1.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 1) {
                redis2.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 1) {
                redis3.set(eq("test"), any<String>(), any<SetParams>())
            }
        }

        @Test
        fun `two instances are in quorum`() {
            every { redis1.set(eq("test"), any(), any()) } returns "OK"
            every { redis2.set(eq("test"), any(), any()) } returns null
            every { redis3.set(eq("test"), any(), any()) } returns "OK"

            val redLock = RedLock(listOf(redis1, redis2, redis3))
            val permit = redLock.lock("test")

            assertTrue(permit)
            verify(exactly = 1) {
                redis1.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 1) {
                redis2.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 1) {
                redis3.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 0) {
                redis1.eval(any<String>(), eq(listOf("test")), any<List<String>>())
            }
            verify(exactly = 0) {
                redis2.eval(any<String>(), eq(listOf("test")), any<List<String>>())
            }
            verify(exactly = 0) {
                redis2.eval(any<String>(), eq(listOf("test")), any<List<String>>())
            }
        }

        @Test
        fun `quorum wasn't reach`() {
            every { redis1.set(eq("test"), any(), any()) } returns null
            every { redis2.set(eq("test"), any(), any()) } returns "OK"
            every { redis3.set(eq("test"), any(), any()) } returns null

            val redLock = RedLock(listOf(redis1, redis2, redis3))
            val permit = redLock.lock("test")

            assertFalse(permit)
            verify(exactly = 3) {
                redis1.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 3) {
                redis2.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 3) {
                redis3.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 3) {
                redis1.eval(any<String>(), eq(listOf("test")), any<List<String>>())
            }
            verify(exactly = 3) {
                redis2.eval(any<String>(), eq(listOf("test")), any<List<String>>())
            }
            verify(exactly = 3) {
                redis2.eval(any<String>(), eq(listOf("test")), any<List<String>>())
            }
        }
    }
}
