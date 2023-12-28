package io.redpulsar.locks

import equalsTo
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
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
            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"

            val redLock = RedLock(listOf(redis))
            val permit = redLock.lock("test")

            assertFalse(permit)

            verify(exactly = 3) {
                redis.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 3) {
                redis.eval(any<String>(), eq(listOf("test")), any<List<String>>())
            }
        }

        @Test
        fun `lock already taken`() {
            every { redis.set(eq("test"), any(), any()) } returns null
            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"

            val redLock = RedLock(listOf(redis))
            val permit = redLock.lock("test", 1.seconds)

            assertFalse(permit)

            verify(exactly = 3) {
                redis.set(eq("test"), any<String>(), any<SetParams>())
            }
            verify(exactly = 3) {
                redis.eval(any<String>(), eq(listOf("test")), any<List<String>>())
            }
        }

        @Test
        fun `unlock resource`() {
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

        @ParameterizedTest(name = "Validated with retry count - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry count`(retryCount: Int) {
            if (retryCount > 0) {
                Assertions.assertDoesNotThrow { RedLock(listOf(redis), retryCount = retryCount) }
            } else {
                assertThrows<IllegalArgumentException> { RedLock(listOf(redis), retryCount = retryCount) }
            }
        }

        @ParameterizedTest(name = "Validated with retry delly - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry delly`(retryDelly: Int) {
            if (retryDelly > 0) {
                Assertions.assertDoesNotThrow { RedLock(listOf(redis), retryDelay = retryDelly.milliseconds) }
            } else {
                assertThrows<IllegalArgumentException> { RedLock(listOf(redis), retryDelay = retryDelly.milliseconds) }
            }
        }

        @Test
        fun `validate instance count`() {
            Assertions.assertDoesNotThrow { RedLock(listOf(redis)) }
            assertThrows<IllegalArgumentException> { RedLock(listOf()) }
        }

        @ParameterizedTest(name = "lock acquired with ttl - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate ttl`(ttl: Int) {
            every { redis.set(eq("test"), any(), any()) } returns "OK"

            val redLock = RedLock(listOf(redis))
            if (ttl > 2) {
                Assertions.assertDoesNotThrow { redLock.lock("test", ttl.milliseconds) }
            } else {
                assertThrows<IllegalArgumentException> { redLock.lock("test", ttl.milliseconds) }
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
            val instances = listOf(redis1, redis2, redis3)
            instances.forEach { redis -> every { redis.set(eq("test"), any(), any()) } returns "OK" }

            val redLock = RedLock(instances)
            val permit = redLock.lock("test")

            assertTrue(permit)
            verify(exactly = 1) {
                instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
            }
        }

        @Test
        fun `two instances are in quorum`() {
            val instances = listOf(redis1, redis2, redis3)
            every { redis1.set(eq("test"), any(), any()) } returns "OK"
            every { redis2.set(eq("test"), any(), any()) } returns null
            every { redis3.set(eq("test"), any(), any()) } returns "OK"

            val redLock = RedLock(instances)
            val permit = redLock.lock("test")

            assertTrue(permit)
            verify(exactly = 1) {
                instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
            }
            verify(exactly = 0) {
                instances.forEach { redis -> redis.eval(any<String>(), eq(listOf("test")), any<List<String>>()) }
            }
        }

        @Test
        fun `quorum wasn't reach`() {
            val instances = listOf(redis1, redis2, redis3)
            every { redis1.set(eq("test"), any(), any()) } returns null
            every { redis2.set(eq("test"), any(), any()) } returns "OK"
            every { redis3.set(eq("test"), any(), any()) } returns null
            instances.forEach { redis ->
                every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"
            }

            val redLock = RedLock(instances)
            val permit = redLock.lock("test")

            assertFalse(permit)
            verify(exactly = 3) {
                instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
            }
            verify(exactly = 3) {
                instances.forEach { redis -> redis.eval(any<String>(), eq(listOf("test")), any<List<String>>()) }
            }
        }

        @Test
        fun `lock declined due to clock drift`() {
            val instances = listOf(redis1, redis2, redis3)
            every { redis1.set(eq("test"), any(), any()) } returns "OK"
            every { redis2.set(eq("test"), any(), any()) } answers {
                runBlocking { delay(20) }
                "OK"
            }
            every { redis3.set(eq("test"), any(), any()) } returns "OK"
            instances.forEach { redis ->
                every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"
            }

            val redLock = RedLock(instances)
            val permit = redLock.lock("test", 20.milliseconds)

            assertFalse(permit)
            verify(exactly = 3) {
                instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
            }
            verify(exactly = 3) {
                instances.forEach { redis -> redis.eval(any<String>(), eq(listOf("test")), any<List<String>>()) }
            }
        }
    }
}
