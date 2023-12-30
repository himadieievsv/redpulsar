package io.redpulsar.core.locks

import TestTags

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.redpulsar.core.locks.abstracts.Backend
import kotlinx.coroutines.CancellationException
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
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.UNIT)
class RedLockTest {
    @Nested
    inner class SingleRedisInstance {
        private lateinit var backend: Backend

        @BeforeEach
        fun setUp() {
            backend = mockk<Backend>()
        }

        @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
        @ValueSource(ints = [1, 2, 5, 7, 10])
        fun `lock acquired`(ttl: Int) {
            // every { redis.set(eq("test"), any(), any()) } returns "OK"
            every { backend.setLock(eq("test"), any(), any()) } returns "OK"


            val redLock = RedLock(listOf(backend))
            val permit = redLock.lock("test", ttl.seconds)

            assertTrue(permit)
//            verify(exactly = 1) {
//                redis.set(
//                    eq("test"),
//                    any<String>(),
//                    match<SetParams> {
//                        it.equalsTo(SetParams().nx().px(ttl.seconds.inWholeMilliseconds))
//                    },
//                )
//            }
            verify(exactly = 1) { backend.setLock(eq("test"), any(), any()) }
            verify(exactly = 0) { backend.removeLock(any(), any()) }
        }

        @Test
        fun `lock already taken or instance is down`() {
//            every { redis.set(eq("test"), any(), any()) } throws IOException()
//            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"
            every { backend.setLock(eq("test"), any(), any()) } returns null
            every { backend.removeLock(eq("test"), any()) } returns "OK"

            val redLock = RedLock(listOf(backend))
            val permit = redLock.lock("test")

            assertFalse(permit)

            verify(exactly = 3) {
                // redis.set(eq("test"), any<String>(), any<SetParams>())
                backend.setLock(eq("test"), any(), any())
                backend.removeLock(eq("test"), any())
            }
//            verify(exactly = 3) {
//                redis.eval(any<String>(), eq(listOf("test")), any<List<String>>())
//            }
        }

        @Test
        fun `lock throws exception`() {
            every { backend.setLock(eq("test"), any(), any()) } throws RuntimeException()

            val redLock = RedLock(listOf(backend))
            val permit = redLock.lock("test")

            assertFalse(permit)

            verify(exactly = 1) { backend.setLock(eq("test"), any(), any()) }
            verify(exactly = 0) { backend.removeLock(any(), any()) }
        }

        @Test
        fun `unlock throws exception`() {
            every { backend.removeLock(eq("test"), any()) } throws RuntimeException()

            val redLock = RedLock(listOf( backend))
            redLock.unlock("test")

            verify(exactly = 0) { backend.setLock(eq("test"), any(), any()) }
            verify(exactly = 1) { backend.removeLock(any(), any()) }
        }

//        @Test
//        fun `lock already taken`() {
//            every { redis.set(eq("test"), any(), any()) } returns null
//            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"
//
//            val redLock = RedLock(listOf(redis))
//            val permit = redLock.lock("test", 1.seconds)
//
//            assertFalse(permit)
//
//            verify(exactly = 3) {
//                redis.set(eq("test"), any<String>(), any<SetParams>())
//            }
//            verify(exactly = 3) {
//                redis.eval(any<String>(), eq(listOf("test")), any<List<String>>())
//            }
//        }

        @Test
        fun `unlock resource`() {
            // every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"
            every { backend.removeLock(eq("test"), any()) } returns "OK"

            val redLock = RedLock(listOf(backend))
            // It cant be guarantied that the lock was actually acquired
            redLock.unlock("test")

            verify(exactly = 1) {
//                redis.eval(
//                    any<String>(),
//                    eq(listOf("test")),
//                    any<List<String>>(),
//                )
                backend.removeLock(eq("test"), any())
            }
            verify(exactly = 0) {
                //redis.set(any<String>(), any(), any())
                backend.setLock(any(), any(), any())
            }
        }

        @ParameterizedTest(name = "Validated with retry count - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry count`(retryCount: Int) {
            if (retryCount > 0) {
                Assertions.assertDoesNotThrow { RedLock(listOf(backend), retryCount = retryCount) }
            } else {
                assertThrows<IllegalArgumentException> { RedLock(listOf(backend), retryCount = retryCount) }
            }
        }

        @ParameterizedTest(name = "Validated with retry delly - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry delly`(retryDelly: Int) {
            if (retryDelly > 0) {
                Assertions.assertDoesNotThrow { RedLock(listOf(backend), retryDelay = retryDelly.milliseconds) }
            } else {
                assertThrows<IllegalArgumentException> {
                    RedLock(listOf(backend), retryDelay = retryDelly.milliseconds)
                }
            }
        }

        @Test
        fun `validate instance count`() {
            Assertions.assertDoesNotThrow { RedLock(listOf(backend)) }
            assertThrows<IllegalArgumentException> { RedLock(listOf()) }
        }

        @ParameterizedTest(name = "lock acquired with ttl - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate ttl`(ttl: Int) {
            //every { redis.set(eq("test"), any(), any()) } returns "OK"
            every { backend.setLock(eq("test"), any(), eq(ttl.milliseconds)) } returns "OK"

            val redLock = RedLock(listOf(backend))
            if (ttl > 2) {
                Assertions.assertDoesNotThrow { redLock.lock("test", ttl.milliseconds) }
            } else {
                assertThrows<IllegalArgumentException> { redLock.lock("test", ttl.milliseconds) }
            }
        }
    }

    @Nested
    inner class MultipleRedisInstance {
        private lateinit var backend1: Backend
        private lateinit var backend2: Backend
        private lateinit var backend3: Backend
        private lateinit var instances: List<Backend>

        @BeforeEach
        fun setUp() {
            backend1 = mockk<Backend>()
            backend2 = mockk<Backend>()
            backend3 = mockk<Backend>()
            instances = listOf(backend1, backend2, backend3)
        }

        @Test
        fun `all instances are in quorum`() {
            //instances.forEach { redis -> every { redis.set(eq("test"), any(), any()) } returns "OK" }
            instances.forEach { backend ->
                every {
                    backend.setLock(eq("test"), any(), any())
                } returns "OK"
            }

            val redLock = RedLock(instances)
            val permit = redLock.lock("test")

            assertTrue(permit)
            verify(exactly = 1) {
                // instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 0) {
                // instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
                instances.forEach { backend -> backend.removeLock(any(), any()) }
            }
        }

        @Test
        fun `two instances are in quorum`() {
//            every { redis1.set(eq("test"), any(), any()) } returns "OK"
//            every { redis2.set(eq("test"), any(), any()) } returns null
//            every { redis3.set(eq("test"), any(), any()) } returns "OK"
            every { backend1.setLock(eq("test"), any(), any()) } returns "OK"
            every { backend2.setLock(eq("test"), any(), any()) } returns null
            every { backend3.setLock(eq("test"), any(), any()) } returns "OK"

            val redLock = RedLock(instances)
            val permit = redLock.lock("test")

            assertTrue(permit)
            verify(exactly = 1) {
                // instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 0) {
                // instances.forEach { redis -> redis.eval(any<String>(), eq(listOf("test")), any<List<String>>()) }
                instances.forEach { backend -> backend.removeLock(any(), any()) }
            }
        }

        @Test
        fun `quorum wasn't reach`() {
//            every { redis1.set(eq("test"), any(), any()) } returns null
//            every { redis2.set(eq("test"), any(), any()) } returns "OK"
//            every { redis3.set(eq("test"), any(), any()) } returns null
            every { backend1.setLock(eq("test"), any(), any()) } returns null
            every { backend2.setLock(eq("test"), any(), any()) } returns "OK"
            every { backend3.setLock(eq("test"), any(), any()) } returns null
            instances.forEach { backend ->
                every { backend.removeLock(eq("test"), any()) } returns "OK"
            }

            val redLock = RedLock(instances)
            val permit = redLock.lock("test")

            assertFalse(permit)
            verify(exactly = 3) {
                // instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 3) {
                //instances.forEach { redis -> redis.eval(any<String>(), eq(listOf("test")), any<List<String>>()) }
                instances.forEach { backend -> backend.removeLock(eq("test"), any()) }
            }
        }

        @Test
        fun `lock declined due to clock drift`() {
//            every { redis1.set(eq("test"), any(), any()) } returns "OK"
//            every { redis2.set(eq("test"), any(), any()) } answers {
//                runBlocking { delay(20) }
//                "OK"
//            }
//            every { redis3.set(eq("test"), any(), any()) } returns "OK"
            every { backend1.setLock(eq("test"), any(), any()) } returns "OK"
            every { backend2.setLock(eq("test"), any(), any()) } answers {
                runBlocking { delay(20) }
                "OK"
            }
            every { backend3.setLock(eq("test"), any(), any()) } returns "OK"
            instances.forEach { backend ->
                every { backend.removeLock(eq("test"), any()) } returns "OK"
            }

            val redLock = RedLock(instances)
            val permit = redLock.lock("test", 20.milliseconds)

            assertFalse(permit)
            verify(exactly = 3) {
                // instances.forEach { redis -> redis.set(eq("test"), any<String>(), any<SetParams>()) }
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 3) {
                // instances.forEach { redis -> redis.eval(any<String>(), eq(listOf("test")), any<List<String>>()) }
                instances.forEach { backend -> backend.removeLock(eq("test"), any()) }
            }
        }
    }
}
