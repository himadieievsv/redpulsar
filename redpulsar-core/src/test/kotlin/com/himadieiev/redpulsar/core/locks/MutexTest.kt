package com.himadieiev.redpulsar.core.locks

import TestTags
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
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
import java.time.Duration

@Tag(TestTags.UNIT)
class MutexTest {
    @Nested
    inner class SingleRedisInstance {
        private lateinit var backend: LocksBackend

        @BeforeEach
        fun setUp() {
            backend = mockk<LocksBackend>()
        }

        @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
        @ValueSource(ints = [1, 2, 5, 7, 10])
        fun `lock acquired`(ttl: Long) {
            every { backend.setLock(eq("test"), any(), eq(Duration.ofSeconds(ttl))) } returns "OK"

            val mutex = Mutex(listOf(backend))
            val permit = mutex.lock("test", Duration.ofSeconds(ttl))

            assertTrue(permit)
            verify(exactly = 1) { backend.setLock(any(), any(), any()) }
            verify(exactly = 0) { backend.removeLock(any(), any()) }
        }

        @Test
        fun `lock already taken or instance is down`() {
            every { backend.setLock(eq("test"), any(), eq(Duration.ofSeconds(10))) } returns null
            every { backend.removeLock(eq("test"), any()) } returns "OK"

            val mutex = Mutex(listOf(backend), retryCount = 3, retryDelay = Duration.ofMillis(20))
            val permit = mutex.lock("test")

            assertFalse(permit)

            verify(exactly = 3) {
                backend.setLock(any(), any(), any())
                backend.removeLock(any(), any())
            }
        }

        @Test
        fun `unlock resource`() {
            every { backend.removeLock(eq("test"), any()) } returns "OK"

            val mutex = Mutex(listOf(backend))
            // It cant be guarantied that the lock was actually acquired
            mutex.unlock("test")

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
                Assertions.assertDoesNotThrow { Mutex(listOf(backend), retryCount = retryCount) }
            } else {
                assertThrows<IllegalArgumentException> { Mutex(listOf(backend), retryCount = retryCount) }
            }
        }

        @ParameterizedTest(name = "Validated with retry delay - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry delay`(retryDelay: Long) {
            if (retryDelay > 0) {
                Assertions.assertDoesNotThrow { Mutex(listOf(backend), retryDelay = Duration.ofMillis(retryDelay)) }
            } else {
                assertThrows<IllegalArgumentException> {
                    Mutex(listOf(backend), retryDelay = Duration.ofMillis(retryDelay))
                }
            }
        }

        @Test
        fun `validate instance count`() {
            Assertions.assertDoesNotThrow { Mutex(listOf(backend)) }
            assertThrows<IllegalArgumentException> { Mutex(listOf()) }
        }

        @ParameterizedTest(name = "lock acquired with ttl - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate ttl`(ttl: Long) {
            every { backend.setLock(eq("test"), any(), eq(Duration.ofMillis(ttl))) } returns "OK"
            // validity can be rejected with tiny ttl
            every { backend.removeLock(eq("test"), any()) } returns "OK"

            val mutex = Mutex(listOf(backend))
            if (ttl > 2) {
                Assertions.assertDoesNotThrow { mutex.lock("test", Duration.ofMillis(ttl)) }
            } else {
                assertThrows<IllegalArgumentException> { mutex.lock("test", Duration.ofMillis(ttl)) }
            }
        }
    }

    @Nested
    inner class MultipleRedisInstance {
        private lateinit var backend1: LocksBackend
        private lateinit var backend2: LocksBackend
        private lateinit var backend3: LocksBackend
        private lateinit var instances: List<LocksBackend>

        @BeforeEach
        fun setUp() {
            backend1 = mockk<LocksBackend>()
            backend2 = mockk<LocksBackend>()
            backend3 = mockk<LocksBackend>()
            instances = listOf(backend1, backend2, backend3)
        }

        @Test
        fun `all instances are in quorum`() {
            instances.forEach { backend ->
                every {
                    backend.setLock(eq("test"), any(), any())
                } returns "OK"
            }

            val mutex = Mutex(instances)
            val permit = mutex.lock("test")

            assertTrue(permit)
            verify(exactly = 1) {
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 0) {
                instances.forEach { backend -> backend.removeLock(any(), any()) }
            }
        }

        @Test
        fun `two instances are in quorum`() {
            every { backend1.setLock(eq("test"), any(), any()) } returns "OK"
            every { backend2.setLock(eq("test"), any(), any()) } returns null
            every { backend3.setLock(eq("test"), any(), any()) } returns "OK"

            val mutex = Mutex(instances)
            val permit = mutex.lock("test")

            assertTrue(permit)
            verify(exactly = 1) {
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 0) {
                instances.forEach { backend -> backend.removeLock(any(), any()) }
            }
        }

        @Test
        fun `quorum wasn't reach`() {
            every { backend1.setLock(eq("test"), any(), eq(Duration.ofSeconds(10))) } returns null
            every { backend2.setLock(eq("test"), any(), eq(Duration.ofSeconds(10))) } returns "OK"
            every { backend3.setLock(eq("test"), any(), eq(Duration.ofSeconds(10))) } returns null
            instances.forEach { backend ->
                every { backend.removeLock(eq("test"), any()) } returns "OK"
            }

            val mutex = Mutex(instances, retryCount = 3, retryDelay = Duration.ofMillis(20))
            val permit = mutex.lock("test")

            assertFalse(permit)
            verify(exactly = 3) {
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 3) {
                instances.forEach { backend -> backend.removeLock(eq("test"), any()) }
            }
        }

        @Test
        fun `lock acquired after retry`() {
            every { backend1.setLock(eq("test"), any(), any()) } returnsMany listOf(null, "OK")
            every { backend2.setLock(eq("test"), any(), any()) } returns "OK"
            every { backend3.setLock(eq("test"), any(), any()) } returnsMany listOf(null, "OK")
            instances.forEach { backend ->
                every { backend.removeLock(eq("test"), any()) } returns "OK"
            }

            val mutex = Mutex(instances, retryCount = 3, retryDelay = Duration.ofMillis(1))
            val permit = mutex.lock("test")

            assertTrue(permit)
            verify(exactly = 2) {
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            // first attempt missed the quorum and partial locks were rolled back
            verify(exactly = 1) {
                instances.forEach { backend -> backend.removeLock(eq("test"), any()) }
            }
        }

        @Test
        fun `lock declined due to clock drift`() {
            every { backend1.setLock(eq("test"), any(), any()) } returns "OK"
            every { backend2.setLock(eq("test"), any(), any()) } answers {
                runBlocking { delay(30) }
                "OK"
            }
            every { backend3.setLock(eq("test"), any(), any()) } returns "OK"
            instances.forEach { backend ->
                every { backend.removeLock(eq("test"), any()) } returns "OK"
            }

            val mutex = Mutex(instances, retryCount = 3, retryDelay = Duration.ofMillis(30))
            val permit = mutex.lock("test", Duration.ofMillis(30))

            assertFalse(permit)
            verify(exactly = 3) {
                instances.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 3) {
                instances.forEach { backend -> backend.removeLock(eq("test"), any()) }
            }
        }
    }

    @Nested
    inner class QuorumAndTtlBoundaries {
        @ParameterizedTest(name = "{0} backends, quorum minus one success does not acquire lock")
        @ValueSource(ints = [2, 4, 5, 7, 10])
        fun `quorum minus one success does not acquire lock`(number: Int) {
            val backends = (1..number).map { mockk<LocksBackend>() }
            val quorum = number / 2 + 1
            backends.forEachIndexed { index, backend ->
                every {
                    backend.setLock(eq("test"), any(), any())
                } returns if (index < quorum - 1) "OK" else null
                every { backend.removeLock(eq("test"), any()) } returns "OK"
            }

            val mutex = Mutex(backends, retryCount = 1, retryDelay = Duration.ofMillis(1))
            val permit = mutex.lock("test", Duration.ofSeconds(10))

            assertFalse(permit)
            verify(exactly = 1) {
                backends.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 1) {
                backends.forEach { backend -> backend.removeLock(eq("test"), any()) }
            }
        }

        @ParameterizedTest(name = "{0} backends, exactly quorum success acquires lock")
        @ValueSource(ints = [2, 4, 5, 7, 10])
        fun `exactly quorum success acquires lock`(number: Int) {
            val backends = (1..number).map { mockk<LocksBackend>() }
            val quorum = number / 2 + 1
            backends.forEachIndexed { index, backend ->
                every {
                    backend.setLock(eq("test"), any(), any())
                } returns if (index < quorum) "OK" else null
            }

            val mutex = Mutex(backends, retryCount = 1, retryDelay = Duration.ofMillis(1))
            val permit = mutex.lock("test", Duration.ofSeconds(10))

            assertTrue(permit)
            verify(exactly = 1) {
                backends.forEach { backend -> backend.setLock(eq("test"), any(), any()) }
            }
            verify(exactly = 0) {
                backends.forEach { backend -> backend.removeLock(any(), any()) }
            }
        }

        @ParameterizedTest(name = "{0} backends, ttl equal to min clock drift is rejected")
        @ValueSource(ints = [1, 3, 5, 7])
        fun `ttl equal to min clock drift is rejected`(number: Int) {
            val backends = (1..number).map { mockk<LocksBackend>() }

            val mutex = Mutex(backends)
            assertThrows<IllegalArgumentException> { mutex.lock("test", Duration.ofMillis(3L * number)) }
        }

        @ParameterizedTest(name = "{0} backends, ttl above min clock drift is accepted")
        @ValueSource(ints = [1, 3, 5, 7])
        fun `ttl above min clock drift is accepted`(number: Int) {
            val backends = (1..number).map { mockk<LocksBackend>() }
            backends.forEach { backend ->
                every {
                    backend.setLock(eq("test"), any(), any())
                } returns "OK"
                every { backend.removeLock(eq("test"), any()) } returns "OK"
            }

            val mutex = Mutex(backends)
            val permit = mutex.lock("test", Duration.ofMillis(3L * number + 100))

            assertTrue(permit)
        }
    }
}
