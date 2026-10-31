package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
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
class SemaphoreTest {
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
            every {
                backend.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofSeconds(ttl)),
                )
            } returns "OK"

            val semaphore = Semaphore(listOf(backend), 3)
            val permit = semaphore.lock("test", Duration.ofSeconds(ttl))

            assertTrue(permit)
            verify(exactly = 1) {
                backend.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    any(),
                    any(),
                )
            }
            verify(exactly = 0) {
                backend.removeSemaphoreLock(any(), any(), any())
                backend.cleanUpExpiredSemaphoreLocks(any(), any())
            }
        }

        @Test
        fun `lock already taken or instance is down`() {
            every {
                backend.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofSeconds(1)),
                )
            } returns null
            every {
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"), match { it.startsWith("{semaphore:test}:") }, any(),
                )
            } returns "OK"
            // cleaning up
            every {
                backend.cleanUpExpiredSemaphoreLocks(
                    eq("{semaphore:test}:leasers"),
                    eq("{semaphore:test}:"),
                )
            } returns "OK"

            val semaphore = Semaphore(listOf(backend), 3, 4, Duration.ofMillis(15))
            val permit = semaphore.lock("test", Duration.ofSeconds(1))

            assertFalse(permit)

            verify(exactly = 4) {
                backend.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    any(),
                )
                // unlocking
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                )
                // cleaning up
                backend.cleanUpExpiredSemaphoreLocks(
                    eq("{semaphore:test}:leasers"),
                    eq("{semaphore:test}:"),
                )
            }
        }

        @Test
        fun `unlock resource`() {
            every {
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                )
            } returns "OK"
            every {
                backend.cleanUpExpiredSemaphoreLocks(eq("{semaphore:test}:leasers"), eq("{semaphore:test}:"))
            } returns "OK"

            val semaphore = Semaphore(listOf(backend), 3)
            assertTrue(semaphore.unlock("test"))

            verify(exactly = 1) {
                // unlocking
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                )
                // cleaning up
                backend.cleanUpExpiredSemaphoreLocks(eq("{semaphore:test}:leasers"), eq("{semaphore:test}:"))
            }
            verify(exactly = 0) {
                backend.setSemaphoreLock(any(), any(), any(), any(), any())
            }
        }

        @Test
        fun `unlock failed returns false`() {
            every {
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                )
            } returns null
            every {
                backend.cleanUpExpiredSemaphoreLocks(eq("{semaphore:test}:leasers"), eq("{semaphore:test}:"))
            } returns "OK"

            val semaphore = Semaphore(listOf(backend), 3)

            assertFalse(semaphore.unlock("test"))
        }

        @Test
        fun `cleanup failure does not flip unlock result`() {
            every {
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                )
            } returns "OK"
            every {
                backend.cleanUpExpiredSemaphoreLocks(eq("{semaphore:test}:leasers"), eq("{semaphore:test}:"))
            } returns null

            val semaphore = Semaphore(listOf(backend), 3)

            assertTrue(semaphore.unlock("test"))
        }

        @ParameterizedTest(name = "Validated with retry count - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry count`(retryCount: Int) {
            if (retryCount > 0) {
                Assertions.assertDoesNotThrow { Semaphore(listOf(backend), 3, retryCount = retryCount) }
            } else {
                assertThrows<IllegalArgumentException> { Semaphore(listOf(backend), 3, retryCount = retryCount) }
            }
        }

        @ParameterizedTest(name = "Validated with retry delay - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry delay`(retryDelay: Long) {
            if (retryDelay > 0) {
                Assertions.assertDoesNotThrow {
                    Semaphore(
                        listOf(backend),
                        3,
                        retryDelay = Duration.ofMillis(retryDelay),
                    )
                }
            } else {
                assertThrows<IllegalArgumentException> {
                    Semaphore(
                        listOf(backend),
                        3,
                        retryDelay = Duration.ofMillis(retryDelay),
                    )
                }
            }
        }

        @ParameterizedTest(name = "Validated with max leases - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate max leases`(maxLeases: Int) {
            if (maxLeases > 0) {
                Assertions.assertDoesNotThrow { Semaphore(listOf(backend), maxLeases) }
            } else {
                assertThrows<IllegalArgumentException> { Semaphore(listOf(backend), maxLeases) }
            }
        }

        @Test
        fun `validate instance count`() {
            Assertions.assertDoesNotThrow { Semaphore(listOf(backend), 3) }
            assertThrows<IllegalArgumentException> { Semaphore(listOf(), 3) }
        }

        @Test
        fun `lock returns false when backend throws`() {
            every {
                backend.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    any(),
                    any(),
                    eq(3),
                    any(),
                )
            } throws RuntimeException("boom")
            every {
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    any(),
                    any(),
                )
            } returns "OK"
            every {
                backend.cleanUpExpiredSemaphoreLocks(eq("{semaphore:test}:leasers"), any())
            } returns "OK"

            val semaphore = Semaphore(listOf(backend), 3, 2, Duration.ofMillis(1))
            val permit = semaphore.lock("test", Duration.ofSeconds(1))

            assertFalse(permit)
            // executor scope is poisoned by the first failure, so retry does not re-run the callee
            verify(exactly = 1) {
                backend.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    any(),
                    any(),
                    eq(3),
                    any(),
                )
            }
        }

        @Test
        fun `unlock returns false when backend throws`() {
            every {
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    any(),
                    any(),
                )
            } throws RuntimeException("boom")

            val semaphore = Semaphore(listOf(backend), 3, 1, Duration.ofMillis(1))

            assertFalse(semaphore.unlock("test"))
            verify(exactly = 1) {
                backend.removeSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    any(),
                    any(),
                )
            }
        }

        @ParameterizedTest(name = "lock acquired with ttl - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 10, 11, 12, 20, 40])
        fun `validate ttl`(ttl: Long) {
            every {
                backend.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofMillis(ttl)),
                )
            } returns "OK"

            val semaphore = Semaphore(listOf(backend), 3)
            if (ttl > 3) {
                Assertions.assertDoesNotThrow { semaphore.lock("test", Duration.ofMillis(ttl)) }
            } else {
                assertThrows<IllegalArgumentException> { semaphore.lock("test", Duration.ofMillis(ttl)) }
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
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(3),
                        eq(Duration.ofSeconds(10)),
                    )
                } returns "OK"
            }

            val semaphore = Semaphore(instances, 3)
            val permit = semaphore.lock("test")

            assertTrue(permit)
            instances.forEach { backend ->
                verify(exactly = 1) {
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(3),
                        any(),
                    )
                }
                verify(exactly = 0) {
                    backend.removeSemaphoreLock(any(), any(), any())
                    backend.cleanUpExpiredSemaphoreLocks(any(), any())
                }
            }
        }

        @Test
        fun `two instances are in quorum`() {
            every {
                backend1.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofSeconds(10)),
                )
            } returns "OK"
            every {
                backend2.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofSeconds(10)),
                )
            } returns null
            every {
                backend3.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofSeconds(10)),
                )
            } returns "OK"

            val semaphore = Semaphore(instances, 3)
            val permit = semaphore.lock("test")

            assertTrue(permit)

            instances.forEach { backend ->
                verify(exactly = 1) {
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(3),
                        any(),
                    )
                }
                verify(exactly = 0) {
                    backend.removeSemaphoreLock(any(), any(), any())
                    backend.cleanUpExpiredSemaphoreLocks(any(), any())
                }
            }
        }

        @Test
        fun `quorum wasn't reach`() {
            every {
                backend1.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofSeconds(3)),
                )
            } returns null
            every {
                backend2.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofSeconds(3)),
                )
            } returns null
            every {
                backend3.setSemaphoreLock(
                    eq("{semaphore:test}:leasers"),
                    match { it.startsWith("{semaphore:test}:") },
                    any(),
                    eq(3),
                    eq(Duration.ofSeconds(3)),
                )
            } returns "OK"
            instances.forEach { backend ->
                every {
                    backend.removeSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                    )
                } returns "OK"
                // cleaning up
                every {
                    backend.cleanUpExpiredSemaphoreLocks(
                        eq("{semaphore:test}:leasers"),
                        eq("{semaphore:test}:"),
                    )
                } returns "OK"
            }

            val semaphore = Semaphore(instances, 3, 3, Duration.ofMillis(20))
            val permit = semaphore.lock("test", Duration.ofSeconds(3))

            assertFalse(permit)

            instances.forEach { backend ->
                verify(exactly = 3) {
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(3),
                        any(),
                    )
                    // unlocking
                    backend.removeSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                    )
                    // cleaning up
                    backend.cleanUpExpiredSemaphoreLocks(eq("{semaphore:test}:leasers"), eq("{semaphore:test}:"))
                }
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
            val maxLeases = 3
            backends.forEachIndexed { index, backend ->
                every {
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(maxLeases),
                        any(),
                    )
                } returns if (index < quorum - 1) "OK" else null
                every {
                    backend.removeSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                    )
                } returns "OK"
                every {
                    backend.cleanUpExpiredSemaphoreLocks(eq("{semaphore:test}:leasers"), eq("{semaphore:test}:"))
                } returns "OK"
            }

            val semaphore = Semaphore(backends, maxLeases, retryCount = 1, retryDelay = Duration.ofMillis(1))
            val permit = semaphore.lock("test", Duration.ofSeconds(10))

            assertFalse(permit)
            verify(exactly = 1) {
                backends.forEach { backend ->
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(maxLeases),
                        any(),
                    )
                }
            }
            verify(exactly = 1) {
                backends.forEach { backend ->
                    backend.removeSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                    )
                }
            }
        }

        @ParameterizedTest(name = "{0} backends, exactly quorum success acquires lock")
        @ValueSource(ints = [2, 4, 5, 7, 10])
        fun `exactly quorum success acquires lock`(number: Int) {
            val backends = (1..number).map { mockk<LocksBackend>() }
            val quorum = number / 2 + 1
            val maxLeases = 3
            backends.forEachIndexed { index, backend ->
                every {
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(maxLeases),
                        any(),
                    )
                } returns if (index < quorum) "OK" else null
            }

            val semaphore = Semaphore(backends, maxLeases, retryCount = 1, retryDelay = Duration.ofMillis(1))
            val permit = semaphore.lock("test", Duration.ofSeconds(10))

            assertTrue(permit)
            verify(exactly = 1) {
                backends.forEach { backend ->
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(maxLeases),
                        any(),
                    )
                }
            }
        }

        @ParameterizedTest(name = "{0} backends, ttl equal to min clock drift is rejected")
        @ValueSource(ints = [1, 3, 5, 7])
        fun `ttl equal to min clock drift is rejected`(number: Int) {
            val backends = (1..number).map { mockk<LocksBackend>() }

            val semaphore = Semaphore(backends, 3)
            assertThrows<IllegalArgumentException> { semaphore.lock("test", Duration.ofMillis(3L * number)) }
        }

        @ParameterizedTest(name = "{0} backends, ttl above min clock drift is accepted")
        @ValueSource(ints = [1, 3, 5, 7])
        fun `ttl above min clock drift is accepted`(number: Int) {
            val backends = (1..number).map { mockk<LocksBackend>() }
            backends.forEach { backend ->
                every {
                    backend.setSemaphoreLock(
                        eq("{semaphore:test}:leasers"),
                        match { it.startsWith("{semaphore:test}:") },
                        any(),
                        eq(3),
                        any(),
                    )
                } returns "OK"
            }

            val semaphore = Semaphore(backends, 3)
            val permit = semaphore.lock("test", Duration.ofMillis(3L * number + 100))

            assertTrue(permit)
        }
    }
}
