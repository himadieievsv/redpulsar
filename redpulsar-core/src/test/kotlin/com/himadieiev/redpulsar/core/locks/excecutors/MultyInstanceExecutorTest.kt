package com.himadieiev.redpulsar.core.locks.excecutors

import TestTags
import com.himadieiev.redpulsar.core.locks.abstracts.Backend
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration

@Tag(TestTags.UNIT)
class MultyInstanceExecutorTest {
    private var backends: List<TestBackend> = emptyList()
    private lateinit var scope: CoroutineScope

    @BeforeEach
    fun setUp() {
        backends = createBackends(1)
        scope = CoroutineScope(Dispatchers.Default)
    }

    @ParameterizedTest(name = "all {0} instances are ok")
    @ValueSource(ints = [1, 2, 3, 4, 5, 7, 10])
    fun `all instances are ok`(number: Int) {
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns "OK" }

        val result =
            multiInstanceExecute(
                backends = backends,
                scope = scope,
                timeout = Duration.ofSeconds(1),
                waiter = ::waitAllJobs,
            ) { backend ->
                backend.test()
            }

        assertEquals(number, result.size)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "quorum instances are down {0} instances")
    @ValueSource(ints = [2, 3, 4, 5, 7, 10])
    fun `quorum instances are down`(number: Int) {
        val quorum = number / 2 + 1
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns "OK" }
        repeat(quorum) { i ->
            every { backends[i].test() } returns null
        }

        val result =
            multiInstanceExecute(
                backends = backends,
                scope = scope,
                timeout = Duration.ofSeconds(1),
                waiter = ::waitAllJobs,
            ) { backend ->
                backend.test()
            }

        assertEquals(emptyList<String>(), result)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "non quorum instances are down {0} instances")
    @ValueSource(ints = [2, 3, 4, 5, 7, 10])
    fun `non quorum instances are down`(number: Int) {
        val quorum = number / 2 + 1
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns "OK" }
        repeat(number - quorum) { i ->
            every { backends[i].test() } returns null
        }

        val result =
            multiInstanceExecute(
                backends = backends,
                scope = scope,
                timeout = Duration.ofSeconds(1),
                waiter = ::waitAllJobs,
            ) { backend ->
                backend.test()
            }

        assertTrue(number / 2 + 1 <= result.size)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "all instances are ok, wait majority with {0} instances")
    @ValueSource(ints = [1, 2, 3, 4, 5, 7, 10])
    fun `all instances are ok, wait majority`(number: Int) {
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns "OK" }

        val result =
            multiInstanceExecute(
                backends = backends,
                scope = scope,
                timeout = Duration.ofSeconds(1),
                waiter = ::waitMajorityJobs,
            ) { backend -> backend.test() }

        assertTrue(number / 2 + 1 <= result.size)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "all instances are ok, wait majority with {0} instances")
    @ValueSource(ints = [1, 2, 3, 4, 5, 7, 10])
    fun `one instance is up, wait majority`(number: Int) {
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns null }
        repeat(number) { i ->
            every { backends[i].test() } returns "OK"
        }

        val result =
            multiInstanceExecute(
                backends = backends,
                scope = scope,
                timeout = Duration.ofSeconds(1),
                waiter = ::waitMajorityJobs,
            ) { backend -> backend.test() }

        assertTrue(number / 2 + 1 <= result.size)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "retry on non quorum instance count is down {0} instances")
    @ValueSource(ints = [2, 3, 4, 5, 7, 10])
    fun `retry on non quorum instance count is down`(number: Int) {
        val quorum = number / 2 + 1
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns "OK" }
        repeat(quorum) { i ->
            every { backends[i].test() } returns null
        }

        val result =
            multyInstanceExecuteWithRetry(
                backends = backends,
                scope = scope,
                timeout = Duration.ofSeconds(1),
                retryCount = 3,
                retryDelay = Duration.ofMillis(1),
                waiter = ::waitAllJobs,
            ) { backend ->
                backend.test()
            }

        assertEquals(emptyList<String>(), result)
        verify(exactly = 3) { backends.forEach { backend -> backend.test() } }
    }

    private abstract inner class TestBackend : Backend() {
        abstract fun test(): String?
    }

    private fun createBackends(number: Int): List<TestBackend> {
        val backends = mutableListOf<TestBackend>()
        repeat(number) {
            backends.add(mockk<TestBackend>())
        }
        return backends
    }
}
