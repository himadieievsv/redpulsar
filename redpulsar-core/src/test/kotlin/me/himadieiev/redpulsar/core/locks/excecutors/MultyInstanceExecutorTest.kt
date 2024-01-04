package me.himadieiev.redpulsar.core.locks.excecutors

import TestTags
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import me.himadieiev.redpulsar.core.locks.abstracts.Backend
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

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
            multyInstanceExecute(
                backends = backends,
                scope = scope,
                releaseTime = 1.seconds,
                waiter = ::waitAllJobs,
            ) { backend ->
                backend.test()
            }

        assertEquals(createResponses(number), result)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "quorum instance count is down {0} instances")
    @ValueSource(ints = [2, 3, 4, 5, 7, 10])
    fun `quorum instance count is down`(number: Int) {
        val quorum = number / 2 + 1
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns "OK" }
        repeat(quorum) { i ->
            every { backends[i].test() } returns null
        }

        val result =
            multyInstanceExecute(
                backends = backends,
                scope = scope,
                releaseTime = 1.seconds,
                waiter = ::waitAllJobs,
            ) { backend ->
                backend.test()
            }

        assertEquals(emptyList<String>(), result)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "non quorum instance count is down {0} instances")
    @ValueSource(ints = [2, 3, 4, 5, 7, 10])
    fun `non quorum instance count is down`(number: Int) {
        val quorum = number / 2 + 1
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns "OK" }
        repeat(number - quorum) { i ->
            every { backends[i].test() } returns null
        }

        val result =
            multyInstanceExecute(
                backends = backends,
                scope = scope,
                releaseTime = 1.seconds,
                waiter = ::waitAllJobs,
            ) { backend ->
                backend.test()
            }

        assertEquals(createResponses(quorum), result)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "all instances are ok, wait any with {0} instances")
    @ValueSource(ints = [1, 2, 3, 4, 5, 7, 10])
    fun `all instances are ok, wait any`(number: Int) {
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns "OK" }

        val result =
            multyInstanceExecute(
                backends = backends,
                scope = scope,
                releaseTime = 1.seconds,
                waiter = ::waitAnyJobs,
            ) { backend -> backend.test() }

        assertEquals(createResponses(number), result)
        verify(exactly = 1) { backends.forEach { backend -> backend.test() } }
    }

    @ParameterizedTest(name = "all instances are ok, wait any with {0} instances")
    @ValueSource(ints = [1, 2, 3, 4, 5, 7, 10])
    fun `one instance is up, wait any`(number: Int) {
        backends = createBackends(number)
        backends.forEach { backend -> every { backend.test() } returns null }
        every { backends[Random.nextInt(0, number)].test() } returns "OK"

        val result =
            multyInstanceExecute(
                backends = backends,
                scope = scope,
                releaseTime = 1.seconds,
                waiter = ::waitAnyJobs,
            ) { backend -> backend.test() }

        assertEquals(createResponses(number), result)
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
                releaseTime = 1.seconds,
                retryCount = 3,
                retryDelay = 1.milliseconds,
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

    private fun createResponses(number: Int): List<String> {
        val responses = mutableListOf<String>()
        repeat(number) {
            responses.add("OK")
        }
        return responses
    }
}
