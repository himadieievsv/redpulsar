package com.himadieiev.redpulsar.core.locks

import TestTags
import com.himadieiev.redpulsar.core.locks.abstracts.backends.CountDownLatchBackend
import com.himadieiev.redpulsar.core.locks.api.CallResult
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.verify
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import java.util.concurrent.CancellationException

@Tag(TestTags.UNIT)
class ListeningCountDownLatchTest {
    @Nested
    inner class SingleRedisInstance {
        private lateinit var backend: CountDownLatchBackend

        @BeforeEach
        fun setUp() {
            backend = mockk<CountDownLatchBackend>()
        }

        @Test
        fun `count down`() {
            backend.everyCount("countdownlatch:test", "countdownlatch:channels:test", 4, 4, returnVal = "OK")
            backend.everyCount("countdownlatch:test", "countdownlatch:channels:test", 3, 4, returnVal = "OK")
            backend.everyCount("countdownlatch:test", "countdownlatch:channels:test", 2, 4, returnVal = "OK")
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    listOf(backend),
                    maxDuration = Duration.ofSeconds(10),
                )
            repeat(3) { assertEquals(CallResult.SUCCESS, latch.countDown()) }

            verify(exactly = 3) { backend.count(any(), any(), any(), any(), any(), any()) }
            verify(exactly = 0) { backend.undoCount(any(), any(), any()) }
        }

        @Test
        fun `count down failing`() {
            backend.everyCount("countdownlatch:test", "countdownlatch:channels:test", 2, 2, returnVal = null)
            backend.everyUndoCount("countdownlatch:test", 2, 1)
            val latch =
                ListeningCountDownLatch(
                    "test",
                    2,
                    listOf(backend),
                    maxDuration = Duration.ofSeconds(10),
                    retryCount = 4,
                    retryDelay = Duration.ofMillis(1),
                )
            repeat(2) { assertEquals(CallResult.FAILED, latch.countDown()) }

            verify(exactly = 8) { backend.count(any(), any(), any(), any(), any(), any()) }
            verify(exactly = 2) { backend.undoCount(any(), any(), any()) }
        }

        @Test
        fun `undo count failing`() {
            backend.everyCount("countdownlatch:test", "countdownlatch:channels:test", 2, 2, returnVal = null)
            backend.everyUndoCount("countdownlatch:test", 2, null)
            val latch =
                ListeningCountDownLatch(
                    "test",
                    2,
                    listOf(backend),
                    maxDuration = Duration.ofSeconds(10),
                    retryCount = 6,
                    retryDelay = Duration.ofMillis(1),
                )
            repeat(2) { assertEquals(CallResult.FAILED, latch.countDown()) }

            verify(exactly = 12) { backend.count(any(), any(), any(), any(), any(), any()) }
            verify(exactly = 12) { backend.undoCount(any(), any(), any()) }
        }

        @Test
        fun await() {
            backend.everyListen("open")
            backend.everyCheckCount("countdownlatch:test", 2)
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    listOf(backend),
                    maxDuration = Duration.ofSeconds(10),
                )

            assertEquals(CallResult.SUCCESS, latch.await())

            coVerify(exactly = 1) {
                backend.listen(any())
            }
            verify(exactly = 1) {
                backend.checkCount(any())
            }
        }

        @Test
        fun `await finished by global count`() {
            backend.everyCheckCount("countdownlatch:test", 4)
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    listOf(backend),
                    maxDuration = Duration.ofSeconds(10),
                )

            assertEquals(CallResult.SUCCESS, latch.await())

            verify(exactly = 1) {
                backend.checkCount(any())
            }
            coVerify(exactly = 0) {
                backend.listen(any())
            }
        }

        @Test
        fun `await - check count failed`() {
            backend.everyCheckCount("countdownlatch:test", null)
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    listOf(backend),
                    maxDuration = Duration.ofSeconds(10),
                    retryCount = 2,
                    retryDelay = Duration.ofMillis(1),
                )

            assertEquals(CallResult.FAILED, latch.await())

            verify(exactly = 2) { backend.checkCount(any()) }
            coVerify(exactly = 0) { backend.listen(any()) }
        }

        @Test
        fun `await canceled`() {
            val latch = ListeningCountDownLatch("test", 4, listOf(backend))
            mockkObject(latch)
            every {
                latch invoke "getCount" withArguments listOf(any<CoroutineScope>())
            } throws CancellationException("test exception")

            assertEquals(CallResult.FAILED, latch.await())
            verify(exactly = 0) {
                backend.checkCount(any())
            }
            coVerify(exactly = 0) {
                backend.listen(any())
            }
        }

        @Test
        fun `await timed out`() {
            coEvery {
                backend.listen(eq("countdownlatch:channels:test"))
            } answers {
                runBlocking { delay(1000) }
                null
            }

            backend.everyCheckCount("countdownlatch:test", 2)
            val latch = ListeningCountDownLatch("test", 4, listOf(backend))

            assertEquals(CallResult.FAILED, latch.await(Duration.ofMillis(110)))
            verify(exactly = 1) {
                backend.checkCount(any())
            }
            coVerify(exactly = 1) {
                backend.listen(any())
            }
        }

        @Test
        fun `await failed`() {
            backend.everyListen(null)
            backend.everyCheckCount("countdownlatch:test", 3)
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    listOf(backend),
                    maxDuration = Duration.ofSeconds(10),
                    retryCount = 5,
                    retryDelay = Duration.ofMillis(1),
                )

            assertEquals(CallResult.FAILED, latch.await())

            verify(exactly = 1) { backend.checkCount(any()) }
            coVerify(exactly = 5) { backend.listen(any()) }
        }

        @ParameterizedTest(name = "current count: {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5])
        fun `check count`(count: Long) {
            backend.everyCheckCount("countdownlatch:test", count)
            val latch = ListeningCountDownLatch("test", 5, listOf(backend))

            assertEquals(5 - count.toInt(), latch.getCount())

            verify(exactly = 1) { backend.checkCount(any()) }
        }

        @Test
        fun `check count failed`() {
            backend.everyCheckCount("countdownlatch:test", null)
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    listOf(backend),
                    retryCount = 8,
                    retryDelay = Duration.ofMillis(1),
                )

            assertEquals(Int.MIN_VALUE, latch.getCount())

            verify(exactly = 8) { backend.checkCount(any()) }
        }

        @ParameterizedTest(name = "Validated with retry count - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry count`(retryCount: Int) {
            if (retryCount > 0) {
                assertDoesNotThrow {
                    ListeningCountDownLatch(
                        "test",
                        3,
                        listOf(backend),
                        retryCount = retryCount,
                    )
                }
            } else {
                assertThrows<IllegalArgumentException> {
                    ListeningCountDownLatch(
                        "test",
                        3,
                        listOf(backend),
                        retryCount = retryCount,
                    )
                }
            }
        }

        @ParameterizedTest(name = "Validated with retry delay - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry delay`(retryDelay: Long) {
            if (retryDelay > 0) {
                assertDoesNotThrow {
                    ListeningCountDownLatch(
                        "test",
                        3,
                        listOf(backend),
                        retryDelay = Duration.ofMillis(retryDelay),
                    )
                }
            } else {
                assertThrows<IllegalArgumentException> {
                    ListeningCountDownLatch(
                        "test",
                        3,
                        listOf(backend),
                        retryDelay = Duration.ofMillis(retryDelay),
                    )
                }
            }
        }

        @ParameterizedTest(name = "Validated with  latch count - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 3, 10])
        fun `validate latch count`(latchCount: Int) {
            if (latchCount > 0) {
                assertDoesNotThrow {
                    ListeningCountDownLatch(
                        "test",
                        latchCount,
                        listOf(backend),
                    )
                }
            } else {
                assertThrows<IllegalArgumentException> {
                    ListeningCountDownLatch(
                        "test",
                        latchCount,
                        listOf(backend),
                    )
                }
            }
        }

        @Test
        fun `validate instance count`() {
            assertDoesNotThrow {
                ListeningCountDownLatch(
                    "test",
                    3,
                    listOf(backend),
                )
            }
            assertThrows<IllegalArgumentException> {
                ListeningCountDownLatch(
                    "test",
                    3,
                    listOf(),
                )
            }
        }

        @Test
        fun `validate latch name`() {
            assertDoesNotThrow {
                ListeningCountDownLatch(
                    "test",
                    3,
                    listOf(backend),
                )
            }
            assertThrows<IllegalArgumentException> {
                ListeningCountDownLatch(
                    "",
                    3,
                    listOf(),
                )
            }
        }

        @ParameterizedTest(name = "Validated with max duration - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 10, 100, 101, 1000])
        fun `validate max duration`(maxDuration: Long) {
            if (maxDuration > 100) {
                assertDoesNotThrow {
                    ListeningCountDownLatch(
                        "test",
                        3,
                        listOf(backend),
                        maxDuration = Duration.ofMillis(maxDuration),
                    )
                }
            } else {
                assertThrows<IllegalArgumentException> {
                    ListeningCountDownLatch(
                        "test",
                        3,
                        listOf(backend),
                        maxDuration = Duration.ofMillis(maxDuration),
                    )
                }
            }
        }
    }

    @Nested
    inner class MultipleRedisInstance {
        private lateinit var backend1: CountDownLatchBackend
        private lateinit var backend2: CountDownLatchBackend
        private lateinit var backend3: CountDownLatchBackend
        private lateinit var instances: List<CountDownLatchBackend>

        @BeforeEach
        fun setUp() {
            backend1 = mockk<CountDownLatchBackend>()
            backend2 = mockk<CountDownLatchBackend>()
            backend3 = mockk<CountDownLatchBackend>()
            instances = listOf(backend1, backend2, backend3)
        }

        @Test
        fun `all instances are in quorum for count down`() {
            instances.forEach { backend ->
                backend.everyCount(
                    "countdownlatch:test",
                    "countdownlatch:channels:test",
                    4,
                    4,
                    Duration.ofMinutes(10),
                    "OK",
                )
            }
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(CallResult.SUCCESS, latch.countDown())

            verify(exactly = 1) {
                instances.forEach { backend -> backend.count(any(), any(), any(), any(), any(), any()) }
            }
            verify(exactly = 0) { instances.forEach { backend -> backend.undoCount(any(), any(), any()) } }
        }

        @Test
        fun `two instances are in quorum for count down`() {
            backend1.everyCount(
                "countdownlatch:test",
                "countdownlatch:channels:test",
                4,
                4,
                Duration.ofMinutes(10),
                "OK",
            )
            backend2.everyCount(
                "countdownlatch:test",
                "countdownlatch:channels:test",
                4,
                4,
                Duration.ofMinutes(10),
                null,
            )
            backend3.everyCount(
                "countdownlatch:test",
                "countdownlatch:channels:test",
                4,
                4,
                Duration.ofMinutes(10),
                "OK",
            )
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(CallResult.SUCCESS, latch.countDown())

            verify(exactly = 1) {
                instances.forEach { backend -> backend.count(any(), any(), any(), any(), any(), any()) }
            }
            verify(exactly = 0) { instances.forEach { backend -> backend.undoCount(any(), any(), any()) } }
        }

        @Test
        fun `quorum wasn't reach for count down`() {
            backend1.everyCount(
                "countdownlatch:test",
                "countdownlatch:channels:test",
                4,
                4,
                Duration.ofMinutes(10),
                null,
            )
            backend2.everyCount(
                "countdownlatch:test",
                "countdownlatch:channels:test",
                4,
                4,
                Duration.ofMinutes(10),
                null,
            )
            backend3.everyCount(
                "countdownlatch:test",
                "countdownlatch:channels:test",
                4,
                4,
                Duration.ofMinutes(10),
                "OK",
            )
            instances.forEach { backend ->
                backend.everyUndoCount("countdownlatch:test", 4, 1)
            }
            val latch =
                ListeningCountDownLatch(
                    "test",
                    4,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(CallResult.FAILED, latch.countDown())

            verify(exactly = 3) {
                instances.forEach { backend -> backend.count(any(), any(), any(), any(), any(), any()) }
            }
            verify(exactly = 1) { instances.forEach { backend -> backend.undoCount(any(), any(), any()) } }
        }

        @Test
        fun `all instances are in quorum for await`() {
            instances.forEach { backend ->
                backend.everyListen("open")
                backend.everyCheckCount("countdownlatch:test", 1)
            }
            val latch =
                ListeningCountDownLatch(
                    "test",
                    2,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(CallResult.SUCCESS, latch.await())

            coVerify(exactly = 1) {
                instances.forEach { backend -> backend.listen(any()) }
            }
            verify(exactly = 1) {
                instances.forEach { backend -> backend.checkCount(any()) }
            }
        }

        @Test
        fun `two instances are in quorum for await`() {
            instances.forEach { backend ->
                backend.everyCheckCount("countdownlatch:test", 1)
            }
            backend1.everyListen("open")
            backend2.everyListen(null)
            backend3.everyListen("open")
            val latch =
                ListeningCountDownLatch(
                    "test",
                    5,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(CallResult.SUCCESS, latch.await())

            coVerify(exactly = 1) {
                instances.forEach { backend -> backend.listen(any()) }
            }
            verify(exactly = 1) {
                instances.forEach { backend -> backend.checkCount(any()) }
            }
        }

        @Test
        fun `quorum wasn't reach but await succeed`() {
            instances.forEach { backend ->
                backend.everyCheckCount("countdownlatch:test", 1)
            }
            backend1.everyListen(null)
            backend2.everyListen("open")
            backend3.everyListen(null)
            val latch =
                ListeningCountDownLatch(
                    "test",
                    3,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(CallResult.SUCCESS, latch.await())

            coVerify(exactly = 1) {
                instances.forEach { backend -> backend.listen(any()) }
            }
            verify(exactly = 1) {
                instances.forEach { backend -> backend.checkCount(any()) }
            }
        }

        @Test
        fun `all instances are down`() {
            instances.forEach { backend ->
                backend.everyCheckCount("countdownlatch:test", 1)
                backend.everyListen(null)
            }
            val latch =
                ListeningCountDownLatch(
                    "test",
                    2,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(CallResult.FAILED, latch.await())

            coVerify(exactly = 3) {
                instances.forEach { backend -> backend.listen(any()) }
            }
            verify(exactly = 1) {
                instances.forEach { backend -> backend.checkCount(any()) }
            }
        }

        @Test
        fun `check count return max value of majority`() {
            backend1.everyCheckCount("countdownlatch:test", 1)
            backend2.everyCheckCount("countdownlatch:test", 2)
            backend3.everyCheckCount("countdownlatch:test", 1)

            val latch =
                ListeningCountDownLatch(
                    "test",
                    2,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(1, latch.getCount())

            verify(exactly = 1) { instances.forEach { backend -> backend.checkCount(any()) } }
        }

        @Test
        fun `check count return min int`() {
            backend1.everyCheckCount("countdownlatch:test", null)
            backend2.everyCheckCount("countdownlatch:test", null)
            backend3.everyCheckCount("countdownlatch:test", 2)

            val latch =
                ListeningCountDownLatch(
                    "test",
                    2,
                    backends = instances,
                    retryDelay = Duration.ofMillis(1),
                )
            assertEquals(Int.MIN_VALUE, latch.getCount())

            verify(exactly = 3) { instances.forEach { backend -> backend.checkCount(any()) } }
        }
    }

    private fun CountDownLatchBackend.everyCount(
        latchKeyName: String,
        channelName: String,
        count: Int,
        initialCount: Int,
        maxDuration: Duration = Duration.ofSeconds(20),
        returnVal: String?,
    ) {
        val backend = this
        every {
            backend.count(
                eq(latchKeyName),
                eq(channelName),
                // Client ID
                any(),
                eq(count),
                eq(initialCount),
                eq(maxDuration),
            )
        } returns returnVal
    }

    private fun CountDownLatchBackend.everyUndoCount(
        latchKeyName: String,
        count: Int,
        returnVal: Long?,
    ) {
        val backend = this
        every {
            backend.undoCount(
                eq(latchKeyName),
                // Client ID
                any(),
                eq(count),
            )
        } returns returnVal
    }

    private fun CountDownLatchBackend.everyListen(returnVal: String?) {
        val backend = this
        coEvery {
            backend.listen(eq("countdownlatch:channels:test"))
        } returns returnVal
    }

    private fun CountDownLatchBackend.everyCheckCount(
        latchKeyName: String,
        returnVal: Long?,
    ) {
        val backend = this
        every {
            backend.checkCount(eq(latchKeyName))
        } returns returnVal
    }
}
