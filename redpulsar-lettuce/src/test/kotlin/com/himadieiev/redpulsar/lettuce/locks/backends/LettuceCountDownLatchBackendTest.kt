package com.himadieiev.redpulsar.lettuce.locks.backends

import com.himadieiev.redpulsar.lettuce.LettucePubSubPooled
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.pubsub.RedisPubSubListener
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.commons.pool2.impl.GenericObjectPool
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException
import java.time.Duration

@Tag(TestTags.UNIT)
class LettuceCountDownLatchBackendTest {
    private lateinit var redis: LettucePubSubPooled<String, String>
    private lateinit var sync: RedisPubSubCommands<String, String>
    private lateinit var countDownLatchBackend: LettuceCountDownLatchBackend

    @BeforeEach
    fun setUp() {
        val pool = mockk<GenericObjectPool<StatefulRedisPubSubConnection<String, String>>>()
        val connection = mockk<StatefulRedisPubSubConnection<String, String>>()
        redis = LettucePubSubPooled(pool)
        sync = mockk<RedisPubSubCommands<String, String>>()
        countDownLatchBackend = LettuceCountDownLatchBackend(redis)
        every { pool.borrowObject() } returns connection
        every { pool.returnObject(connection) } returns Unit
        every { connection.sync() } returns sync
        every { connection.isMulti } returns false
    }

    @Nested
    inner class CountTests {
        @Test
        fun `count successful`() {
            val clientId = "uuid"
            every {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.STATUS),
                    eq(arrayOf("latch:test", "latch:channel:test")),
                    eq("${clientId}0"), eq("5000"), eq("4"),
                )
            } returns "OK"
            val callResult =
                countDownLatchBackend.count("latch:test", "latch:channel:test", clientId, 0, 4, Duration.ofSeconds(5))

            Assertions.assertEquals("OK", callResult)
            verify(exactly = 1) {
                sync.eval<String>(any<String>(), any(), any<Array<String>>(), any(), any(), any())
            }
        }

        @Test
        fun `in count throws exception`() {
            val clientId = "uuid"
            every {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.STATUS),
                    eq(arrayOf("latch:test", "latch:channel:test")),
                    eq("${clientId}0"), eq("5000"), eq("4"),
                )
            } throws IOException("test exception")
            val callResult =
                countDownLatchBackend.count("latch:test", "latch:channel:test", clientId, 0, 4, Duration.ofSeconds(5))

            Assertions.assertNull(callResult)
            verify(exactly = 1) {
                sync.eval<String>(any<String>(), any(), any<Array<String>>(), any(), any(), any())
            }
        }
    }

    @Nested
    inner class UndoCountTests {
        @ParameterizedTest(name = "undo count successful with {0} seconds ttl")
        @ValueSource(ints = [0, 1])
        fun `undo count successful`(res: Long) {
            val clientId = "uuid"
            every { sync.srem("latch:test", "${clientId}0") } returns res
            val callResult = countDownLatchBackend.undoCount("latch:test", clientId, 0)

            Assertions.assertEquals(res, callResult)
            verify(exactly = 1) { sync.srem(any<String>(), any()) }
        }

        @Test
        fun `in undo count throws exception`() {
            val clientId = "uuid"
            every { sync.srem("latch:test", "${clientId}0") } throws IOException("test exception")
            val callResult = countDownLatchBackend.undoCount("latch:test", clientId, 0)

            Assertions.assertNull(callResult)
            verify(exactly = 1) { sync.srem(any<String>(), any()) }
        }
    }

    @Nested
    inner class CheckCountTests {
        @ParameterizedTest(name = "check count successful with {0} seconds ttl")
        @ValueSource(ints = [0, 1, 3, 4, 5])
        fun `check count successful`(res: Long) {
            every { sync.scard("latch:test") } returns res
            val callResult = countDownLatchBackend.checkCount("latch:test")

            Assertions.assertEquals(res, callResult)
            verify(exactly = 1) { sync.scard(any<String>()) }
        }

        @Test
        fun `in check count throws exception`() {
            every { sync.scard("latch:test") } throws IOException("test exception")
            val callResult = countDownLatchBackend.checkCount("latch:test")

            Assertions.assertNull(callResult)
            verify(exactly = 1) { sync.scard(any<String>()) }
        }
    }

    @Nested
    inner class ListenTests {
        private lateinit var connection: StatefulRedisPubSubConnection<String, String>

        @BeforeEach
        fun setUp() {
            connection = mockk()
            every { sync.subscribe(eq("latch:channel:test")) } returns Unit
            every { sync.unsubscribe(eq("latch:channel:test")) } returns Unit
            every { sync.statefulConnection } returns connection
            every { connection.addListener(any<RedisPubSubListener<String, String>>()) } returns Unit
            every { connection.removeListener(any<RedisPubSubListener<String, String>>()) } returns Unit
        }

        @ParameterizedTest(name = "listen produce value receive {0} messages")
        @ValueSource(ints = [1, 2, 3, 5])
        fun `listen produce value`(messageCount: Int) {
            val listener = slot<RedisPubSubListener<String, String>>()
            every { connection.addListener(capture(listener)) } returns Unit
            CoroutineScope(CoroutineName("latch:channel:test")).launch {
                val result = countDownLatchBackend.listen("latch:channel:test")
                Assertions.assertEquals("open", result)
            }

            runBlocking { Thread.sleep(200) }
            repeat(messageCount) {
                listener.captured.message("latch:channel:test", "open")
            }
            runBlocking { Thread.sleep(100) }
            verify(exactly = 1) {
                sync.subscribe(eq("latch:channel:test"))
                sync.unsubscribe(eq("latch:channel:test"))
            }
        }

        @Test
        fun `message not received`() {
            runBlocking {
                assertThrows<TimeoutCancellationException> {
                    withTimeout(100) { countDownLatchBackend.listen("latch:channel:test") }
                }
            }
            verify(exactly = 1) {
                sync.subscribe("latch:channel:test")
            }
        }
    }
}
