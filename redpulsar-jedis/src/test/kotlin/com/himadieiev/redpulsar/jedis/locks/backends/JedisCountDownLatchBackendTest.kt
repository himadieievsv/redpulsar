package com.himadieiev.redpulsar.jedis.locks.backends

import com.himadieiev.redpulsar.core.common.LuaScriptEntry
import com.himadieiev.redpulsar.jedis.locks.evalSha1
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import redis.clients.jedis.JedisPubSub
import redis.clients.jedis.UnifiedJedis
import java.io.IOException
import java.time.Duration

@Tag(TestTags.UNIT)
class JedisCountDownLatchBackendTest {
    private lateinit var redis: UnifiedJedis
    private lateinit var countDownLatchBackend: JedisCountDownLatchBackend

    @BeforeEach
    fun setUp() {
        redis = mockk()
        countDownLatchBackend = JedisCountDownLatchBackend(redis)
    }

    @Nested
    inner class CountTests {
        @Test
        fun `count successful`() {
            val clientId = "uuid"
            every {
                redis.evalsha(
                    any(),
                    eq(listOf("latch:test", "latch:test:channel")),
                    eq(listOf("${clientId}0", "5000", "4")),
                )
            } returns "OK"
            val callResult =
                countDownLatchBackend.count("latch:test", "latch:test:channel", clientId, 0, 4, Duration.ofSeconds(5))

            assertEquals("OK", callResult)
            verify(exactly = 1) {
                redis.evalsha(any(), any<List<String>>(), any<List<String>>())
            }
        }

        @Test
        fun `in count throws exception`() {
            val clientId = "uuid"
            every {
                redis.evalsha(
                    any(),
                    eq(listOf("latch:test", "latch:test:channel")),
                    eq(listOf("${clientId}0", "5000", "4")),
                )
            } throws IOException("test exception")
            val callResult =
                countDownLatchBackend.count("latch:test", "latch:test:channel", clientId, 0, 4, Duration.ofSeconds(5))

            assertNull(callResult)
            verify(exactly = 1) {
                redis.evalsha(any(), any<List<String>>(), any<List<String>>())
            }
        }
    }

    @Nested
    inner class UndoCountTests {
        @ParameterizedTest(name = "undo count successful with {0} seconds ttl")
        @ValueSource(ints = [0, 1])
        fun `undo count successful`(res: Long) {
            val clientId = "uuid"
            every { redis.srem("latch:test", "${clientId}0") } returns res
            val callResult = countDownLatchBackend.undoCount("latch:test", clientId, 0)

            assertEquals(res, callResult)
            verify(exactly = 1) { redis.srem(any<String>(), any()) }
        }

        @Test
        fun `in undo count throws exception`() {
            val clientId = "uuid"
            every { redis.srem("latch:test", "${clientId}0") } throws IOException("test exception")
            val callResult = countDownLatchBackend.undoCount("latch:test", clientId, 0)

            assertNull(callResult)
            verify(exactly = 1) { redis.srem(any<String>(), any()) }
        }
    }

    @Nested
    inner class CheckCountTests {
        @ParameterizedTest(name = "check count successful with {0} seconds ttl")
        @ValueSource(ints = [0, 1, 3, 4, 5])
        fun `check count successful`(res: Long) {
            every { redis.scard("latch:test") } returns res
            val callResult = countDownLatchBackend.checkCount("latch:test")

            assertEquals(res, callResult)
            verify(exactly = 1) { redis.scard(any<String>()) }
        }

        @Test
        fun `in check count throws exception`() {
            every { redis.scard("latch:test") } throws IOException("test exception")
            val callResult = countDownLatchBackend.checkCount("latch:test")

            assertNull(callResult)
            verify(exactly = 1) { redis.scard(any<String>()) }
        }
    }

    @Nested
    inner class ListenTests {
        @ParameterizedTest(name = "listen produce value receive {0} messages")
        @ValueSource(ints = [1, 2, 3, 5])
        fun `listen produce value`(messageCount: Int) {
            val pubSubSlot = slot<JedisPubSub>()
            val channel = slot<String>()
            every { redis.subscribe(capture(pubSubSlot), capture(channel)) } returns Unit
            CoroutineScope(CoroutineName("test")).launch {
                val result = countDownLatchBackend.listen("latch:test:channel")
                assertEquals("open", result)
            }
            runBlocking { delay(200) }
            repeat(messageCount) {
                pubSubSlot.captured.onMessage("latch:test:channel", "open")
            }
            assertEquals("latch:test:channel", channel.captured)
            verify(exactly = 1) {
                redis.subscribe(any<JedisPubSub>(), any<String>())
            }
        }

        @Test
        fun `message not received`() {
            every { redis.subscribe(any(), eq("latch:test:channel")) } returns Unit

            runBlocking {
                assertThrows<TimeoutCancellationException> {
                    withTimeout(100) { countDownLatchBackend.listen("latch:test:channel") }
                }
            }
            verify(exactly = 1) {
                redis.subscribe(any<JedisPubSub>(), any<String>())
            }
        }
    }
}
