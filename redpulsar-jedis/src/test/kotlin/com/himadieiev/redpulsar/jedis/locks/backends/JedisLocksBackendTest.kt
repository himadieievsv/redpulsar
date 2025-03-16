package com.himadieiev.redpulsar.jedis.locks.backends

import TestTags
import com.himadieiev.redpulsar.core.common.LuaScriptEntry
import com.himadieiev.redpulsar.jedis.locks.evalSha1
import equalsTo
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import redis.clients.jedis.Pipeline
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.params.SetParams
import java.io.IOException
import java.time.Duration

@Tag(TestTags.UNIT)
class JedisLocksBackendTest {
    private lateinit var redis: UnifiedJedis
    private lateinit var lock: JedisLocksBackend

    @BeforeEach
    fun setUp() {
        redis = mockk()
        lock = JedisLocksBackend(redis)
    }

    @Nested
    inner class SetLockTests {
        @Test
        fun `set lock successful`() {
            val clientId = "uuid"
            every {
                redis.set(
                    eq("test"), eq(clientId),
                    match<SetParams> {
                        it.equalsTo(SetParams().nx().px(Duration.ofSeconds(5).toMillis()))
                    },
                )
            } returns "OK"

            val permit = lock.setLock("test", clientId, Duration.ofSeconds(5))

            assertEquals("OK", permit)

            verify(exactly = 1) {
                redis.set(
                    eq("test"),
                    eq(clientId),
                    match<SetParams> {
                        it.equalsTo(SetParams().nx().px(Duration.ofSeconds(5).toMillis()))
                    },
                )
            }
        }

        @Test
        fun `set lock failed`() {
            val clientId = "uuid"
            every {
                redis.set(
                    eq("test"), eq(clientId),
                    match<SetParams> {
                        it.equalsTo(SetParams().nx().px(Duration.ofSeconds(10).toMillis()))
                    },
                )
            } returns null

            val permit = lock.setLock("test", clientId, Duration.ofSeconds(10))

            assertNull(permit)
        }

        @Test
        fun `set lock throws exception`() {
            val clientId = "uuid"
            every {
                redis.set(
                    eq("test"), eq(clientId),
                    match<SetParams> {
                        it.equalsTo(SetParams().nx().px(200))
                    },
                )
            } throws IOException("test exception")
            val permit = lock.setLock("test", clientId, Duration.ofMillis(200))

            assertNull(permit)
        }
    }

    @Nested
    inner class RemoveLockTests {
        @Test
        fun `remove lock successful`() {
            val clientId = "uuid"
            every { redis.evalsha(any(), eq(listOf("test")), eq(listOf(clientId))) } returns "OK"
            val permit = lock.removeLock("test", clientId)

            assertEquals("OK", permit)
            verify(exactly = 1) {
                redis.evalsha(any(), eq(listOf("test")), eq(listOf(clientId)))
            }
            verify(exactly = 0) {
                redis.set(any<String>(), any(), any())
            }
        }

        @Test
        fun `remove lock failed`() {
            val clientId = "uuid"
            every { redis.evalsha(any(), eq(listOf("test")), eq(listOf(clientId))) } returns null
            val permit = lock.removeLock("test", clientId)

            assertNull(permit)
        }

        @Test
        fun `remove lock throws exception`() {
            val clientId = "uuid"
            every { redis.evalsha(any(), eq(listOf("test")), eq(listOf(clientId))) } throws IOException("test exception")
            val permit = lock.removeLock("test", clientId)

            assertNull(permit)
        }
    }

    @Nested
    inner class SetSemaphoreLockTests {
        @Test
        fun `set semaphore lock successful`() {
            val clientId = "uuid"
            every {
                redis.evalsha(any(), eq(listOf("test-key1", "test-key2")), eq(listOf(clientId, "4", "5000")))
            } returns "OK"
            val permit = lock.setSemaphoreLock("test-key1", "test-key2", clientId, 4, Duration.ofSeconds(5))

            assertEquals("OK", permit)
            verify(exactly = 1) {
                redis.evalsha(any(), eq(listOf("test-key1", "test-key2")), eq(listOf(clientId, "4", "5000")))
            }
        }

        @Test
        fun `set semaphore lock failed`() {
            val clientId = "uuid"
            every {
                redis.evalsha(any<String>(), eq(listOf("test-key1", "test-key2")), eq(listOf(clientId, "4", "5000")))
            } returns null
            val permit = lock.setSemaphoreLock("test-key1", "test-key2", clientId, 4, Duration.ofSeconds(5))

            assertNull(permit)
            verify(exactly = 1) {
                redis.evalsha(any<String>(), eq(listOf("test-key1", "test-key2")), eq(listOf(clientId, "4", "5000")))
            }
        }

        @Test
        fun `set semaphore lock throws exceptions`() {
            val clientId = "uuid"
            every {
                redis.evalsha(any<String>(), eq(listOf("test-key1", "test-key2")), eq(listOf(clientId, "10", "100")))
            } throws IOException("test exception")
            val permit = lock.setSemaphoreLock("test-key1", "test-key2", clientId, 10, Duration.ofMillis(100))

            assertNull(permit)
            verify(exactly = 1) {
                redis.evalsha(any<String>(), eq(listOf("test-key1", "test-key2")), eq(listOf(clientId, "10", "100")))
            }
        }
    }

    @Nested
    inner class RemoveSemaphoreLockTests {
        @Test
        fun `remove semaphore lock successful`() {
            val clientId = "uuid"
            val pipelined = mockk<Pipeline>()
            every { redis.pipelined() } returns pipelined
            every { pipelined.srem(eq("test-key1"), eq(clientId)) } returns mockk()
            every { pipelined.del(eq("test-key2")) } returns mockk()
            every { pipelined.sync() } returns Unit
            every { pipelined.close() } returns Unit
            val permit = lock.removeSemaphoreLock("test-key1", "test-key2", clientId)

            assertEquals("OK", permit)
            verify(exactly = 1) {
                redis.pipelined()
                pipelined.srem(eq("test-key1"), eq(clientId))
                pipelined.del(eq("test-key2"))
                pipelined.sync()
            }
        }

        @Test
        fun `remove semaphore lock failed`() {
            val clientId = "uuid"
            val pipelined = mockk<Pipeline>()
            every { redis.pipelined() } returns pipelined
            every { pipelined.srem(any<String>(), any()) } returns mockk()
            every { pipelined.del(any<String>()) } returns mockk()
            every { pipelined.sync() } throws IOException("test exception")
            every { pipelined.close() } returns Unit
            val permit = lock.removeSemaphoreLock("test-key1", "test-key2", clientId)

            assertNull(permit)
            verify(exactly = 1) {
                redis.pipelined()
                pipelined.srem(eq("test-key1"), eq(clientId))
                pipelined.del(eq("test-key2"))
                pipelined.sync()
            }
        }
    }

    @Nested
    inner class CleanUpExpiredSemaphoreLocksTests {
        @Test
        fun `clean up semaphore locks successful`() {
            every { redis.evalsha(any(), eq(listOf("test-key")), eq(listOf("test-key-prefix"))) } returns "OK"
            val permit = lock.cleanUpExpiredSemaphoreLocks("test-key", "test-key-prefix")

            assertEquals("OK", permit)
            verify(exactly = 1) {
                redis.evalsha(any(), eq(listOf("test-key")), eq(listOf("test-key-prefix")))
            }
        }

        @Test
        fun `clean up semaphore locks failed`() {
            every { redis.evalsha(any(), eq(listOf("test-key")), eq(listOf("test-key-prefix"))) } returns null
            val permit = lock.cleanUpExpiredSemaphoreLocks("test-key", "test-key-prefix")

            assertNull(permit)
            verify(exactly = 1) {
                redis.evalsha(any(), eq(listOf("test-key")), eq(listOf("test-key-prefix")))
            }
        }

        @Test
        fun `clean up semaphore locks throws exception`() {
            every {
                redis.evalsha(
                    any(),
                    eq(listOf("test-key")),
                    eq(listOf("test-key-prefix")),
                )
            } throws IOException("test exception")
            val permit = lock.cleanUpExpiredSemaphoreLocks("test-key", "test-key-prefix")

            assertNull(permit)
            verify(exactly = 1) {
                redis.evalsha(any(), eq(listOf("test-key")), eq(listOf("test-key-prefix")))
            }
        }
    }
}
