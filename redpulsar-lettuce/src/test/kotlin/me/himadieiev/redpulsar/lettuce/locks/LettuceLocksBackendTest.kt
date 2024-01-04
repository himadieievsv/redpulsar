package me.himadieiev.redpulsar.lettuce.locks

import equalsTo
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import me.himadieiev.redpulsar.lettuce.LettucePooled
import org.apache.commons.pool2.impl.GenericObjectPool
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.UNIT)
class LettuceLocksBackendTest {
    private lateinit var redis: LettucePooled<String, String>
    private lateinit var backend: LettuceLocksBackend
    private lateinit var sync: RedisCommands<String, String>

    @BeforeEach
    fun setUp() {
        val pool = mockk<GenericObjectPool<StatefulRedisConnection<String, String>>>()
        val connection = mockk<StatefulRedisConnection<String, String>>()
        sync = mockk()
        redis = LettucePooled(pool)
        backend = LettuceLocksBackend(redis)
        every { pool.borrowObject() } returns connection
        every { pool.returnObject(connection) } returns Unit
        every { connection.sync() } returns sync
        every { connection.isMulti } returns false
    }

    @Nested
    inner class SetLockTests {
        @Test
        fun `set lock successful`() {
            val clientId = "uuid"
            every {
                sync.set(
                    eq("test"),
                    eq(clientId),
                    match { it.equalsTo(SetArgs().nx().px(5.seconds.inWholeMilliseconds)) },
                )
            } returns "OK"
            val permit = backend.setLock("test", clientId, 5.seconds)

            assertEquals("OK", permit)
            verify(exactly = 1) {
                sync.set(
                    eq("test"),
                    eq(clientId),
                    match { it.equalsTo(SetArgs().nx().px(5.seconds.inWholeMilliseconds)) },
                )
            }
        }

        @Test
        fun `set lock failed`() {
            val clientId = "uuid"
            every {
                sync.set(
                    eq("test"),
                    eq(clientId),
                    match { it.equalsTo(SetArgs().nx().px(10.seconds.inWholeMilliseconds)) },
                )
            } returns null
            val permit = backend.setLock("test", clientId, 10.seconds)

            assertNull(permit)
        }

        @Test
        fun `set lock throws exception`() {
            val clientId = "uuid"
            every {
                sync.set(
                    eq("test"),
                    eq(clientId),
                    match { it.equalsTo(SetArgs().nx().px(200.milliseconds.inWholeMilliseconds)) },
                )
            } throws IOException("test exception")
            val permit = backend.setLock("test", clientId, 200.milliseconds)

            assertNull(permit)
        }
    }

    @Nested
    inner class RemoveLockTests {
        @Test
        fun `remove lock successful`() {
            val clientId = "uuid"
            every {
                sync.eval<String>(any<String>(), eq(ScriptOutputType.INTEGER), eq(arrayOf("test")), eq(clientId))
            } returns "OK"
            val permit = backend.removeLock("test", clientId)

            assertEquals("OK", permit)
            verify(exactly = 1) {
                sync.eval<String>(any<String>(), eq(ScriptOutputType.INTEGER), eq(arrayOf("test")), eq(clientId))
            }
            verify(exactly = 0) {
                sync.set(any<String>(), any(), any())
            }
        }

        @Test
        fun `remove lock failed`() {
            val clientId = "uuid"
            every {
                sync.eval<String>(any<String>(), eq(ScriptOutputType.INTEGER), eq(arrayOf("test")), eq(clientId))
            } returns null
            val permit = backend.removeLock("test", clientId)

            assertNull(permit)
        }

        @Test
        fun `remove lock throws exception`() {
            val clientId = "uuid"
            every {
                sync.eval<String>(any<String>(), eq(ScriptOutputType.INTEGER), eq(arrayOf("test")), eq(clientId))
            } throws IOException("test exception")
            val permit = backend.removeLock("test", clientId)

            assertNull(permit)
        }
    }

    @Nested
    inner class SetSemaphoreLockTests {
        @Test
        fun `set semaphore lock successful`() {
            val clientId = "uuid"
            every {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.VALUE),
                    eq(arrayOf("test-key1", "test-key2")),
                    eq(clientId),
                    eq("4"),
                    eq("5000"),
                )
            } returns "OK"
            val permit = backend.setSemaphoreLock("test-key1", "test-key2", clientId, 4, 5.seconds)

            assertEquals("OK", permit)
            verify(exactly = 1) {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.VALUE),
                    eq(arrayOf("test-key1", "test-key2")),
                    eq(clientId),
                    eq("4"),
                    eq("5000"),
                )
            }
        }

        @Test
        fun `set semaphore lock failed`() {
            val clientId = "uuid"
            every {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.VALUE),
                    eq(arrayOf("test-key1", "test-key2")),
                    eq(clientId),
                    eq("4"),
                    eq("5000"),
                )
            } returns null
            val permit = backend.setSemaphoreLock("test-key1", "test-key2", clientId, 4, 5.seconds)

            assertNull(permit)
            verify(exactly = 1) {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.VALUE),
                    eq(arrayOf("test-key1", "test-key2")),
                    eq(clientId),
                    eq("4"),
                    eq("5000"),
                )
            }
        }

        @Test
        fun `set semaphore lock throws exceptions`() {
            val clientId = "uuid"
            every {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.VALUE),
                    eq(arrayOf("test-key1", "test-key2")),
                    eq(clientId),
                    eq("10"),
                    eq("100"),
                )
            } throws IOException("test exception")
            val permit = backend.setSemaphoreLock("test-key1", "test-key2", clientId, 10, 100.milliseconds)

            assertNull(permit)
            verify(exactly = 1) {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.VALUE),
                    eq(arrayOf("test-key1", "test-key2")),
                    eq(clientId),
                    eq("10"),
                    eq("100"),
                )
            }
        }
    }

    @Nested
    inner class RemoveSemaphoreLockTests {
        @Test
        fun `remove semaphore lock successful`() {
            val clientId = "uuid"
            every { sync.srem(eq("test-key1"), eq(clientId)) } returns 1
            every { sync.del(eq("test-key2")) } returns 1
            val permit = backend.removeSemaphoreLock("test-key1", "test-key2", clientId)

            assertEquals("OK", permit)
            verify(exactly = 1) {
                sync.srem(eq("test-key1"), eq(clientId))
                sync.del(eq("test-key2"))
            }
        }

        @Test
        fun `remove semaphore lock failed`() {
            val clientId = "uuid"
            every { sync.srem(eq("test-key1"), eq(clientId)) } returns 1
            every { sync.del(eq("test-key2")) } throws IOException("test exception")
            val permit = backend.removeSemaphoreLock("test-key1", "test-key2", clientId)

            assertNull(permit)
            verify(exactly = 1) {
                sync.srem(eq("test-key1"), eq(clientId))
                sync.del(eq("test-key2"))
            }
        }
    }

    @Nested
    inner class CleanUpExpiredSemaphoreLocksTests {
        @Test
        fun `clean up semaphore locks successful`() {
            every {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.STATUS),
                    eq(arrayOf("test-key")),
                    eq("test-key-prefix"),
                )
            } returns "OK"
            val permit = backend.cleanUpExpiredSemaphoreLocks("test-key", "test-key-prefix")

            assertEquals("OK", permit)
            verify(exactly = 1) {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.STATUS),
                    eq(arrayOf("test-key")),
                    eq("test-key-prefix"),
                )
            }
        }

        @Test
        fun `clean up semaphore locks failed`() {
            every {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.STATUS),
                    eq(arrayOf("test-key")),
                    eq("test-key-prefix"),
                )
            } returns null
            val permit = backend.cleanUpExpiredSemaphoreLocks("test-key", "test-key-prefix")

            assertNull(permit)
            verify(exactly = 1) {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.STATUS),
                    eq(arrayOf("test-key")),
                    eq("test-key-prefix"),
                )
            }
        }

        @Test
        fun `clean up semaphore locks throws exception`() {
            every {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.STATUS),
                    eq(arrayOf("test-key")),
                    eq("test-key-prefix"),
                )
            } throws IOException("test exception")
            val permit = backend.cleanUpExpiredSemaphoreLocks("test-key", "test-key-prefix")

            assertNull(permit)
            verify(exactly = 1) {
                sync.eval<String>(
                    any<String>(),
                    eq(ScriptOutputType.STATUS),
                    eq(arrayOf("test-key")),
                    eq("test-key-prefix"),
                )
            }
        }
    }
}
