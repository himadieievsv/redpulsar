package io.redpulsar.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.commons.pool2.impl.GenericObjectPool
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

@Tag(TestTags.UNIT)
class LettucePooledTest {
    private lateinit var connectionPool: GenericObjectPool<StatefulRedisConnection<String, String>>
    private lateinit var lettucePooled: LettucePooled<String, String>
    private lateinit var connection: StatefulRedisConnection<String, String>

    @BeforeEach
    fun setUp() {
        connectionPool = mockk<GenericObjectPool<StatefulRedisConnection<String, String>>>()
        lettucePooled = LettucePooled(connectionPool)
        connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.isMulti } returns false
        every { connectionPool.borrowObject() } returns connection
        every { connectionPool.returnObject(eq(connection)) } returns Unit
    }

    @Test
    fun `sync calls success`() {
        every { connection.sync() } returns mockk()
        lettucePooled.sync {}

        verify(exactly = 1) {
            connection.sync()
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
    }

    @Test
    fun `will close opened transaction`() {
        val sync = mockk<RedisCommands<String, String>>()
        every { connection.isMulti } returns true
        every { connection.sync() } returns sync
        every { sync.discard() } returns "OK"
        lettucePooled.sync {}

        verify(exactly = 1) {
            sync.discard()
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
        verify(exactly = 2) { connection.sync() }
    }

    @Test
    fun `async calls success`() {
        every { connection.async() } returns mockk()
        lettucePooled.async {}

        verify(exactly = 1) {
            connection.async()
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
    }

    @Test
    fun `reactive calls success`() {
        every { connection.reactive() } returns mockk()
        lettucePooled.reactive {}

        verify(exactly = 1) {
            connection.reactive()
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
    }

    @Test
    fun `borrowObject throws exception`() {
        every { connectionPool.borrowObject() } throws IOException()

        assertThrows<LettucePooledException> { lettucePooled.sync {} }
        verify(exactly = 1) { connectionPool.borrowObject() }
    }
}
