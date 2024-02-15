package com.himadieiev.redpulsar.lettuce

import com.himadieiev.redpulsar.lettuce.exceptions.LettucePooledException
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
        connection = mockk<StatefulRedisConnection<String, String>>()
        every { connection.isMulti } returns false
        every { connection.close() } returns Unit
        every { connectionPool.borrowObject() } returns connection
        every { connectionPool.returnObject(eq(connection)) } returns Unit
        lettucePooled = LettucePooled(connectionPool)
    }

    @Test
    fun `sync calls success`() {
        every { connection.sync() } returns mockk()
        lettucePooled.sync {}

        verify(exactly = 1) {
            connection.sync()
            connectionPool.returnObject(eq(connection))
        }
        verify(exactly = 2) {
            connectionPool.borrowObject()
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
            connectionPool.returnObject(eq(connection))
        }
        verify(exactly = 2) {
            connectionPool.borrowObject()
            connection.sync()
        }
    }

    @Test
    fun `borrowObject throws exception`() {
        every { connectionPool.borrowObject() } throws IOException()

        assertThrows<LettucePooledException> { lettucePooled.sync {} }
        verify(exactly = 2) { connectionPool.borrowObject() }
    }
}
