package me.himadieiev.redpulsar.lettuce

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import me.himadieiev.redpulsar.lettuce.exceptions.LettucePooledException
import org.apache.commons.pool2.impl.GenericObjectPool
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

@Tag(TestTags.UNIT)
class LettucePubSubPooledTest {
    private lateinit var connectionPool: GenericObjectPool<StatefulRedisPubSubConnection<String, String>>
    private lateinit var lettucePubSubPooled: LettucePubSubPooled<String, String>
    private lateinit var connection: StatefulRedisPubSubConnection<String, String>

    @BeforeEach
    fun setUp() {
        connectionPool = mockk<GenericObjectPool<StatefulRedisPubSubConnection<String, String>>>()
        lettucePubSubPooled = LettucePubSubPooled(connectionPool)
        connection = mockk<StatefulRedisPubSubConnection<String, String>>()
        every { connection.isMulti } returns false
        every { connectionPool.borrowObject() } returns connection
        every { connectionPool.returnObject(eq(connection)) } returns Unit
    }

    @Test
    fun `sync calls success`() {
        every { connection.sync() } returns mockk()
        lettucePubSubPooled.sync { }

        verify(exactly = 1) {
            connection.sync()
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
    }

    @Test
    fun `will close opened transaction`() {
        val sync = mockk<RedisPubSubCommands<String, String>>()
        every { connection.isMulti } returns true
        every { connection.sync() } returns sync
        every { sync.discard() } returns "OK"
        lettucePubSubPooled.sync {}

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
        lettucePubSubPooled.async {}

        verify(exactly = 1) {
            connection.async()
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
    }

    @Test
    fun `reactive calls success`() {
        every { connection.reactive() } returns mockk()
        lettucePubSubPooled.reactive {}

        verify(exactly = 1) {
            connection.reactive()
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
    }

    @Test
    fun `borrowObject throws exception`() {
        every { connectionPool.borrowObject() } throws IOException()

        assertThrows<LettucePooledException> { lettucePubSubPooled.sync {} }
        verify(exactly = 1) { connectionPool.borrowObject() }
    }
}
