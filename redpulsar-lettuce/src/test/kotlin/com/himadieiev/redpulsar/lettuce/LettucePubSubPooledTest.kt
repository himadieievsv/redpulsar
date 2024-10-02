package com.himadieiev.redpulsar.lettuce

import com.himadieiev.redpulsar.lettuce.exceptions.LettucePooledException
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands
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
class LettucePubSubPooledTest {
    private lateinit var connectionPool: GenericObjectPool<StatefulRedisPubSubConnection<String, String>>
    private lateinit var lettucePubSubPooled: LettucePubSubPooled<String, String>
    private lateinit var connection: StatefulRedisPubSubConnection<String, String>

    @BeforeEach
    fun setUp() {
        connectionPool = mockk<GenericObjectPool<StatefulRedisPubSubConnection<String, String>>>()
        connection = mockk<StatefulRedisPubSubConnection<String, String>>()
        every { connection.isMulti } returns false
        every { connection.close() } returns Unit
        every { connectionPool.borrowObject() } returns connection
        every { connectionPool.returnObject(eq(connection)) } returns Unit
        lettucePubSubPooled = LettucePubSubPooled(connectionPool)
    }

    @Test
    fun `sync calls success`() {
        every { connection.sync() } returns mockk()
        lettucePubSubPooled.syncPubSub { }

        verify(exactly = 1) {
            connection.sync()
        }
        verify(exactly = 2) {
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
        lettucePubSubPooled.syncPubSub {}

        verify(exactly = 1) {
            sync.discard()
        }
        verify(exactly = 2) {
            connection.sync()
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
    }

    @Test
    fun `borrowObject throws exception`() {
        every { connectionPool.borrowObject() } throws IOException()

        assertThrows<LettucePooledException> { lettucePubSubPooled.syncPubSub {} }
        verify(exactly = 2) { connectionPool.borrowObject() }
    }

    @Test
    fun `sync operation is disabled`() {
        assertThrows<UnsupportedOperationException> { lettucePubSubPooled.sync {} }
        verify(exactly = 1) { connectionPool.borrowObject() }
    }
}
