package io.redpulsar.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.commons.pool2.impl.GenericObjectPool
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

class LettucePooledTest {
    @Test
    fun `sync calls borrowObject of pool`() {
        val connectionPool = mockk<GenericObjectPool<StatefulRedisConnection<String, String>>>()
        val lettucePooled = LettucePooled(connectionPool)
        val connection = mockk<StatefulRedisConnection<String, String>>()
        every { connectionPool.borrowObject() } returns connection
        every { connectionPool.returnObject(eq(connection)) } returns Unit
        every { connection.sync() } returns mockk()
        lettucePooled.sync {}

        verify(exactly = 1) {
            connectionPool.borrowObject()
            connectionPool.returnObject(eq(connection))
        }
    }

    @Test
    fun `borrowObject throws exception`() {
        val connectionPool = mockk<GenericObjectPool<StatefulRedisConnection<String, String>>>()
        val lettucePooled = LettucePooled(connectionPool)
        every { connectionPool.borrowObject() } throws IOException()

        assertThrows<LettucePooledException> { lettucePooled.sync {} }
        verify(exactly = 1) { connectionPool.borrowObject() }
    }
}
