package io.redpulsar.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

class LettucePooled<K, V>(
    private val connectionPool: GenericObjectPool<StatefulRedisConnection<K, V>>,
) : AutoCloseable {
    constructor(
        poolConfig: GenericObjectPoolConfig<StatefulRedisConnection<K, V>>,
        connectionSupplier: () -> StatefulRedisConnection<K, V>,
    ) : this(ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig))

    fun <T> sync(block: (sync: RedisCommands<K, V>) -> T): T {
        return executeSync { sync ->
            block(sync)
        }
    }

    override fun close() {
        connectionPool.close()
    }

    private fun borrowConnection(): StatefulRedisConnection<K, V> {
        try {
            return connectionPool.borrowObject()
        } catch (e: Exception) {
            throw LettucePooledException(e, "Could not borrow connection from pool.")
        }
    }

    private fun <T> executeSync(block: (sync: RedisCommands<K, V>) -> T): T {
        val connection = borrowConnection()
        try {
            return block(connection.sync())
        } finally {
            connectionPool.returnObject(connection)
        }
    }
}

class LettucePooledException(e: Exception, message: String) : RuntimeException(message, e)
