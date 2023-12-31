package io.redpulsar.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

/**
 * A handy wrapper for Lettuce Redis Client that provides a simple way to use functionally
 * without warring about managing connections.
 * @param connectionPool a pool of connections to redis.
 */
class LettucePooled<K, V>(
    private val connectionPool: GenericObjectPool<StatefulRedisConnection<K, V>>,
) : AutoCloseable {
    /**
     * Alternative constructor that uses a supplier and pool config for connections to redis.
     * @param poolConfig a configuration for the pool.
     * @param connectionSupplier a supplier for connections to redis.
     */
    constructor(
        poolConfig: GenericObjectPoolConfig<StatefulRedisConnection<K, V>>,
        connectionSupplier: () -> StatefulRedisConnection<K, V>,
    ) : this(ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig))

    /**
     * Executes a block of code with a connection from the pool.
     * Redis command set is represented by [RedisCommands].
     * @param consumer is a block of code to execute.
     * @return a result of the block.
     */
    fun <R> sync(consumer: (sync: RedisCommands<K, V>) -> R): R {
        return executeSync { sync ->
            consumer(sync)
        }
    }

    /**
     * Executes a block of code with a connection from the pool.
     * Redis command set is represented by [RedisAsyncCommands].
     * @param consumer is a block of code to execute.
     * @return a result of the block.
     */
    fun <R> async(consumer: (async: RedisAsyncCommands<K, V>) -> R): R {
        return executeAsync { async ->
            consumer(async)
        }
    }

    /**
     * Executes a block of code with a connection from the pool.
     * Redis command set is represented by [RedisReactiveCommands].
     * @param consumer is a block of code to execute.
     * @return a result of the block.
     */
    fun <R> reactive(consumer: (reactive: RedisReactiveCommands<K, V>) -> R): R {
        return executeReactive { reactive ->
            consumer(reactive)
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

    private fun <R> executeSync(block: (sync: RedisCommands<K, V>) -> R): R {
        return execute { connection -> block(connection.sync()) }
    }

    private fun <R> executeAsync(block: (async: RedisAsyncCommands<K, V>) -> R): R {
        return execute { connection -> block(connection.async()) }
    }

    private fun <R> executeReactive(block: (reactive: RedisReactiveCommands<K, V>) -> R): R {
        return execute { connection -> block(connection.reactive()) }
    }

    private fun <R> execute(block: (sync: StatefulRedisConnection<K, V>) -> R): R {
        val connection = borrowConnection()
        try {
            return block(connection)
        } finally {
            // Cleaning up connection if a transaction was not handled correctly.
            if (connection.isMulti) {
                connection.sync().discard()
            }
            connectionPool.returnObject(connection)
        }
    }
}

class LettucePooledException(e: Exception, message: String) : RuntimeException(message, e)
