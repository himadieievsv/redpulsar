package com.himadieiev.redpulsar.lettuce.abstracts

import com.himadieiev.redpulsar.lettuce.exceptions.LettucePooledException
import io.lettuce.core.api.StatefulConnection
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import mu.KotlinLogging
import org.apache.commons.pool2.impl.GenericObjectPool

/**
 * A handy wrapper for Lettuce Redis Client that provides a simple way to use functionally
 * without worrying about managing connections.
 * @param connectionPool a pool of connections to redis.
 */
abstract class AbstractLettucePooled<K, V, T : StatefulConnection<K, V>>(
    protected val connectionPool: GenericObjectPool<T>,
) : AutoCloseable, LettuceUnified<K, V> {
    protected val logger = KotlinLogging.logger { }

    init {
        val connection = connectionPool.borrowObject()
        if (connection !is StatefulRedisConnection<*, *> && connection !is StatefulRedisClusterConnection<*, *>) {
            throw IllegalArgumentException(
                "Connection pool must be of type StatefulRedisConnection or StatefulRedisClusterConnection.",
            )
        }
        connectionPool.returnObject(connection)
    }

    override fun close() {
        connectionPool.close()
    }

    @Suppress("UNCHECKED_CAST")
    override fun <R> sync(consumer: (sync: RedisClusterCommands<K, V>) -> R): R {
        return execute { connection ->
            when (connection) {
                is StatefulRedisConnection<*, *> -> consumer(connection.sync() as RedisClusterCommands<K, V>)
                is StatefulRedisClusterConnection<*, *> -> consumer(connection.sync() as RedisClusterCommands<K, V>)
                else -> throw IllegalStateException("Connection pool of wrong type.")
            }
        }
    }

    protected inline fun <R> execute(block: (StatefulConnection<K, V>) -> R): R {
        val connection =
            try {
                connectionPool.borrowObject()
            } catch (e: Exception) {
                throw LettucePooledException(e, "Could not borrow connection from pool.")
            }
        try {
            return block(connection)
        } finally {
            // Cleaning up connection if a transaction was not handled correctly.
            if (connection is StatefulRedisConnection<*, *> && connection.isMulti) {
                try {
                    connection.sync().discard()
                } catch (e: Exception) {
                    logger.error(e) { "Could not discard a transaction." }
                }
            }
            try {
                connectionPool.returnObject(connection)
            } catch (e: IllegalStateException) {
                logger.info { "Failed to return connection to the pool. Skipping error: " + e.message }
            }
        }
    }
}
