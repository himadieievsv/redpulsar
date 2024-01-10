package com.himadieiev.redpulsar.lettuce.abstracts

import com.himadieiev.redpulsar.lettuce.exceptions.LettucePooledException
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.api.sync.RedisCommands
import mu.KotlinLogging
import org.apache.commons.pool2.impl.GenericObjectPool

/**
 * A handy wrapper for Lettuce Redis Client that provides a simple way to use functionally
 * without worrying about managing connections.
 * @param connectionPool a pool of connections to redis.
 */
abstract class AbstractLettucePooled<K, V>(
    protected val connectionPool: GenericObjectPool<StatefulRedisConnection<K, V>>,
) : AutoCloseable {
    override fun close() {
        connectionPool.close()
    }

    protected open fun <R> executeSync(block: (sync: RedisCommands<K, V>) -> R): R {
        return execute { connection -> block(connection.sync()) }
    }

    protected open fun <R> executeAsync(block: (async: RedisAsyncCommands<K, V>) -> R): R {
        return execute { connection -> block(connection.async()) }
    }

    protected open fun <R> executeReactive(block: (reactive: RedisReactiveCommands<K, V>) -> R): R {
        return execute { connection -> block(connection.reactive()) }
    }

    protected inline fun <R> execute(block: (StatefulRedisConnection<K, V>) -> R): R {
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
            if (connection.isMulti) {
                try {
                    connection.sync().discard()
                } catch (e: Exception) {
                    val logger = KotlinLogging.logger { }
                    logger.error(e) { "Could not discard transaction." }
                }
            }
            connectionPool.returnObject(connection)
        }
    }
}
