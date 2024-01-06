package com.himadieiev.redpulsar.lettuce

import com.himadieiev.redpulsar.lettuce.abstracts.AbstractLettucePooled
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

/**
 * A handy wrapper for Lettuce Redis Client that provides a simple way to use functionally
 * without worrying about managing connections.
 * @param connectionPool a pool of connections to redis.
 */
open class LettucePooled<K, V>(
    connectionPool: GenericObjectPool<StatefulRedisConnection<K, V>>,
) : AbstractLettucePooled<K, V>(connectionPool) {
    /**
     * Alternative constructor that uses a supplier and pool config for connections to redis.
     * @param poolConfig a configuration for the pool, argument have a default value.
     * @param connectionSupplier a supplier for connections to redis.
     */
    constructor(
        poolConfig: GenericObjectPoolConfig<StatefulRedisConnection<K, V>> = GenericObjectPoolConfig(),
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
}
