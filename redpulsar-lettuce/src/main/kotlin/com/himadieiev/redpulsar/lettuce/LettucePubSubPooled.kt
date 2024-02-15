package com.himadieiev.redpulsar.lettuce

import com.himadieiev.redpulsar.lettuce.abstracts.AbstractLettucePooled
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.api.sync.BaseRedisCommands
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

/**
 * A handy wrapper for Lettuce Redis Client that provides a simple way to use Pub/Sub functionally
 * without worrying about managing connections.
 * @param connectionPool a pool of connections to redis.
 */
class LettucePubSubPooled<K, V>(
    connectionPool: GenericObjectPool<StatefulRedisPubSubConnection<K, V>>,
) : AbstractLettucePooled<K, V, StatefulRedisPubSubConnection<K, V>>(connectionPool) {
    /**
     * Alternative constructor that uses a supplier and pool config for connections to redis.
     * @param poolConfig a configuration for the pool, argument have a default value.
     * @param connectionSupplier a supplier for connections to redis.
     */
    constructor(
        poolConfig: GenericObjectPoolConfig<StatefulRedisPubSubConnection<K, V>> = GenericObjectPoolConfig(),
        connectionSupplier: () -> StatefulRedisPubSubConnection<K, V>,
    ) : this(ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig))

    /**
     * Executes a block of code with a connection from the pool.
     * Redis command set is represented by [RedisPubSubCommands].
     * @param consumer is a block of code to execute.
     * @return a result of the block.
     */
    @Suppress("UNCHECKED_CAST")
    fun <R> syncPubSub(consumer: (sync: RedisPubSubCommands<K, V>) -> R): R {
        return execute { connection ->
            consumer((connection as StatefulRedisPubSubConnection<*, *>).sync() as RedisPubSubCommands<K, V>)
        }
    }

    override fun <R> sync(consumer: (sync: RedisCommands<K, V>) -> R): R {
        throw UnsupportedOperationException("Pub/Sub connection does not support RedisCommands.")
    }
}
