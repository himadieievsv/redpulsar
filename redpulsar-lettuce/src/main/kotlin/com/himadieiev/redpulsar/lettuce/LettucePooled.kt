package com.himadieiev.redpulsar.lettuce

import com.himadieiev.redpulsar.lettuce.abstracts.AbstractLettucePooled
import io.lettuce.core.api.StatefulConnection
import io.lettuce.core.api.StatefulRedisConnection
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
) : AbstractLettucePooled<K, V, StatefulRedisConnection<K, V>>(connectionPool) {
    /**
     * Alternative constructor that uses a supplier and pool config for connections to redis.
     * @param poolConfig a configuration for the pool, argument have a default value.
     * @param connectionSupplier a supplier for connections to redis.
     */
    constructor(
        poolConfig: GenericObjectPoolConfig<StatefulRedisConnection<K, V>> = GenericObjectPoolConfig(),
        connectionSupplier: () -> StatefulRedisConnection<K, V>,
    ) : this(ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig))
}
