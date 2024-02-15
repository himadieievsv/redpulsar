package com.himadieiev.redpulsar.lettuce

import com.himadieiev.redpulsar.lettuce.abstracts.AbstractLettucePooled
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

/**
 * A handy wrapper for Lettuce Redis Cluster Client that provides a simple way to use functionally
 * without worrying about managing connections.
 * @param connectionPool a pool of connections to redis.
 */
open class LettuceClusterPooled<K, V>(
    connectionPool: GenericObjectPool<StatefulRedisClusterConnection<K, V>>,
) : AbstractLettucePooled<K, V, StatefulRedisClusterConnection<K, V>>(connectionPool) {
    /**
     * Alternative constructor that uses a supplier and pool config for connections to redis.
     * @param poolConfig a configuration for the pool, argument have a default value.
     * @param connectionSupplier a supplier for connections to redis.
     */
    constructor(
        poolConfig: GenericObjectPoolConfig<StatefulRedisClusterConnection<K, V>> = GenericObjectPoolConfig(),
        connectionSupplier: () -> StatefulRedisClusterConnection<K, V>,
    ) : this(ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, poolConfig))
}
