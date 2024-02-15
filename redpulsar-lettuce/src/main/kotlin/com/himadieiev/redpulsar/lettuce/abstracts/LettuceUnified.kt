package com.himadieiev.redpulsar.lettuce.abstracts

import io.lettuce.core.cluster.api.sync.RedisClusterCommands

interface LettuceUnified<K, V> {
    fun <R> sync(consumer: (sync: RedisClusterCommands<K, V>) -> R): R
}
