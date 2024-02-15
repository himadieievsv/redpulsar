package com.himadieiev.redpulsar.lettuce.abstracts

import io.lettuce.core.api.sync.RedisCommands

interface LettuceUnified<K, V> {
    fun <R> sync(consumer: (sync: RedisCommands<K, V>) -> R): R
}
