package com.himadieiev.redpulsar.jedis.locks.backends

import com.himadieiev.redpulsar.core.common.countDownLatchCountScriptPath
import com.himadieiev.redpulsar.core.common.loadScript
import com.himadieiev.redpulsar.core.locks.abstracts.backends.CountDownLatchBackend
import com.himadieiev.redpulsar.core.utils.failsafe
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import mu.KotlinLogging
import redis.clients.jedis.JedisPubSub
import redis.clients.jedis.UnifiedJedis
import java.time.Duration

/**
 * An implementation of [CountDownLatchBackend] that uses Redis as a storage.
 */
internal class JedisCountDownLatchBackend(private val jedis: UnifiedJedis) : CountDownLatchBackend() {
    override fun count(
        latchKeyName: String,
        channelName: String,
        clientId: String,
        count: Int,
        initialCount: Int,
        ttl: Duration,
    ): String? {
        val luaScript = loadScript(countDownLatchCountScriptPath)
        return failsafe(null) {
            convertToString(
                jedis.eval(
                    luaScript,
                    listOf(latchKeyName, channelName),
                    listOf("$clientId$count", ttl.toMillis().toString(), initialCount.toString()),
                ),
            )
        }
    }

    override fun undoCount(
        latchKeyName: String,
        clientId: String,
        count: Int,
    ): Long? {
        return failsafe(null) {
            jedis.srem(latchKeyName, "$clientId$count")
        }
    }

    override fun checkCount(latchKeyName: String): Long? {
        return failsafe(null) {
            jedis.scard(latchKeyName)
        }
    }

    override suspend fun listen(channelName: String): String? {
        return failsafe(null) {
            callbackFlow {
                val pubSub =
                    object : JedisPubSub() {
                        override fun onMessage(
                            channel: String?,
                            message: String?,
                        ) {
                            message?.let {
                                if (message == "open") trySend(it)
                            }
                        }
                    }
                val job = launch { jedis.subscribe(pubSub, channelName) }
                awaitClose {
                    try {
                        pubSub.unsubscribe(channelName)
                    } catch (e: Exception) {
                        // suppress unsubscribe errors as it might be uew to lost connection
                        val logger = KotlinLogging.logger {}
                        logger.info { "Unsubscribe failed: ${e.message}" }
                    }
                    job.cancel()
                }
            }.first()
        }
    }
}
