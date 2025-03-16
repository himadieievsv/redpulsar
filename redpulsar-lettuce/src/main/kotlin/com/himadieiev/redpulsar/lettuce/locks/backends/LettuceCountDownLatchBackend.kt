package com.himadieiev.redpulsar.lettuce.locks.backends

import com.himadieiev.redpulsar.core.common.COUNT_DOWN_LATCH_COUNT_SCRIPT_PATH
import com.himadieiev.redpulsar.core.common.loadScript
import com.himadieiev.redpulsar.core.locks.abstracts.backends.CountDownLatchBackend
import com.himadieiev.redpulsar.core.utils.failsafe
import com.himadieiev.redpulsar.lettuce.LettucePubSubPooled
import com.himadieiev.redpulsar.lettuce.evalCashed
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.pubsub.RedisPubSubListener
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.time.Duration

/**
 * An implementation of [CountDownLatchBackend] that uses Redis as a storage.
 */
internal class LettuceCountDownLatchBackend(private val redis: LettucePubSubPooled<String, String>) :
    CountDownLatchBackend() {
    override fun count(
        latchKeyName: String,
        channelName: String,
        clientId: String,
        count: Int,
        initialCount: Int,
        ttl: Duration,
    ): String? {
        val luaScript = loadScript(COUNT_DOWN_LATCH_COUNT_SCRIPT_PATH)
        return failsafe(null) {
            convertToString(
                redis.syncPubSub { sync ->
                    sync.evalCashed(
                        luaScript,
                        ScriptOutputType.STATUS,
                        arrayOf(latchKeyName, channelName),
                        arrayOf("$clientId$count", ttl.toMillis().toString(), initialCount.toString()),
                    )
                },
            )
        }
    }

    override fun undoCount(
        latchKeyName: String,
        clientId: String,
        count: Int,
    ): Long? {
        return failsafe(null) {
            redis.syncPubSub { sync ->
                sync.srem(latchKeyName, "$clientId$count")
            }
        }
    }

    override fun checkCount(latchKeyName: String): Long? {
        return failsafe(null) {
            redis.syncPubSub { sync ->
                sync.scard(latchKeyName)
            }
        }
    }

    override suspend fun listen(channelName: String): String? {
        return failsafe(null) {
            callbackFlow {
                val pubSub =
                    object : RedisPubSubListener<String, String> {
                        override fun message(
                            channel: String?,
                            message: String?,
                        ) {
                            message?.let {
                                if (message == "open") trySend(it)
                            }
                        }

                        override fun message(
                            pattern: String?,
                            channel: String?,
                            message: String?,
                        ) {
                        }

                        override fun subscribed(
                            channel: String?,
                            count: Long,
                        ) {
                        }

                        override fun psubscribed(
                            pattern: String?,
                            count: Long,
                        ) {
                        }

                        override fun unsubscribed(
                            channel: String?,
                            count: Long,
                        ) {
                        }

                        override fun punsubscribed(
                            pattern: String?,
                            count: Long,
                        ) {
                        }
                    }
                val job =
                    launch {
                        redis.syncPubSub { sync ->
                            sync.statefulConnection.addListener(pubSub)
                            sync.subscribe(channelName)
                            while (isActive) {
                                Thread.sleep(20)
                            }
                            sync.statefulConnection.removeListener(pubSub)
                            sync.unsubscribe(channelName)
                        }
                    }
                awaitClose {
                    job.cancel()
                }
            }.first()
        }
    }
}
