package com.himadieiev.redpulsar.lettuce.locks.backends

import com.himadieiev.redpulsar.core.common.countDownLatchCountScriptPath
import com.himadieiev.redpulsar.core.common.loadScript
import com.himadieiev.redpulsar.core.locks.abstracts.backends.CountDownLatchBackend
import com.himadieiev.redpulsar.core.utils.failsafe
import com.himadieiev.redpulsar.lettuce.LettucePubSubPooled
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
        val luaScript = loadScript(countDownLatchCountScriptPath)
        return failsafe(null) {
            convertToString(
                redis.sync { sync ->
                    sync.eval(
                        luaScript,
                        ScriptOutputType.STATUS,
                        arrayOf(latchKeyName, channelName),
                        "$clientId$count",
                        ttl.toMillis().toString(),
                        initialCount.toString(),
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
            redis.sync { sync ->
                sync.srem(latchKeyName, "$clientId$count")
            }
        }
    }

    override fun checkCount(latchKeyName: String): Long? {
        return failsafe(null) {
            redis.sync { sync ->
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
                        redis.sync { sync ->
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
