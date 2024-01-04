package io.redpulsar.lettuce.locks

import io.lettuce.core.ScriptOutputType
import io.lettuce.core.pubsub.RedisPubSubListener
import io.redpulsar.core.locks.abstracts.backends.CountDownLatchBackend
import io.redpulsar.core.utils.failsafe
import io.redpulsar.lettuce.LettucePubSubPooled
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.time.Duration

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
        // ARGV[1] - clientId + count
        // ARGV[2] - ttl.inWholeMilliseconds
        // ARGV[3] - initialCount
        val luaScript =
            """
            redis.call("sadd", KEYS[1], ARGV[1])
            if redis.call("pttl", KEYS[1]) == -1 then
                redis.call("pexpire", KEYS[1], ARGV[2])
            else
                redis.call("pexpire", KEYS[1], ARGV[2], "GT")
            end
            local elementsCount = redis.call("scard", KEYS[1])
            if elementsCount >= tonumber(ARGV[3]) then
                redis.call("publish", KEYS[2], "open")
            end
            return "OK"
            """.trimIndent()
        return failsafe(null) {
            convertToString(
                redis.sync { sync ->
                    sync.eval(
                        luaScript,
                        ScriptOutputType.STATUS,
                        arrayOf(latchKeyName, channelName),
                        "$clientId$count",
                        ttl.inWholeMilliseconds.toString(),
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

    override fun listen(channelName: String): Flow<String> {
        val flow =
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
            }
        return flow
    }
}
