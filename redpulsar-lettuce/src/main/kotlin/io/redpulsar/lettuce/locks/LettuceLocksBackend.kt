package io.redpulsar.lettuce.locks

import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import io.redpulsar.core.locks.abstracts.backends.LocksBackend
import io.redpulsar.core.utils.failsafe
import io.redpulsar.lettuce.LettucePooled
import kotlin.time.Duration

internal class LettuceLocksBackend(private val redis: LettucePooled<String, String>) : LocksBackend() {
    override fun setLock(
        resourceName: String,
        clientId: String,
        ttl: Duration,
    ): String? {
        val args = SetArgs().nx().px(ttl.inWholeMilliseconds)
        return failsafe(null) {
            redis.sync { sync -> sync.set(resourceName, clientId, args) }
        }
    }

    override fun removeLock(
        resourceName: String,
        clientId: String,
    ): String? {
        val luaScript =
            """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            end
            return 0
            """.trimIndent()
        return failsafe(null) {
            convertToString(
                redis.sync { sync -> sync.eval(luaScript, ScriptOutputType.INTEGER, arrayOf(resourceName), clientId) },
            )
        }
    }

    override fun setSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
        maxLeases: Int,
        ttl: Duration,
    ): String? {
        val luaScript =
            """
            local maxLeases = tonumber(ARGV[2])
            local leasersCount = tonumber(redis.call("scard", KEYS[1]))
            if leasersCount < maxLeases then
                redis.call("sadd", KEYS[1], ARGV[1])
                redis.call("set", KEYS[2], "", "PX", tonumber(ARGV[3]))
                return "OK"
            end
            return nil
            """.trimIndent()
        return failsafe(null) {
            convertToString(
                redis.sync { sync ->
                    sync.eval(
                        luaScript,
                        ScriptOutputType.VALUE,
                        arrayOf(leasersKey, leaserValidityKey),
                        clientId,
                        maxLeases.toString(),
                        ttl.inWholeMilliseconds.toString(),
                    )
                },
            )
        }
    }

    override fun removeSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
    ): String? =
        failsafe(null) {
            redis.sync { sync ->
                sync.srem(leasersKey, clientId)
                sync.del(leaserValidityKey)
                // Regardless return values success execution counts as operation completed.
                return@sync "OK"
            }
        }

    override fun cleanUpExpiredSemaphoreLocks(
        leasersKey: String,
        leaserValidityKeyPrefix: String,
    ): String? {
        val luaScript =
            """
            local leasersKey = KEYS[1]
            local leasers = redis.call("smembers", leasersKey)
            for _, leaser in ipairs(leasers) do
                local leaserValidityKey = ARGV[1] .. ":" .. leaser
                if redis.call("exists", leaserValidityKey) == 0 then
                    redis.call("srem", leasersKey, leaser)
                end
            end
            return "OK"
            """.trimIndent()
        return failsafe(null) {
            convertToString(
                redis.sync { sync ->
                    sync.eval(
                        luaScript,
                        ScriptOutputType.STATUS,
                        arrayOf(leasersKey),
                        leaserValidityKeyPrefix,
                    )
                },
            )
        }
    }
}
