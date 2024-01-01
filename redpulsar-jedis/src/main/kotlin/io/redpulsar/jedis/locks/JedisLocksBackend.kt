package io.redpulsar.jedis.locks

import io.redpulsar.core.locks.abstracts.backends.LocksBackend
import io.redpulsar.core.utils.failsafe
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.params.SetParams
import kotlin.time.Duration

internal class JedisLocksBackend(private val jedis: UnifiedJedis) : LocksBackend() {
    override fun setLock(
        resourceName: String,
        clientId: String,
        ttl: Duration,
    ): String? {
        val lockParams = SetParams().nx().px(ttl.inWholeMilliseconds)
        return failsafe(null) { jedis.set(resourceName, clientId, lockParams) }
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
            return nil
            """.trimIndent()
        return failsafe(null) {
            convertToString(jedis.eval(luaScript, listOf(resourceName), listOf(clientId)))
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
                jedis.eval(
                    luaScript,
                    listOf(leasersKey, leaserValidityKey),
                    listOf(clientId, maxLeases.toString(), ttl.inWholeMilliseconds.toString()),
                ),
            )
        }
    }

    override fun removeSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
    ): String? =
        failsafe(null) {
            jedis.pipelined().use { pipe ->
                pipe.srem(leasersKey, clientId)
                pipe.del(leaserValidityKey)
                pipe.sync()
            }
            // Regardless return values success execution counts as operation completed.
            return "OK"
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
            """.trimIndent()
        return failsafe(null) {
            convertToString(jedis.eval(luaScript, listOf(leasersKey), listOf(leaserValidityKeyPrefix)))
        }
    }
}
