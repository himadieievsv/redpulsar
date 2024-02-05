package com.himadieiev.redpulsar.jedis.locks.backends

import com.himadieiev.redpulsar.core.common.CLEAN_UP_EXPIRED_SEMAPHORE_LOCKS_SCRIPT_PATH
import com.himadieiev.redpulsar.core.common.REMOVE_LOCK_SCRIPT_PATH
import com.himadieiev.redpulsar.core.common.SET_SEMAPHORE_LOCK_SCRIPT_PATH
import com.himadieiev.redpulsar.core.common.loadScript
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.core.utils.failsafe
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.params.SetParams
import java.time.Duration

/**
 * An implementation of [LocksBackend] that uses Redis as a storage.
 */
internal class JedisLocksBackend(private val jedis: UnifiedJedis) : LocksBackend() {
    override fun setLock(
        resourceName: String,
        clientId: String,
        ttl: Duration,
    ): String? {
        val lockParams = SetParams().nx().px(ttl.toMillis())
        return failsafe(null) { jedis.set(resourceName, clientId, lockParams) }
    }

    override fun removeLock(
        resourceName: String,
        clientId: String,
    ): String? {
        val luaScript = loadScript(REMOVE_LOCK_SCRIPT_PATH)
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
        val luaScript = loadScript(SET_SEMAPHORE_LOCK_SCRIPT_PATH)
        return failsafe(null) {
            convertToString(
                jedis.eval(
                    luaScript,
                    listOf(leasersKey, leaserValidityKey),
                    listOf(clientId, maxLeases.toString(), ttl.toMillis().toString()),
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
        val luaScript = loadScript(CLEAN_UP_EXPIRED_SEMAPHORE_LOCKS_SCRIPT_PATH)
        return failsafe(null) {
            convertToString(jedis.eval(luaScript, listOf(leasersKey), listOf(leaserValidityKeyPrefix)))
        }
    }
}
