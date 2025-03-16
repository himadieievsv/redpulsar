package com.himadieiev.redpulsar.lettuce.locks.backends

import com.himadieiev.redpulsar.core.common.CLEAN_UP_EXPIRED_SEMAPHORE_LOCKS_SCRIPT_PATH
import com.himadieiev.redpulsar.core.common.REMOVE_LOCK_SCRIPT_PATH
import com.himadieiev.redpulsar.core.common.SET_SEMAPHORE_LOCK_SCRIPT_PATH
import com.himadieiev.redpulsar.core.common.loadScript
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.core.utils.failsafe
import com.himadieiev.redpulsar.lettuce.abstracts.LettuceUnified
import com.himadieiev.redpulsar.lettuce.evalCashed
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import java.time.Duration

/**
 * An implementation of [LocksBackend] that uses Redis as a storage.
 */
internal class LettuceLocksBackend(private val redis: LettuceUnified<String, String>) : LocksBackend() {
    override fun setLock(
        resourceName: String,
        clientId: String,
        ttl: Duration,
    ): String? {
        val args = SetArgs().nx().px(ttl.toMillis())
        return failsafe(null) {
            redis.sync { sync -> sync.set(resourceName, clientId, args) }
        }
    }

    override fun removeLock(
        resourceName: String,
        clientId: String,
    ): String? {
        val luaScript = loadScript(REMOVE_LOCK_SCRIPT_PATH)
        return failsafe(null) {
            convertToString(
                redis.sync { sync ->
                    sync.evalCashed(
                        luaScript,
                        ScriptOutputType.INTEGER,
                        arrayOf(resourceName),
                        arrayOf(clientId),
                    )
                },
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
        val luaScript = loadScript(SET_SEMAPHORE_LOCK_SCRIPT_PATH)
        return failsafe(null) {
            convertToString(
                redis.sync { sync ->
                    sync.evalCashed(
                        luaScript,
                        ScriptOutputType.VALUE,
                        arrayOf(leasersKey, leaserValidityKey),
                        arrayOf(clientId, maxLeases.toString(), ttl.toMillis().toString()),
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
        val luaScript = loadScript(CLEAN_UP_EXPIRED_SEMAPHORE_LOCKS_SCRIPT_PATH)
        return failsafe(null) {
            convertToString(
                redis.sync { sync ->
                    sync.evalCashed(
                        luaScript,
                        ScriptOutputType.STATUS,
                        arrayOf(leasersKey),
                        arrayOf(leaserValidityKeyPrefix),
                    )
                },
            )
        }
    }
}
