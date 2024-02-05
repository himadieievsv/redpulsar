package com.himadieiev.redpulsar.lettuce.locks.backends

import com.himadieiev.redpulsar.core.common.cleanUpExpiredSemaphoreLocksScriptPath
import com.himadieiev.redpulsar.core.common.loadScript
import com.himadieiev.redpulsar.core.common.removeLockScriptPath
import com.himadieiev.redpulsar.core.common.setSemaphoreLockScriptPath
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.core.utils.failsafe
import com.himadieiev.redpulsar.lettuce.LettucePooled
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import java.time.Duration

/**
 * An implementation of [LocksBackend] that uses Redis as a storage.
 */
internal class LettuceLocksBackend(private val redis: LettucePooled<String, String>) : LocksBackend() {
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
        val luaScript = loadScript(removeLockScriptPath)
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
        val luaScript = loadScript(setSemaphoreLockScriptPath)
        return failsafe(null) {
            convertToString(
                redis.sync { sync ->
                    sync.eval(
                        luaScript,
                        ScriptOutputType.VALUE,
                        arrayOf(leasersKey, leaserValidityKey),
                        clientId,
                        maxLeases.toString(),
                        ttl.toMillis().toString(),
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
        val luaScript = loadScript(cleanUpExpiredSemaphoreLocksScriptPath)
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
