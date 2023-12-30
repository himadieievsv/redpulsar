package io.redpulsar.core.locks

import io.redpulsar.core.locks.abstracts.AbstractMultyInstanceLock
import io.redpulsar.core.locks.abstracts.Backend
import io.redpulsar.core.utils.failsafe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * An implementation for Semaphore lock in distributed systems.
 * Semaphore lock is a lock that allows multiple clients to acquire the lock until limit reached.
 */
class Semaphore(
    backends: List<Backend>,
    private val maxLeases: Int,
    private val retryDelay: Duration = 200.milliseconds,
    private val retryCount: Int = 3,
    scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
) : AbstractMultyInstanceLock(backends, scope) {
    private val globalKeyPrefix = "semaphore"
    private val leasersKey = "leasers"

    init {
        require(maxLeases > 0) { "Max leases should be positive number" }
        require(retryDelay > 0.milliseconds) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        val minTtl = 10.milliseconds
        // ttl is longer that other locks because process of acquiring lock is longer.
        require(ttl > minTtl) { "Timeout is too small." }
        return try {
            multyLock(resourceName, ttl, minTtl, retryCount, retryDelay)
        } catch (e: CancellationException) {
            logger.error(e) { "Coroutines unexpectedly terminated." }
            false
        }
    }

    override fun lockInstance(
        backend: Backend,
        resourceName: String,
        ttl: Duration,
    ): Boolean {
//        val luaScript =
//            """
//            local maxLeases = tonumber(ARGV[2])
//            local leasersCount = tonumber(redis.call("scard", KEYS[1]))
//            if leasersCount < maxLeases then
//                redis.call("sadd", KEYS[1], ARGV[1])
//                redis.call("set", KEYS[2], "", "PX", tonumber(ARGV[3]))
//                return "OK"
//            end
//            return nil
//            """.trimIndent()
//        val result =
//            failsafe(null) {
//                val leasersKey = buildKey(leasersKey, resourceName)
//                val leaserValidityKey = buildKey(resourceName, clientId)
//                instance.eval(
//                    luaScript,
//                    listOf(leasersKey, leaserValidityKey),
//                    listOf(clientId, maxLeases.toString(), ttl.inWholeMilliseconds.toString()),
//                )
//            }
        val leasersKey = buildKey(leasersKey, resourceName)
        val leaserValidityKey = buildKey(resourceName, clientId)
        return backend.setSemaphoreLock(leasersKey, leaserValidityKey, clientId, maxLeases, ttl) != null
    }

    override fun unlockInstance(
        backend: Backend,
        resourceName: String,
    ) {
//        failsafe {
//            instance.pipelined().use { redis ->
//                redis.srem(buildKey(leasersKey, resourceName), clientId)
//                redis.del(buildKey(resourceName, clientId))
//                redis.sync()
//            }
//        }
        val leasersKey = buildKey(leasersKey, resourceName)
        val leaserValidityKey = buildKey(resourceName, clientId)
        backend.removeSemaphoreLock(leasersKey, leaserValidityKey, clientId)
        // clean up expired other leasers
        cleanUp(backend, resourceName)
    }

    private fun cleanUp(
        backend: Backend,
        resourceName: String,
    ) {
//        val luaScript =
//            """
//            local leasersKey = KEYS[1]
//            local leasers = redis.call("smembers", leasersKey)
//            for _, leaser in ipairs(leasers) do
//                local leaserValidityKey = ARGV[1] .. ":" .. leaser
//                if redis.call("exists", leaserValidityKey) == 0 then
//                    redis.call("srem", leasersKey, leaser)
//                end
//            end
//            """.trimIndent()
//        failsafe(null) {
//
//            instance.eval(luaScript, listOf(leasersKey), listOf(leaserValidityKeyPrefix))
//        }
        val leasersKey = buildKey(leasersKey, resourceName)
        val leaserValidityKeyPrefix = buildKey(resourceName)
        backend.cleanUpExpiredSemaphoreLocks(leasersKey, leaserValidityKeyPrefix)
    }

    private inline fun buildKey(vararg parts: String) = globalKeyPrefix + ":" + parts.joinToString(":")
}
