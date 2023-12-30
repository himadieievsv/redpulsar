package io.redpulsar.core.locks.abstracts

import io.redpulsar.core.locks.api.Lock
import mu.KotlinLogging
import java.util.UUID
import kotlin.time.Duration

/**
 * Common functions for broad range of different locks.
 */
abstract class AbstractLock : Lock {
    protected val logger = KotlinLogging.logger {}
    protected val clientId: String = UUID.randomUUID().toString()

    /**
     * Locks the resource on the given Redis instance.
     */
    protected open fun lockInstance(
        backend: Backend,
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        // val lockParams = SetParams().nx().px(ttl.inWholeMilliseconds)
        // val result = failsafe(null) { instance.set(resourceName, clientId, lockParams) }

        return backend.setLock(resourceName, clientId, ttl) != null
    }

    /**
     * Unlocks the resource on the given Redis instance.
     */
    protected open fun unlockInstance(
        backend: Backend,
        resourceName: String,
    ) {
//        val luaScript =
//            """
//            if redis.call("get", KEYS[1]) == ARGV[1] then
//                return redis.call("del", KEYS[1])
//            end
//            return 0
//            """.trimIndent()
//        failsafe { instance.eval(luaScript, listOf(resourceName), listOf(clientId)) }
        backend.removeLock(resourceName, clientId)
    }
}
