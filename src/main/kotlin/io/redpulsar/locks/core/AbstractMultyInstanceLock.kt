package io.redpulsar.locks.core

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import redis.clients.jedis.UnifiedJedis
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.system.measureTimeMillis
import kotlin.time.Duration

/**
 * A distributed lock implementation based on the Redlock algorithm.
 * Algorithm depends on single or multiple Redis instances / clusters.
 * It uses a quorum to determine if the lock was acquired.
 * Details: https://redis.io/docs/manual/patterns/distributed-locks/
 */
abstract class AbstractMultyInstanceLock(private val instances: List<UnifiedJedis>) : AbstractLock() {
    private val scope = CoroutineScope(Dispatchers.IO)
    private val quorum: Int = instances.size / 2 + 1

    init {
        require(instances.isNotEmpty()) { "Redis instances must not be empty" }
    }

    override fun unlock(resourceName: String) {
        try {
            allInstances { jedis ->
                unlockInstance(jedis, resourceName)
            }
        } catch (e: CancellationException) {
            logger.error(e) { "Unlocking coroutines unexpectedly terminated." }
        }
    }

    protected fun multyLock(
        resourceName: String,
        ttl: Duration,
        defaultDrift: Duration,
        retryCount: Int,
        retryDelay: Duration,
    ): Boolean {
        val clockDrift = (ttl.inWholeMilliseconds * 0.01 + defaultDrift.inWholeMilliseconds).toInt()
        var retries = retryCount
        do {
            val acceptedLocks = AtomicInteger(0)
            val timeDiff =
                measureTimeMillis {
                    allInstances { jedis ->
                        if (lockInstance(jedis, resourceName, ttl)) acceptedLocks.incrementAndGet()
                    }
                }
            val validity = ttl.inWholeMilliseconds - timeDiff - clockDrift
            if (acceptedLocks.get() >= quorum && validity > 0) {
                return true
            } else {
                allInstances { jedis ->
                    unlockInstance(jedis, resourceName)
                }
            }
            runBlocking {
                delay(retryDelay.inWholeMilliseconds)
            }
        } while (--retries > 0)
        return false
    }

    private fun allInstances(block: suspend (jedis: UnifiedJedis) -> Unit) {
        val jobs = mutableListOf<Job>()
        instances.forEach { jedis ->
            jobs.add(
                scope.launch {
                    block(jedis)
                },
            )
        }
        runBlocking { joinAll(*jobs.toTypedArray()) }
    }
}
