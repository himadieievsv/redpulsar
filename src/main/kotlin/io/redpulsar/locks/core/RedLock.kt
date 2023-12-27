package io.redpulsar.locks.core

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import redis.clients.jedis.UnifiedJedis
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

private val logger = KotlinLogging.logger {}

/**
 * A distributed lock implementation based on the Redlock algorithm.
 * [RedLock] depends on multiple single Redis instances or clusters to be available.
 * It uses a quorum to determine if the lock was acquired.
 * Details: https://redis.io/docs/manual/patterns/distributed-locks/
 */
class RedLock(
    private val instances: List<UnifiedJedis>,
    private val retryDelay: Duration = 200.milliseconds,
    private val retryCount: Int = 3,
) : AbstractLock() {
    private val quorum: Int = instances.size / 2 + 1
    private val scope = CoroutineScope(Dispatchers.IO)

    init {
        require(instances.isNotEmpty()) { "Redis instances must not be empty" }
        require(retryDelay > 0.milliseconds) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    override fun lock(
        resourceName: String,
        ttl: Duration,
    ): Boolean {
        require(ttl > 2.milliseconds) { "Timeout must be greater that min clock drift." }
        try {
            // Clock drift is defined as 1% of ttl + 2 milliseconds
            val clockDrift = (ttl.inWholeMilliseconds * 0.01 + 2).toInt()
            var retries = retryCount
            do {
                val acceptedLocks = AtomicInteger(0)
                val t1 = System.currentTimeMillis()
                allInstances { jedis ->
                    if (lockInstance(jedis, resourceName, ttl)) acceptedLocks.incrementAndGet()
                }
                val t2 = System.currentTimeMillis()
                val validity = ttl.inWholeMilliseconds - (t2 - t1) - clockDrift
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
        } catch (e: CancellationException) {
            logger.error(e) { "Coroutines unexpectedly terminated." }
            return false
        }
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
