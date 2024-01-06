package com.himadieiev.redpulsar.core.locks.abstracts

import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.system.measureTimeMillis

/**
 * A distributed lock implementation based on the Redlock algorithm.
 * Algorithm depends on single or multiple Redis instances / clusters.
 * It uses a quorum to determine if the lock was acquired.
 * Details: https://redis.io/docs/manual/patterns/distributed-locks/
 */
abstract class AbstractMultyInstanceLock(
    private val backends: List<LocksBackend>,
    private val scope: CoroutineScope,
) : AbstractLock() {
    private val quorum: Int = backends.size / 2 + 1

    init {
        require(backends.isNotEmpty()) { "Redis instances must not be empty" }
    }

    override fun unlock(resourceName: String) {
        allInstances { backend ->
            unlockInstance(backend, resourceName)
        }
    }

    protected fun multyLock(
        resourceName: String,
        ttl: Duration,
        defaultDrift: Duration,
        retryCount: Int,
        retryDelay: Duration,
    ): Boolean {
        val clockDrift = (ttl.toMillis() * 0.01 + defaultDrift.toMillis()).toInt()
        var retries = retryCount
        do {
            val acceptedLocks = AtomicInteger(0)
            val timeDiff =
                measureTimeMillis {
                    allInstances { backend ->
                        if (lockInstance(backend, resourceName, ttl)) acceptedLocks.incrementAndGet()
                    }
                }
            val validity = ttl.toMillis() - timeDiff - clockDrift
            if (acceptedLocks.get() >= quorum && validity > 0) {
                return true
            } else {
                allInstances { backend ->
                    unlockInstance(backend, resourceName)
                }
            }
            runBlocking {
                delay(retryDelay.toMillis())
            }
        } while (--retries > 0)
        return false
    }

    private fun allInstances(block: suspend (backend: LocksBackend) -> Unit) {
        val jobs = mutableListOf<Job>()
        backends.forEach { backend ->
            jobs.add(
                scope.launch {
                    try {
                        block(backend)
                    } catch (e: CancellationException) {
                        logger.error(e) { "Coroutines unexpectedly terminated." }
                    }
                },
            )
        }
        runBlocking { joinAll(*jobs.toTypedArray()) }
    }
}
