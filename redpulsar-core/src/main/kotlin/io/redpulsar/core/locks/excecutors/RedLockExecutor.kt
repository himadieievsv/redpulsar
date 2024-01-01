package io.redpulsar.core.locks.excecutors

import io.redpulsar.core.locks.abstracts.Backend
import io.redpulsar.core.utils.withRetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.Collections
import kotlin.coroutines.cancellation.CancellationException
import kotlin.system.measureTimeMillis
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

internal inline fun <T : Backend, R> redLockExecute(
    backends: List<T>,
    scope: CoroutineScope,
    releaseTime: Duration,
    defaultDrift: Duration = 3.milliseconds,
    crossinline waiter: suspend (jobs: List<Job>, results: MutableList<R>) -> Unit = { jobs, _ -> jobs.joinAll() },
    crossinline callee: suspend (backend: T) -> R,
): List<R> {
    val jobs = mutableListOf<Job>()
    val quorum: Int = backends.size / 2 + 1
    val results = Collections.synchronizedList(mutableListOf<R>())
    val clockDrift =
        (releaseTime.inWholeMilliseconds * 0.01).toLong() + defaultDrift.inWholeMilliseconds * backends.size
    val timeDiff =
        measureTimeMillis {
            backends.forEach { backend ->
                jobs.add(
                    scope.launch {
                        try {
                            val result = callee(backend)
                            if (result != null) {
                                results.add(result)
                            }
                        } catch (e: CancellationException) {
                            val logger = KotlinLogging.logger {}
                            logger.info { "Job canceled: ${e.message}" }
                        }
                    },
                )
            }
            runBlocking { waiter(jobs, results) }
        }
    val validity = releaseTime.inWholeMilliseconds - timeDiff - clockDrift
    if (results.size < quorum || validity < 0) {
        return emptyList()
    }
    return results
}

internal inline fun <T : Backend, R> redLockExecuteWithRetry(
    backends: List<T>,
    scope: CoroutineScope,
    releaseTime: Duration,
    defaultDrift: Duration = 3.milliseconds,
    retryCount: Int = 3,
    retryDelay: Duration = 100.milliseconds,
    crossinline waiter: suspend (jobs: List<Job>, results: MutableList<R>) -> Unit = { jobs, _ -> jobs.joinAll() },
    crossinline callee: suspend (backend: T) -> R,
): List<R> {
    return withRetry(retryCount = retryCount, retryDelay = retryDelay) {
        return@withRetry redLockExecute(
            backends = backends,
            scope = scope,
            releaseTime = releaseTime,
            defaultDrift = defaultDrift,
            waiter = waiter,
            callee = callee,
        )
    }
}

internal fun <T : Backend, R> List<T>.executeWithRetry(
    scope: CoroutineScope,
    releaseTime: Duration,
    defaultDrift: Duration = 3.milliseconds,
    retryCount: Int = 3,
    retryDelay: Duration = 100.milliseconds,
    waiter: suspend (jobs: List<Job>, results: MutableList<R>) -> Unit = { jobs, _ -> jobs.joinAll() },
    callee: suspend (backend: T) -> R,
): List<R> {
    return redLockExecuteWithRetry(
        backends = this,
        scope = scope,
        releaseTime = releaseTime,
        defaultDrift = defaultDrift,
        retryCount = retryCount,
        retryDelay = retryDelay,
        waiter = waiter,
        callee = callee,
    )
}
