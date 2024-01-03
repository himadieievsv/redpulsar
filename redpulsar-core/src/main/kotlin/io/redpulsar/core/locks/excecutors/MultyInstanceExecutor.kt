package io.redpulsar.core.locks.excecutors

import io.redpulsar.core.locks.abstracts.Backend
import io.redpulsar.core.utils.withRetry
import io.redpulsar.core.utils.withTimeoutInThread
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.system.measureTimeMillis
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * Coroutine used buy callee must be cooperative coroutine (not blocking).
 * In order to cancel jobs forcefully, use [withTimeoutInThread] instead.
 */
inline fun <T : Backend, R> multyInstanceExecute(
    backends: List<T>,
    scope: CoroutineScope,
    releaseTime: Duration,
    defaultDrift: Duration = 3.milliseconds,
    crossinline waiter: suspend (jobs: List<Job>, results: MutableList<R>) -> Unit = ::waitAllJobs,
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
                        val result = callee(backend)
                        if (result != null) {
                            results.add(result)
                        }
                    },
                )
            }
            runBlocking(scope.coroutineContext) { waiter(jobs, results) }
        }
    val validity = releaseTime.inWholeMilliseconds - timeDiff - clockDrift
    if (results.size < quorum || validity < 0) {
        return emptyList()
    }
    return results
}

inline fun <T : Backend, R> multyInstanceExecuteWithRetry(
    backends: List<T>,
    scope: CoroutineScope,
    releaseTime: Duration,
    defaultDrift: Duration = 3.milliseconds,
    retryCount: Int = 3,
    retryDelay: Duration = 100.milliseconds,
    crossinline waiter: suspend (jobs: List<Job>, results: MutableList<R>) -> Unit = ::waitAllJobs,
    crossinline callee: suspend (backend: T) -> R,
): List<R> {
    return withRetry(retryCount = retryCount, retryDelay = retryDelay) {
        return@withRetry multyInstanceExecute(
            backends = backends,
            scope = scope,
            releaseTime = releaseTime,
            defaultDrift = defaultDrift,
            waiter = waiter,
            callee = callee,
        )
    }
}

fun <T : Backend, R> List<T>.executeWithRetry(
    scope: CoroutineScope,
    releaseTime: Duration,
    defaultDrift: Duration = 3.milliseconds,
    retryCount: Int = 3,
    retryDelay: Duration = 100.milliseconds,
    waiter: suspend (jobs: List<Job>, results: MutableList<R>) -> Unit = ::waitAllJobs,
    callee: suspend (backend: T) -> R,
): List<R> {
    return multyInstanceExecuteWithRetry(
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
