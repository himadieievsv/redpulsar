package com.himadieiev.redpulsar.core.locks.excecutors

import com.himadieiev.redpulsar.core.locks.abstracts.Backend
import com.himadieiev.redpulsar.core.utils.withRetry
import com.himadieiev.redpulsar.core.utils.withTimeoutInThread
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import java.time.Duration
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

/**
 * An algorithm for running closure on multiple remote instances proxied by [backends].
 * Each call will be executed in separate [Job] and wait for the result using one of two self-explanatory strategies:
 * [WaitStrategy.ALL] and [WaitStrategy.MAJORITY].
 * Also, it checks whether the result is successful on majority (depends on waiting strategy) of instances and time
 * spend for getting results is not exceeding some reasonable time difference using [timeout] and
 * clock drift.
 * It returns list of results from each instance or empty list if either time validity wasn't met or operation was
 * failing on majority of instances.
 *
 * Coroutine used by callee must be cooperative coroutine (not blocking).
 * In order to cancel jobs forcefully, use [withTimeoutInThread] instead.
 *
 * @param backends [List] of [Backend] instances.
 * @param scope [CoroutineScope] the scope to run coroutine in.
 * @param timeout [Duration] the maximum time to wait.
 * @param defaultDrift [Duration] the default clock drift.
 * @param waitStrategy [WaitStrategy] the strategy to wait for results.
 * @param cleanUp [Function] the function to clean up resources on each backend.
 * @param callee [Function] the function to call on each backend.
 */
@OptIn(ExperimentalCoroutinesApi::class)
suspend inline fun <T : Backend, R> multiInstanceExecute(
    backends: List<T>,
    scope: CoroutineScope,
    timeout: Duration,
    defaultDrift: Duration = Duration.ofMillis(3),
    waitStrategy: WaitStrategy = WaitStrategy.ALL,
    crossinline cleanUp: (backend: T) -> Unit = { _ -> },
    crossinline callee: suspend (backend: T) -> R,
): List<R> {
    val jobs = mutableListOf<Deferred<R>>()
    val quorum = backends.size / 2 + 1
    val results = Collections.synchronizedList(mutableListOf<R>())
    val clockDrift = (timeout.toMillis() * 0.01).toLong() + defaultDrift.toMillis()
    val t1 = System.currentTimeMillis()
    backends.forEach { backend ->
        jobs.add(
            scope.async { callee(backend) },
        )
    }
    val succeed = AtomicInteger(0)
    val failed = AtomicInteger(0)
    jobs.forEach { job ->
        job.invokeOnCompletion { cause ->
            if (cause == null) {
                val result = job.getCompleted()
                if (result != null) {
                    results.add(result)
                }
                succeed.incrementAndGet()
            } else {
                failed.incrementAndGet()
            }
        }
    }
    while (succeed.get() + failed.get() < backends.size) {
        if (waitStrategy == WaitStrategy.MAJORITY && results.size >= quorum) {
            jobs.forEach(Job::cancel)
            break
        }
        yield()
    }
    val t2 = System.currentTimeMillis()
    val validity = timeout.toMillis() - (t2 - t1) - clockDrift
    if (results.size < quorum || validity < 0) {
        val cleanUpJobs = mutableListOf<Job>()
        backends.forEach { backend ->
            cleanUpJobs.add(scope.launch { cleanUp(backend) })
        }
        cleanUpJobs.joinAll()
        return emptyList()
    }
    return results
}

suspend inline fun <T : Backend, R> multiInstanceExecuteWithRetry(
    backends: List<T>,
    scope: CoroutineScope,
    timeout: Duration,
    defaultDrift: Duration = Duration.ofMillis(3),
    retryCount: Int = 3,
    retryDelay: Duration = Duration.ofMillis(100),
    waitStrategy: WaitStrategy = WaitStrategy.ALL,
    crossinline cleanUp: (backend: T) -> Unit = { _ -> },
    crossinline callee: suspend (backend: T) -> R,
): List<R> {
    return withRetry(retryCount = retryCount, retryDelay = retryDelay) {
        return@withRetry multiInstanceExecute(
            backends = backends,
            scope = scope,
            timeout = timeout,
            defaultDrift = defaultDrift,
            waitStrategy = waitStrategy,
            callee = callee,
            cleanUp = cleanUp,
        )
    }
}

suspend inline fun <T : Backend, R> List<T>.executeWithRetry(
    scope: CoroutineScope,
    timeout: Duration,
    defaultDrift: Duration = Duration.ofMillis(3),
    retryCount: Int = 3,
    retryDelay: Duration = Duration.ofMillis(100),
    waitStrategy: WaitStrategy = WaitStrategy.ALL,
    crossinline cleanUp: (backend: T) -> Unit = { _ -> },
    crossinline callee: suspend (backend: T) -> R,
): List<R> {
    return multiInstanceExecuteWithRetry(
        backends = this,
        scope = scope,
        timeout = timeout,
        defaultDrift = defaultDrift,
        retryCount = retryCount,
        retryDelay = retryDelay,
        waitStrategy = waitStrategy,
        callee = callee,
        cleanUp = cleanUp,
    )
}
