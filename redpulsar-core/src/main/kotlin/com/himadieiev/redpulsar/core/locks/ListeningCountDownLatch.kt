package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.backends.CountDownLatchBackend
import com.himadieiev.redpulsar.core.locks.api.CallResult
import com.himadieiev.redpulsar.core.locks.api.CountDownLatch
import com.himadieiev.redpulsar.core.locks.excecutors.executeWithRetry
import com.himadieiev.redpulsar.core.locks.excecutors.waitAnyJobs
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.cancellation.CancellationException

/**
 * A distributed locking mechanics, intended for synchronization of multiple workloads distributed across cloud.
 * Allows one or more workloads to wait until a set of other workload are get ready or finished.
 * [ListeningCountDownLatch] implementation uses Redis Pub/Sub mechanism to notify other instances
 * about counter reached to 0.
 *
 * @param name [String] the name of the latch.
 * @param count [Int] the number of times [countDown] must be invoked before threads can pass through [await].
 * @param backends [List] of [CountDownLatchBackend] instances.
 * @param maxDuration [Duration] the maximum time to wait.
 * @param retryCount [Int] the number of retries to acquire lock.
 * @param retryDelay [Duration] the delay between retries.
 */
class ListeningCountDownLatch(
    private val name: String,
    private val count: Int,
    private val backends: List<CountDownLatchBackend>,
    private val maxDuration: Duration = Duration.ofMinutes(5),
    private val retryCount: Int = 3,
    private val retryDelay: Duration = Duration.ofMillis(100),
) : CountDownLatch {
    private val scope: CoroutineScope = CoroutineScope(CoroutineName("listeningCountDownLatch") + Dispatchers.IO)
    private val clientId: String = UUID.randomUUID().toString()
    private val keySpace = "countdownlatch"
    private val channelSpace = "channels"
    private val currentCounter = AtomicInteger(count)
    private val minimalMaxDuration = Duration.ofMillis(100)

    init {
        require(backends.isNotEmpty()) { "Backend instances must not be empty" }
        require(count > 0) { "Count must be positive" }
        require(name.isNotBlank()) { "Name must not be blank" }
        require(maxDuration > minimalMaxDuration) { "Max duration must be greater that 0.1 second" }
        require(retryDelay.toMillis() > 0) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    /**
     * Decrements the count of the latch.
     * @return [CallResult] SUCCESS or FAILED. SUCCESS means that the count was decremented on majority
     * of underling instances. And FAILED means that majority of underling instances are down or other network issue.
     */
    override fun countDown(): CallResult {
        // Skip if internal counter is already 0
        if (currentCounter.get() <= 0) return CallResult.SUCCESS
        val result = count()
        return if (result.isEmpty()) {
            undoCount()
            CallResult.FAILED
        } else {
            currentCounter.decrementAndGet()
            CallResult.SUCCESS
        }
    }

    /**
     * Block exception in current thread until the count of the latch is zero.
     * @return [CallResult] SUCCESS or FAILED. SUCCESS means that the count was decremented on majority instances
     */
    override fun await(): CallResult {
        return await(maxDuration)
    }

    /**
     * Block exception in current thread until the count of the latch is zero with timeout.
     * @param timeout [Duration] the maximum time to wait.
     * @return [CallResult] SUCCESS or FAILED. SUCCESS means that the count was decremented on majority instances
     */
    override fun await(timeout: Duration): CallResult {
        require(timeout > minimalMaxDuration) { "Timeout must be greater that [minimalMaxDuration]" }
        val job =
            scope.async {
                withTimeout(timeout.toMillis()) {
                    val globalCount = getCount(this)
                    if (globalCount == Int.MIN_VALUE) return@withTimeout CallResult.FAILED
                    // Open latch if internal counter or global one is already 0 or less
                    if (currentCounter.get() <= 0 || globalCount <= 0) return@withTimeout CallResult.SUCCESS
                    val result = listen(timeout, this)
                    return@withTimeout if (result.isEmpty()) {
                        CallResult.FAILED
                    } else {
                        CallResult.SUCCESS
                    }
                }
            }
        var result: CallResult
        runBlocking {
            result =
                try {
                    job.await()
                } catch (e: CancellationException) {
                    CallResult.FAILED
                }
        }
        return result
    }

    /**
     * Returns the current count. It will return range from 'count' to 0 or below.
     * If some instance are not available and quorum wasn't reached, returns [Int.MIN_VALUE].
     */
    override fun getCount(): Int {
        return getCount(scope)
    }

    private fun getCount(scope: CoroutineScope): Int {
        val result = checkCount(scope)
        return if (result.isEmpty()) {
            Int.MIN_VALUE
        } else {
            val maxValue =
                result.map { it }
                    .groupBy { it }
                    .mapValues { it.value.size }
                    .maxBy { it.value }.key?.toInt()
            if (maxValue != null) {
                count - maxValue
            } else {
                Int.MIN_VALUE
            }
        }
    }

    private fun count(): List<String?> {
        return backends.executeWithRetry(
            scope = scope,
            timeout = maxDuration,
            retryCount = retryCount,
            retryDelay = retryDelay,
        ) { backend ->
            backend.count(
                latchKeyName = buildKey(name),
                channelName = buildKey(channelSpace, name),
                clientId = clientId,
                count = currentCounter.get(),
                initialCount = count,
                ttl = maxDuration.multipliedBy(2),
            )
        }
    }

    private fun undoCount() {
        backends.executeWithRetry(
            scope = scope,
            timeout = maxDuration,
            retryCount = retryCount,
            retryDelay = retryDelay,
        ) { backend ->
            backend.undoCount(
                latchKeyName = buildKey(name),
                clientId = clientId,
                count = currentCounter.get(),
            )
        }
    }

    private fun checkCount(scope: CoroutineScope): List<Long?> {
        return backends.executeWithRetry(
            scope = scope,
            timeout = maxDuration.multipliedBy(2),
            retryCount = retryCount,
            retryDelay = retryDelay,
        ) { backend ->
            backend.checkCount(latchKeyName = buildKey(name))
        }
    }

    private suspend fun listen(
        timeout: Duration,
        scope: CoroutineScope,
    ): List<String?> {
        return backends.executeWithRetry(
            scope = scope,
            timeout = timeout,
            retryCount = retryCount,
            retryDelay = retryDelay,
            // Allow non-quorum polling here. That might need to be changed as it could lead to unexpected behavior
            // if multiple instances goes down or encounter network issue.
            waiter = ::waitAnyJobs,
        ) { backend ->
            backend.listen(channelName = buildKey(channelSpace, name))
        }
    }

    @Suppress("NOTHING_TO_INLINE")
    private inline fun buildKey(vararg parts: String) = keySpace + ":" + parts.joinToString(":")
}
