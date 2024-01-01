package io.redpulsar.core.locks

import io.redpulsar.core.locks.abstracts.backends.CountDownLatchBackend
import io.redpulsar.core.locks.api.CallResult
import io.redpulsar.core.locks.api.CountDownLatch
import io.redpulsar.core.locks.excecutors.executeWithRetry
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.selects.select
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

class ListeningCountDownLatch(
    private val name: String,
    private val count: Int,
    private val backends: List<CountDownLatchBackend>,
    private val maxDuration: Duration = 5.minutes,
    private val retryCount: Int = 3,
    private val retryDelay: Duration = 101.milliseconds,
) : CountDownLatch {
    private val scope: CoroutineScope = CoroutineScope(CoroutineName("listeningCountDownLatch") + Dispatchers.IO)
    private val clientId: String = UUID.randomUUID().toString()
    private val keySpace = "countdownlatch"
    private val channelSpace = "channels"
    private val openLatchMessage = "open"
    private val currentCounter = AtomicInteger(count)

    init {
        require(backends.isNotEmpty()) { "Backend instances must not be empty" }
        require(count > 0) { "Count must be positive" }
        require(name.isNotBlank()) { "Name must not be blank" }
        require(maxDuration > 100.milliseconds) { "Max duration must be greater that 0.1 second" }
        require(retryDelay > 0.milliseconds) { "Retry delay must be positive" }
        require(retryCount > 0) { "Retry count must be positive" }
    }

    override fun countDown(): CallResult {
        // Skip if internal counter is already 0
        if (currentCounter.get() <= 0) return CallResult.SUCCESS
        val result =
            backends.executeWithRetry(
                scope = scope,
                releaseTime = maxDuration,
                retryCount = retryCount,
                retryDelay = retryDelay,
            ) { backend ->
                backend.count(
                    latchKeyName = buildKey(name),
                    channelName = buildKey(channelSpace, name),
                    clientId = clientId,
                    count = currentCounter.get(),
                    initialCount = count,
                    ttl = maxDuration * 2,
                )
            }
        return if (result.isEmpty()) {
            backends.executeWithRetry(
                scope = scope,
                releaseTime = maxDuration,
                retryCount = retryCount,
                retryDelay = retryDelay,
            ) { backend ->
                backend.undoCount(
                    latchKeyName = buildKey(name),
                    clientId = clientId,
                    count = currentCounter.get(),
                )
            }
            CallResult.FAILED
        } else {
            currentCounter.decrementAndGet()
            CallResult.SUCCESS
        }
    }

    override fun await(): CallResult {
        return await(maxDuration)
    }

    override fun await(timeout: Duration): CallResult {
        // Open latch if internal counter or global one is already 0 or less
        val globalCount = getCount()
        if (globalCount == Int.MIN_VALUE) return CallResult.FAILED
        if (currentCounter.get() <= 0 || globalCount <= 0) return CallResult.SUCCESS
        val result =
            backends.executeWithRetry(
                scope = scope,
                releaseTime = timeout,
                retryCount = retryCount,
                retryDelay = retryDelay,
                waiter = { jobs, results ->
                    select { jobs.forEach { job -> job.onJoin { } } }
                    jobs.forEach { job -> job.cancel() }
                    // enough one success result to consider latch opened
                    if (results.isNotEmpty()) {
                        repeat(backends.size - 1) { results.add("") }
                    }
                },
            ) { backend ->
                backend.listen(
                    channelName = buildKey(channelSpace, name),
                    messageConsumer = { message ->
                        if (message == openLatchMessage) {
                            "OK"
                        } else {
                            null
                        }
                    },
                )
            }
        return if (result.isEmpty()) {
            CallResult.FAILED
        } else {
            CallResult.SUCCESS
        }
    }

    /**
     * Returns the current count. Typycally it will return range from 'count' to 0 or below.
     * If some instance are not available and quorum wasn't reached, returns [Int.MIN_VALUE].
     */
    override fun getCount(): Int {
        val result =
            backends.executeWithRetry(
                scope = scope,
                releaseTime = maxDuration * 2,
                retryCount = retryCount,
                retryDelay = retryDelay,
            ) { backend ->
                backend.checkCount(latchKeyName = buildKey(name))
            }
        return if (result.isEmpty()) {
            Int.MIN_VALUE
        } else {
            result.map { it }
                .groupBy { it }
                .mapValues { it.value.size }
                .maxBy { it.value }.key ?: Int.MIN_VALUE
        }
    }

    @Suppress("NOTHING_TO_INLINE")
    private inline fun buildKey(vararg parts: String) = keySpace + ":" + parts.joinToString(":")
}
