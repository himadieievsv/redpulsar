package io.redpulsar.core.locks.api

import kotlin.time.Duration

interface CountDownLatch {
    fun countDown(): CallResult

    fun await(): CallResult

    fun await(timeout: Duration): CallResult

    fun getCount(): Int
}

enum class CallResult {
    SUCCESS,
    FAILED,
}
