package com.himadieiev.redpulsar.core.locks.excecutors

enum class WaitStrategy {
    ALL,
    MAJORITY,
}

fun requiredToSuccessCount(
    waitStrategy: WaitStrategy,
    backendsSize: Int,
): Int {
    return when (waitStrategy) {
        WaitStrategy.ALL -> backendsSize
        WaitStrategy.MAJORITY -> backendsSize / 2 + 1
    }
}

fun enoughToFailCount(
    waitStrategy: WaitStrategy,
    backendsSize: Int,
): Int {
    return when (waitStrategy) {
        WaitStrategy.ALL -> 1
        WaitStrategy.MAJORITY -> backendsSize - requiredToSuccessCount(waitStrategy, backendsSize) + 1
    }
}
