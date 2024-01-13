package com.himadieiev.redpulsar.core.locks.excecutors

import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.yield
import java.util.concurrent.atomic.AtomicInteger

suspend inline fun waitAllJobs(jobs: List<Job>) {
    jobs.joinAll()
}

suspend inline fun waitMajorityJobs(jobs: List<Job>) {
    val quorum: Int = jobs.size / 2 + 1
    val succeed = AtomicInteger(0)
    val failed = AtomicInteger(0)
    jobs.forEach { job ->
        job.invokeOnCompletion { cause ->
            if (cause == null) {
                succeed.incrementAndGet()
            } else {
                failed.incrementAndGet()
            }
        }
    }
    while (succeed.get() < quorum && failed.get() < quorum) {
        yield()
    }
    return
}
