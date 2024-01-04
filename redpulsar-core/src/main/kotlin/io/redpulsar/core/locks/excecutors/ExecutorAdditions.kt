package io.redpulsar.core.locks.excecutors

import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.selects.select

suspend inline fun <T> waitAllJobs(
    jobs: List<Job>,
    @Suppress("UNUSED_PARAMETER") results: MutableList<T>,
) {
    jobs.joinAll()
}

suspend inline fun <T> waitAnyJobs(
    jobs: List<Job>,
    results: MutableList<T>,
) {
    select { jobs.forEach { job -> job.onJoin { } } }
    jobs.forEach { job -> job.cancel() }
    // enough one success result to consider latch opened
    if (results.isNotEmpty()) {
        repeat(jobs.size - results.size) { results.add(results.first()) }
    }
}
