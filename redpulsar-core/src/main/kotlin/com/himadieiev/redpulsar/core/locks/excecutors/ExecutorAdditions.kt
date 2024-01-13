package com.himadieiev.redpulsar.core.locks.excecutors

import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.yield
import java.util.Collections

suspend inline fun <T> waitAllJobs(
    jobs: List<Job>,
    @Suppress("UNUSED_PARAMETER") results: MutableList<T>,
) {
    jobs.joinAll()
}

suspend inline fun <T> waitMajorityJobs(
    jobs: List<Job>,
    results: MutableList<T>,
) {
    val quorum: Int = jobs.size / 2 + 1
    val results = Collections.synchronizedList(mutableListOf<String>())
    val failed = Collections.synchronizedList(mutableListOf<String>())
    jobs.forEach { job ->
        job.invokeOnCompletion { cause ->
            if (cause == null) {
                results.add("OK")
            } else {
                failed.add("FAILED")
            }
        }
    }
    while (results.size < quorum && failed.size < quorum) {
        yield()
    }
    return
}
