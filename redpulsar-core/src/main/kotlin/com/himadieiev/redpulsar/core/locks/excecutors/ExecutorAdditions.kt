package com.himadieiev.redpulsar.core.locks.excecutors

import kotlinx.coroutines.Job
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.yield
import java.util.Collections

suspend inline fun waitAllJobs(jobs: List<Job>) {
    jobs.joinAll()
}

suspend inline fun waitMajorityJobs(jobs: List<Job>) {
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
