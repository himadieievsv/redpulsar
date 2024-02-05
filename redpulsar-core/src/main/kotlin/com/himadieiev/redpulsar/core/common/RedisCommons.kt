package com.himadieiev.redpulsar.core.common

import com.himadieiev.redpulsar.core.locks.abstracts.Backend

const val removeLockScript = """
"""

const val setSemaphoreLockScript = """

"""

const val cleanUpExpiredSemaphoreLocksScript = """

"""

const val countDownLatchCountScript = """

"""

fun Backend.getScript(script: String): String {
    return this::class.java.getResource("/lua/$script.lua").readText()
}
