package com.himadieiev.redpulsar.core.common

import com.himadieiev.redpulsar.core.locks.abstracts.Backend

const val removeLockScriptPath = "/lua/RemoveLockScript.lua"
const val setSemaphoreLockScriptPath = "/lua/SetSemaphoreLockScript.lua"
const val cleanUpExpiredSemaphoreLocksScriptPath = "/lua/CleanUpExpiredSemaphoreLocksScript.lua"
const val countDownLatchCountScriptPath = "/lua/CountDownLatchCountScript.lua"

fun Backend.loadScript(script: String): String {
    return this::class.java.getResource("/lua/$script.lua").readText().trimIndent()
}
