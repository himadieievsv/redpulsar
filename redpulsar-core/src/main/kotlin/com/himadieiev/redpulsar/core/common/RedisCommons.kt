package com.himadieiev.redpulsar.core.common

import com.himadieiev.redpulsar.core.locks.abstracts.Backend

const val REMOVE_LOCK_SCRIPT_PATH = "lua/RemoveLockScript.lua"
const val SET_SEMAPHORE_LOCK_SCRIPT_PATH = "lua/SetSemaphoreLockScript.lua"
const val CLEAN_UP_EXPIRED_SEMAPHORE_LOCKS_SCRIPT_PATH = "lua/CleanUpExpiredSemaphoreLocksScript.lua"
const val COUNT_DOWN_LATCH_COUNT_SCRIPT_PATH = "lua/CountDownLatchCountScript.lua"

fun Backend.loadScript(scriptPath: String): String {
    val resourceStream =
        this::class.java.classLoader.getResourceAsStream(scriptPath)
            ?: throw IllegalArgumentException("Script $scriptPath not found")
    return String(resourceStream.readAllBytes()).trimIndent()
}
