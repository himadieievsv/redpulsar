package com.himadieiev.redpulsar.core.common

import com.himadieiev.redpulsar.core.utils.sha1
import java.util.concurrent.ConcurrentHashMap

class ScriptCache {
    private val cache = ConcurrentHashMap<String, LuaScriptEntry>()

    fun addScript(
        scriptName: String,
        script: String,
    ): LuaScriptEntry {
        val entry = LuaScriptEntry(script.sha1(), script)
        cache[scriptName] = entry
        return entry
    }

    fun getScript(scriptName: String): LuaScriptEntry? {
        return cache[scriptName]
    }
}

data class LuaScriptEntry(val sha1: String, val script: String)
