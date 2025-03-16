package com.himadieiev.redpulsar.lettuce

import com.himadieiev.redpulsar.core.common.LuaScriptEntry
import io.lettuce.core.RedisNoScriptException
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.cluster.api.sync.RedisClusterCommands

fun <T> RedisClusterCommands<String, String>.evalCashed(
    luaScriptEntry: LuaScriptEntry,
    outputType: ScriptOutputType,
    keys: Array<String>,
    args: Array<String>,
): T? {
    return try {
        this.evalsha<T>(luaScriptEntry.sha1, outputType, keys, *args)
    } catch (e: RedisNoScriptException) {
        this.eval<T>(luaScriptEntry.script, outputType, keys, *args)
    }
}
