package com.himadieiev.redpulsar.jedis.locks

import com.himadieiev.redpulsar.core.common.LuaScriptEntry
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.exceptions.JedisNoScriptException

fun UnifiedJedis.evalSha1(
    luaScriptEntry: LuaScriptEntry,
    keys: List<String>,
    args: List<String>,
): Any? {
    return try {
        this.evalsha(luaScriptEntry.sha1, keys, args)
    } catch (e: JedisNoScriptException) {
        this.eval(luaScriptEntry.script, keys, args)
    }
}
