package com.himadieiev.redpulsar.core.common

const val removeLockScript = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return nil
"""

const val setSemaphoreLockScript = """
local maxLeases = tonumber(ARGV[2])
local leasersCount = tonumber(redis.call("scard", KEYS[1]))
if leasersCount < maxLeases then
    redis.call("sadd", KEYS[1], ARGV[1])
    redis.call("set", KEYS[2], "", "PX", tonumber(ARGV[3]))
    return "OK"
end
return nil
"""

const val cleanUpExpiredSemaphoreLocksScript = """
local leasersKey = KEYS[1]
local leasers = redis.call("smembers", leasersKey)
for _, leaser in ipairs(leasers) do
    local leaserValidityKey = ARGV[1] .. ":" .. leaser
    if redis.call("exists", leaserValidityKey) == 0 then
        redis.call("srem", leasersKey, leaser)
    end
end
"""

const val countDownLatchCountScript = """
redis.call("sadd", KEYS[1], ARGV[1])
if redis.call("pttl", KEYS[1]) == -1 then
    redis.call("pexpire", KEYS[1], ARGV[2])
else
    redis.call("pexpire", KEYS[1], ARGV[2], "GT")
end
local elementsCount = redis.call("scard", KEYS[1])
if elementsCount >= tonumber(ARGV[3]) then
    redis.call("publish", KEYS[2], "open")
end
return "OK"
"""
