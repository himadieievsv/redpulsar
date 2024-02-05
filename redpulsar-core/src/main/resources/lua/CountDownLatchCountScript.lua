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
