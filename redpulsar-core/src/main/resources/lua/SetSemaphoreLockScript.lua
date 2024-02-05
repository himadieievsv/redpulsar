local maxLeases = tonumber(ARGV[2])
local leasersCount = tonumber(redis.call("scard", KEYS[1]))
if leasersCount < maxLeases then
    redis.call("sadd", KEYS[1], ARGV[1])
    redis.call("set", KEYS[2], "", "PX", tonumber(ARGV[3]))
    return "OK"
end
return nil
