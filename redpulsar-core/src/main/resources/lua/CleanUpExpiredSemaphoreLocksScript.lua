local leasersKey = KEYS[1]
local leasers = redis.call("smembers", leasersKey)
for _, leaser in ipairs(leasers) do
    local leaserValidityKey = ARGV[1] .. ":" .. leaser
    if redis.call("exists", leaserValidityKey) == 0 then
        redis.call("srem", leasersKey, leaser)
    end
end
