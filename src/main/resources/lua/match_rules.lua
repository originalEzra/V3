if redis.call('EXISTS', KEYS[1]) == 0 then
    return {'NOT_READY'}
end

local result = {}
local above = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
for i = 1, #above do
    result[#result + 1] = above[i]
end

local below = redis.call('ZRANGEBYSCORE', KEYS[3], ARGV[1], '+inf')
for i = 1, #below do
    result[#result + 1] = below[i]
end

return result
