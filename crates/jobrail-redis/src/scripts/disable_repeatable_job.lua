local data = redis.call(
    "GET",
    KEYS[1]
)

if not data then
    return 0
end

local job = cjson.decode(data)

job.enabled = false

redis.call(
    "SET",
    KEYS[1],
    cjson.encode(job)
)

redis.call(
    "ZREM",
    KEYS[2],
    ARGV[1]
)

return 1