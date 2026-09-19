local occurrence_key = KEYS[1]
local job_key = KEYS[2]
local scheduled_queue = KEYS[3]

local occurrence_exists = redis.call(
    "EXISTS",
    occurrence_key
)

if occurrence_exists == 1 then
    return 0
end

redis.call(
    "SET",
    occurrence_key,
    ARGV[1]
)

redis.call(
    "SET",
    job_key,
    ARGV[2]
)

redis.call(
    "ZADD",
    scheduled_queue,
    ARGV[3],
    ARGV[1]
)

return 1