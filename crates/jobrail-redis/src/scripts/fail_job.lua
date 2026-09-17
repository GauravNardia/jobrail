local lease_member = ARGV[1]
local new_state = ARGV[2]
local retry_at = ARGV[3]

local exists = redis.call(
    "ZSCORE",
    KEYS[1],
    lease_member
)

if not exists then
    return 0
end

local job_key = KEYS[2]

local job_data = redis.call(
    "GET",
    job_key
)

if not job_data then
    return 0
end

local job = cjson.decode(job_data)

job.state = new_state

redis.call(
    "SET",
    job_key,
    cjson.encode(job)
)

redis.call(
    "ZREM",
    KEYS[1],
    lease_member
)

redis.call(
    "LREM",
    KEYS[3],
    0,
    job.id
)

if new_state == "Delayed" then
    redis.call(
        "ZADD",
        KEYS[4],
        retry_at,
        job.id
    )
end

return 1