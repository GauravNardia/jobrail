local job_key = KEYS[1]
local scheduled_queue = KEYS[2]
local jobs_index = KEYS[3]

local job_id = ARGV[1]
local job_data = ARGV[2]
local run_at = ARGV[3]

redis.call(
    "SET",
    job_key,
    job_data
)

redis.call(
    "ZADD",
    scheduled_queue,
    run_at,
    job_id
)

redis.call(
    "ZADD",
    jobs_index,
    run_at,
    job_id
)

return 1