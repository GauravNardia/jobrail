local occurrence_key = KEYS[1]
local job_key = KEYS[2]
local scheduled_queue = KEYS[3]
local repeatable_key = KEYS[4]
local repeatable_schedule = KEYS[5]
local jobs_index = KEYS[6]

local occurrence_exists = redis.call(
    "EXISTS",
    occurrence_key
)

if occurrence_exists == 1 then
    return 0
end

local repeatable_data = redis.call(
    "GET",
    repeatable_key
)

if not repeatable_data then
    return 0
end

local repeatable_job = cjson.decode(
    repeatable_data
)

if not repeatable_job.enabled then
    return 0
end

redis.call(
    "SET",
    occurrence_key,
    ARGV[1],
    "EX",
    ARGV[6]
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

redis.call(
    "ZADD",
    jobs_index,
    ARGV[3],
    ARGV[1]
)

repeatable_job.next_run_at =
    tonumber(ARGV[4])

redis.call(
    "SET",
    repeatable_key,
    cjson.encode(repeatable_job)
)

redis.call(
    "ZADD",
    repeatable_schedule,
    ARGV[4],
    ARGV[5]
)

return 1