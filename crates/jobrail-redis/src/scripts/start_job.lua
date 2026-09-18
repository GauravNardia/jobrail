local lease_member = ARGV[1]

-- Check that this worker still owns the job.
local exists = redis.call(
    "ZSCORE",
    KEYS[1],
    lease_member
)

if not exists then
    return 0
end

-- Load the job.
local job_key = KEYS[2]

local job_data = redis.call(
    "GET",
    job_key
)

if not job_data then
    return 0
end

local job = cjson.decode(job_data)

-- Increment attempt counters.
job.attempts_started = job.attempts_started + 1
job.attempts_made = job.attempts_made + 1

-- Mark the job as Active.
job.state = "Active"

-- Save the updated job.
redis.call(
    "SET",
    job_key,
    cjson.encode(job)
)

return 1