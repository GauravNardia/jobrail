local job_data = redis.call("GET", KEYS[1])

if not job_data then
    return 0
end

local job = cjson.decode(job_data)

-- Terminal jobs cannot be cancelled.
if job.state == "Completed"
    or job.state == "Failed"
    or job.state == "Cancelled"
then
    return 2
end

local job_id = ARGV[1]

-- Remove the job from every non-terminal queue.
redis.call("LREM", KEYS[2], 0, job_id)
redis.call("LREM", KEYS[3], 0, job_id)
redis.call("ZREM", KEYS[4], job_id)
redis.call("ZREM", KEYS[5], job_id)

-- Processing members are stored as:
-- <job_id>:<lease_token>
--
-- We do not know the token here, so find and remove
-- the processing lease belonging to this job.
local processing_members = redis.call(
    "ZRANGE",
    KEYS[6],
    0,
    -1
)

local prefix = job_id .. ":"

for _, member in ipairs(processing_members) do
    if string.sub(member, 1, string.len(prefix)) == prefix then
        redis.call("ZREM", KEYS[6], member)
    end
end

-- Finally make the state transition.
job.state = "Cancelled"

redis.call(
    "SET",
    KEYS[1],
    cjson.encode(job)
)

return 1