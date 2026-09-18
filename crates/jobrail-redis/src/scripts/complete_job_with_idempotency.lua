local lease_member = ARGV[1]
local idempotency_key = ARGV[2]

-- KEYS[1] = processing leases
-- KEYS[2] = job key
-- KEYS[3] = active queue
-- KEYS[4] = idempotency key

-- 1. Verify that this worker still owns the job.

local exists = redis.call(
    "ZSCORE",
    KEYS[1],
    lease_member
)

if not exists then
    return 0
end

-- 2. Load the job.

local job_data = redis.call(
    "GET",
    KEYS[2]
)

if not job_data then
    return 0
end

local job = cjson.decode(job_data)

-- 3. Verify the idempotency record belongs to this job.

local idempotency_data = redis.call(
    "GET",
    KEYS[4]
)

if not idempotency_data then
    return 0
end

local idempotency = cjson.decode(idempotency_data)

if idempotency.job_id ~= job.id then
    return 0
end

-- 4. Mark idempotency as completed.

idempotency.status = "Completed"

redis.call(
    "SET",
    KEYS[4],
    cjson.encode(idempotency)
)

-- 5. Mark the job as completed.

job.state = "Completed"

redis.call(
    "SET",
    KEYS[2],
    cjson.encode(job)
)

-- 6. Remove the lease.

redis.call(
    "ZREM",
    KEYS[1],
    lease_member
)

-- 7. Remove the job from active.

redis.call(
    "LREM",
    KEYS[3],
    0,
    job.id
)

return 1