local lease_member = ARGV[1]
local target_state = ARGV[2]
local retry_at = ARGV[3]

-- ---------------------------------------------------------
-- 1. The caller must still own the lease.
-- ---------------------------------------------------------

local lease_score = redis.call(
    "ZSCORE",
    KEYS[1],
    lease_member
)

if not lease_score then
    return 0
end

-- ---------------------------------------------------------
-- 2. The job must still exist.
-- ---------------------------------------------------------

local job_data = redis.call(
    "GET",
    KEYS[2]
)

if not job_data then
    redis.call(
        "ZREM",
        KEYS[1],
        lease_member
    )

    return 0
end

local job = cjson.decode(job_data)

-- ---------------------------------------------------------
-- 3. Only an Active job can fail/retry.
--
-- This prevents:
--
-- Worker A → old token
-- Worker B → already completed/reclaimed job
-- Worker A → accidentally changes Worker B's state
-- ---------------------------------------------------------

if job.state ~= "Active" then
    return 0
end

local job_id = job.id

-- ---------------------------------------------------------
-- 4. Remove Active ownership.
-- ---------------------------------------------------------

redis.call(
    "LREM",
    KEYS[3],
    0,
    job_id
)

redis.call(
    "ZREM",
    KEYS[1],
    lease_member
)

-- ---------------------------------------------------------
-- 5. Apply the requested failure transition.
-- ---------------------------------------------------------

if target_state == "Waiting" then

    job.state = "Waiting"

    -- Never leave an old delayed retry behind.
    redis.call(
        "ZREM",
        KEYS[4],
        job_id
    )

    -- Avoid duplicate waiting entries.
    if not redis.call(
        "LPOS",
        KEYS[5],
        job_id
    ) then
        redis.call(
            "LPUSH",
            KEYS[5],
            job_id
        )
    end

elseif target_state == "Delayed" then

    job.state = "Delayed"

    redis.call(
        "ZADD",
        KEYS[4],
        retry_at,
        job_id
    )

elseif target_state == "Failed" then

    job.state = "Failed"

    -- A terminal failure must not remain scheduled
    -- for retry.
    redis.call(
        "ZREM",
        KEYS[4],
        job_id
    )

else
    return 0
end

-- ---------------------------------------------------------
-- 6. Persist the state transition.
-- ---------------------------------------------------------

redis.call(
    "SET",
    KEYS[2],
    cjson.encode(job)
)

return 1