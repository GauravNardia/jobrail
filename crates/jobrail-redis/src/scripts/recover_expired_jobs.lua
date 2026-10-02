local now = ARGV[1]

local expired_members = redis.call(
    "ZRANGEBYSCORE",
    KEYS[1],
    "-inf",
    now
)

for _, lease_member in ipairs(expired_members) do
    -- Lease members are:
    -- <job_id>:<lease_token>
    local separator = string.find(lease_member, ":")

    if separator then
        local job_id = string.sub(lease_member, 1, separator - 1)

        local job_key = "jobrail:job:" .. job_id

        local job_data = redis.call(
            "GET",
            job_key
        )

        if job_data then
            local job = cjson.decode(job_data)

            -- Recover the job only if it is still Active.
            --
            -- This protects us from accidentally requeueing a job
            -- that was already completed/cancelled by another owner.
            if job.state == "Active" then
                job.state = "Waiting"

                redis.call(
                    "SET",
                    job_key,
                    cjson.encode(job)
                )

                redis.call(
                    "LPUSH",
                    KEYS[2],
                    job_id
                )
            end

            -- If the job has an idempotency key, clear ONLY a
            -- Processing record belonging to this recovered job.
            --
            -- Completed idempotency records must remain.
local idempotency_key = job.idempotency_key

-- cjson.null is returned by Redis Lua when the JSON field
-- exists but its value is null.
--
-- Only attempt idempotency recovery when the job actually
-- has an idempotency key.
if idempotency_key ~= nil
    and idempotency_key ~= cjson.null
then
    local idempotency_redis_key =
        "jobrail:idempotency:" .. tostring(idempotency_key)

    local idempotency_data = redis.call(
        "GET",
        idempotency_redis_key
    )

    if idempotency_data then
        local idempotency_record =
            cjson.decode(idempotency_data)

        local record_job_id =
            idempotency_record.job_id

        local record_status =
            idempotency_record.status

        if record_job_id == job_id
            and record_status == "Processing"
        then
            redis.call(
                "DEL",
                idempotency_redis_key
            )
        end
    end
end
        end

        -- Always remove the expired lease.
        redis.call(
            "ZREM",
            KEYS[1],
            lease_member
        )
    end
end

return expired_members