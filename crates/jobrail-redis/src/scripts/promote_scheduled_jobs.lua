local now = ARGV[1]

local job_ids = redis.call(
    "ZRANGEBYSCORE",
    KEYS[1],
    "-inf",
    now
)

for _, job_id in ipairs(job_ids) do

    local job_key = "jobrail:job:" .. job_id

    local job_data = redis.call(
        "GET",
        job_key
    )

    if job_data then
        local job = cjson.decode(job_data)

        if job.state == "Scheduled" then
            job.state = "Waiting"

            redis.call(
                "SET",
                job_key,
                cjson.encode(job)
            )

            redis.call(
                "ZREM",
                KEYS[1],
                job_id
            )

            redis.call(
                "LPUSH",
                KEYS[2],
                job_id
            )
        else
            -- The job is no longer scheduled.
            -- Remove stale scheduled entry.
            redis.call(
                "ZREM",
                KEYS[1],
                job_id
            )
        end
    else
        -- Job disappeared; remove stale schedule entry.
        redis.call(
            "ZREM",
            KEYS[1],
            job_id
        )
    end
end

return job_ids