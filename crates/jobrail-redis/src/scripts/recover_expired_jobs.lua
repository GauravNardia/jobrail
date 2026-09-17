local now = ARGV[1]

local lease_members = redis.call(
    "ZRANGEBYSCORE",
    KEYS[1],
    "-inf",
    now
)

for _, lease_member in ipairs(lease_members) do
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

            job.state = "Waiting"

            redis.call(
                "SET",
                job_key,
                cjson.encode(job)
            )
        end

        redis.call(
            "ZREM",
            KEYS[1],
            lease_member
        )

        redis.call(
            "LPUSH",
            KEYS[2],
            job_id
        )
    end
end

return lease_members