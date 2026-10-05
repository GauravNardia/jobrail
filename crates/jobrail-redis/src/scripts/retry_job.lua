local job_data = redis.call(
    "GET",
    KEYS[1]
)

if not job_data then
    return 0
end

local job = cjson.decode(job_data)

-- Only failed jobs can be manually retried.
if job.state ~= "Failed" then
    return 2
end

local job_id = ARGV[1]

if tonumber(job.priority) > 0 then
    job.state = "Prioritized"
else
    job.state = "Waiting"
end

job.run_at = cjson.null

redis.call(
    "SET",
    KEYS[1],
    cjson.encode(job)
)

-- Do not create duplicate waiting entries.
if not redis.call(
    "LPOS",
    KEYS[2],
    job_id
) then
    redis.call(
        "LPUSH",
        KEYS[2],
        job_id
    )
end

return 1