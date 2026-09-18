local expected_job_id = ARGV[1]

local record_data = redis.call(
    "GET",
    KEYS[1]
)

if not record_data then
    return 0
end

local record = cjson.decode(record_data)

-- Only the job that owns the idempotency key
-- can mark it completed.
if record.job_id ~= expected_job_id then
    return 0
end

if record.status == "Completed" then
    return 1
end

record.status = "Completed"

redis.call(
    "SET",
    KEYS[1],
    cjson.encode(record)
)

return 1