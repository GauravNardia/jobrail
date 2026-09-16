local now = ARGV[1]

local job_ids = redis.call(
    "ZRANGEBYSCORE",
    KEYS[1],
    "-inf",
    now
)

for _, job_id in ipairs(job_ids) do
    redis.call("ZREM", KEYS[1], job_id)
    redis.call("RPUSH", KEYS[2], job_id)
end

return job_ids