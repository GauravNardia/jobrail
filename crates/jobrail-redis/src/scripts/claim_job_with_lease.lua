local job_id = redis.call(
    "RPOP",
    KEYS[1]
)

if not job_id then
    return nil
end

redis.call(
    "LPUSH",
    KEYS[2],
    job_id
)

local lease_member = job_id .. ":" .. ARGV[1]

redis.call(
    "ZADD",
    KEYS[3],
    ARGV[2],
    lease_member
)

return job_id