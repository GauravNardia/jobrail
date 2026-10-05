local job_id = ARGV[1]

-- Only enqueue if the job is not already waiting.
if redis.call(
    "LPOS",
    KEYS[1],
    job_id
) then
    return 0
end

redis.call(
    "LPUSH",
    KEYS[1],
    job_id
)

return 1