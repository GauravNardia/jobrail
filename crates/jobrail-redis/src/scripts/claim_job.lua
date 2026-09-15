local job_id = redis.call("LPOP", KEYS[1])

if not job_id then
    return nil
end

local job_key = "jobrail:job:" .. job_id

local job = redis.call("GET", job_key)

if not job then
    return nil
end

redis.call("RPUSH", KEYS[2], job_id)

return job_id