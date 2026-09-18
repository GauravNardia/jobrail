local exists = redis.call(
    "EXISTS",
    KEYS[1]
)

if exists == 1 then
    return 0
end

local record = cjson.encode({
    job_id = ARGV[1],
    status = "Processing"
})

redis.call(
    "SET",
    KEYS[1],
    record
)

return 1