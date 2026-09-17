local lease_member = ARGV[1]
local lease_until = ARGV[2]

local exists = redis.call(
    "ZSCORE",
    KEYS[1],
    lease_member
)

if not exists then
    return 0
end

redis.call(
    "ZADD",
    KEYS[1],
    lease_until,
    lease_member
)

return 1