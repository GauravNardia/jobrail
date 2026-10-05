local deleted = redis.call(
    "DEL",
    KEYS[1]
)

redis.call(
    "SREM",
    KEYS[2],
    ARGV[1]
)

redis.call(
    "ZREM",
    KEYS[3],
    ARGV[1]
)

return deleted