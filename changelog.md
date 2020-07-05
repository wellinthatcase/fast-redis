# fastredis follows Semantic Versioning 2.0.0 (https://semver.org/spec/v2.0.0.html)

# 0.1.1
    - Documentation improvements on the RedisClient methods.
    - The ``info`` method now returns a tuple of values. See to this specification:
        - The first element will be the number of the database.
        - The second element will be a boolean indicating if pipelining is supported on the connection.
        - The third element will be the URL of the Redis client.
    - Better licensing.