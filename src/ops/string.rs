//! Implementation of string operations for the client. 
use crate::*; 

#[pymethods]
impl RedisClient {
    /// If key already exists and is a string, this command appends the value at the end of the string. 
    /// If key does not exist it is created and set as an empty string, 
    /// so APPEND will be similar to SET in this special case.
    /// 
    /// Arguments
    /// =========
    /// `key` - The key.
    /// 
    /// `value` - The value to append to the key. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.set("key", "hello_") # Set the key. 
    /// client.append("key", "world!") == 12 # Append "world!" to the value.
    /// client.get("key") == "hello_world!" # Checking out the new value.
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// The length of the string after the command.
    /// 
    /// Time Complexity
    /// ===============
    /// `O(1)` : Assuming the appended value is small, and the already present value is of any size.
    /// 
    /// [Read about APPEND in the Redis documentation.](https://redis.io/commands/append)
    #[text_signature = "($self, key, value, /)"]
    pub fn append(&mut self, key: &str, value: &PyAny) -> PyResult<usize> {
        let val = value.to_string();
        Ok(route_command(self, "APPEND", Some(&[key, &val]))?)
    }
    
    /// Count the number of set bits (population counting) in a string. 
    /// By default all the bytes contained in the string are examined. 
    /// It is possible to specify the counting operation only in an interval 
    /// passing the additional arguments start and end.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// 
    /// `beginning` - An index of where to start on the string.
    /// 
    /// `end` - An index of where to end on the string. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.bitcount("key", 0, 0)
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// The number of bits set to 1. 
    /// 
    /// Time Complexity
    /// =============== 
    /// `O(n)` : Where n = the number of set bits in the string.
    /// 
    /// [Read about BITCOUNT in the Redis documentation.](https://redis.io/commands/bitcount)
    #[text_signature = "($self, key, beginning, end, /)"]
    pub fn bitcount(&mut self, key: &str, beginning: isize, end: isize) -> PyResult<usize> {
        let start = beginning.to_string();
        let stop = end.to_string();
        Ok(route_command(self, "BITCOUNT", Some(&[key, &start, &stop]))?)
    }

    /// Get the value of key. If the key does not exist the special value nil is returned. 
    /// An empty string is returned if the value stored at key is not a string, because GET only handles string values.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// value = client.get("key")
    /// ```
    /// 
    /// Bulk String Reply
    /// =================
    /// The value of the key, typically. 
    /// 
    /// An empty string if the key is an improper type or doesn't exist. 
    /// 
    /// Time Complexity
    /// ===============
    /// `O(1)`
    /// 
    /// [Read about GET in the Redis documentation.](https://redis.io/commands/get)
    #[text_signature = "($self, key, /)"]
    pub fn get(&mut self, key: &str) -> PyResult<String> {
        Ok(route_command(self, "GET", Some(key)).unwrap_or_default())
    }

    /// Set key to hold the string value. 
    /// If key already holds a value, it is overwritten, regardless of its type. 
    /// Any previous time to live associated with the key is discarded on successful SET operation.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// 
    /// `value` - The string value of the key.
    /// 
    /// `no_overwrite` - Set to False if the key shall be replaced in the case of a duplicate. Otherwise, True.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.set("my_key", "hello world!", no_overwrite=True)
    /// ```
    /// 
    /// Simple String Reply
    /// ===================
    /// `"OK"`: SET was executed correctly. 
    /// 
    /// [Read about SET in the Redis documentation.](https://redis.io/commands/set)
    #[args(no_overwrite="**")]
    #[text_signature = "($self, name, value, *, no_overwrite=True)"]
    pub fn sset(&mut self, key: &str, value: &PyAny, no_overwrite: Option<&PyDict>) -> PyResult<String> {
        let val = value.to_string();
        let command = nx_x_decider("SET", "NX", no_overwrite);
        Ok(route_command(self, &command, Some(&[key, &val]))?)
    }

    /// Atomically sets key to value and returns the old value stored at key. 
    /// Returns -1 when key exists but does not hold a string value.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// 
    /// `value` - The new value of the key. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.set("hello", "world")
    /// client.get("hello") == "world"
    ///
    /// old = client.getset("hello", "redis!")
    /// old == "world"
    /// client.get("hello") == "redis!"
    /// ```
    /// 
    /// Bulk String Reply
    /// =================
    /// The old value of the key. 
    /// 
    /// Time Complexity 
    /// ===============
    /// `O(1)`
    /// 
    /// [Read about GETSET in the Redis documentation.](https://redis.io/commands/getset)
    #[text_signature = "($self, key, value, /)"]
    pub fn getset(&mut self, key: &str, value: &PyAny) -> PyResult<String> {
        let val = value.to_string();
        Ok(route_command(self, "GETSET", Some(&[key, &val]))?)
    }

    /// Decrements the number stored at key by one. 
    /// If the key does not exist, it is set to 0 before performing the operation. 
    /// An error is returned if the key contains a value of the wrong type or contains a string 
    /// that can not be represented as integer. This operation is limited to 64 bit signed integers.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key to decrement.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.decr("key")
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// The value of the key after the decrement.
    /// 
    /// Time Complexity
    /// ===============
    /// `O(1)`
    /// 
    /// [Read about DECR in the Redis documentation.](https://redis.io/commands/decr)
    #[text_signature = "($self, key, /)"]
    pub fn decr(&mut self, key: &str) -> PyResult<i64> {
        Ok(route_command(self, "DECR", Some(key))?)
    }

    /// Decrements the number stored at key by the amount.
    /// If the key does not exist, it is set to 0 before performing the operation. 
    /// An error is returned if the key contains a value of the wrong type or contains a string 
    /// that can not be represented as integer. This operation is limited to 64 bit signed integers.
    /// 
    /// Arguments
    /// ========= 
    /// `key` - The name of the key to decrement.
    /// 
    /// `amount` - The amount to decrement by.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.decrby("key", 3)   // if key == 10, key now == 7
    /// ```
    /// 
    /// Integer Reply 
    /// =============
    /// The value of the key after the decrement, or -1 if an error occurs.
    /// 
    /// Time Complexity
    /// =============== 
    /// `O(1)`
    /// 
    /// [Read about DECRBY in the Redis documentation.](https://redis.io/commands/decrby)
    #[text_signature = "($self, key, amount, /)"]
    pub fn decrby(&mut self, key: &str, amount: usize) -> PyResult<isize> {
        let amt = amount.to_string();
        Ok(route_command(self, "DECRBY", Some(&[key, &amt]))?)
    }

    /// Increments the number stored at key by one. If the key does not exist, 
    /// it is set to 0 before performing the operation. 
    /// An error is returned if the key contains a value of the wrong type or contains a string 
    /// that can not be represented as integer. This operation is limited to 64 bit signed integers.
    /// 
    /// There is no overhead for storing the string representation of the integer.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.incr("key")
    /// ```
    /// 
    /// Integer Reply 
    /// =============
    /// The value of the key after the increment. 
    /// 
    /// Time Complexity 
    /// ===============
    /// `O(1)`
    /// 
    /// [Read about INCR in the Redis documentation.](https://redis.io/commands/incr)
    #[text_signature = "($self, key, /)"]
    pub fn incr(&mut self, key: &str) -> PyResult<isize> {
        Ok(route_command(self, "INCR", Some(key))?)
    }

    /// Increments the number stored at key by increment. 
    /// If the key does not exist, it is set to 0 before performing the operation. 
    /// An error is returned if the key contains a value of the wrong type or contains a string 
    /// that can not be represented as integer. This operation is limited to 64 bit signed integers.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// 
    /// `amount` - The amount to increment by. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.incrby("key", 5) // if key == 2, key now == 7
    /// ```
    /// 
    /// Integer Reply 
    /// =============
    /// The value of the key after the increment. 
    /// 
    /// Time Complexity
    /// ===============
    /// `O(1)`
    /// 
    /// [Read about INCRBY in the Redis documentation.](https://redis.io/commands/incrby)
    #[text_signature = "($self, key, amount, /)"]
    pub fn incrby(&mut self, key: &str, amount: usize) -> PyResult<isize> {
        let amt = amount.to_string();
        Ok(route_command(self, "INCRBY", Some(&[key, &amt]))?)
    } 

    /// Increment the string representing a floating point number stored at key by the specified increment. 
    /// By using a negative increment value, the result is that the value stored at the key is decremented 
    /// (by the obvious properties of addition).
    /// If the key does not exist, it is set to 0 before performing the operation.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// 
    /// `amount` - The amount to increment by. 
    /// 
    /// Example 
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.incrbyfloat("key", 5.3)
    /// ```
    /// 
    /// Bulk String Reply
    /// ================= 
    /// The value of the key after the increment.
    /// 
    /// Time Complexity
    /// =============== 
    /// O(1)
    /// 
    /// [Read about INCRBYFLOAT in the Redis documentation.](https://redis.io/commands/incrbyfloat)
    #[text_signature = "($self, key, amount, /)"]
    pub fn incrbyfloat(&mut self, key: &str, amount: f64) -> PyResult<String> {
        let amt = amount.to_string();
        Ok(route_command(self, "INCRBYFLOAT", Some(&[key, &amt]))?)
    }

    /// Returns the substring of the string value stored at key, 
    /// determined by the offsets start and end (both are inclusive). 
    /// Negative offsets can be used in order to provide an offset starting from the end of the string. 
    /// So -1 means the last character, -2 the penultimate and so forth.
    /// 
    /// The function handles out of range requests by limiting the resulting range to the actual length of the string.
    /// 
    /// Arguments 
    /// =========
    /// `key` - The name of the key.
    ///  
    /// `beginning` - The starting index. 
    /// 
    /// `end` - The ending index. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("")
    /// client.set("key", "hello world!")
    /// client.getrange("key", 0, 4) == "hello"
    /// ```
    /// 
    /// Bulk String Reply
    /// ================= 
    /// The indexed substring. 
    /// 
    /// Time Complexity
    /// =============== 
    /// O(n) : Where n = the length of the returned string. 
    /// 
    /// O(1) : For small strings. 
    /// 
    /// [Read about GETRANGE in the Redis documentation.](https://redis.io/commands/getrange)
    #[text_signature = "($self, key, beginning, end, /)"]
    pub fn getrange(&mut self, key: &str, beginning: usize, end: usize) -> PyResult<String> {
        let start = beginning.to_string();
        let stop = end.to_string();
        Ok(route_command(self, "GETRANGE", Some(&[key, &start, &stop]))?)
    }

    /// Returns the values of all specified keys.
    /// For every key that does not hold a string value or does not exist, the special value nil is returned. 
    /// Because of this, the operation never fails.
    /// 
    /// Arguments
    /// =========
    /// `keys` - A list of keys, by name, to get.
    /// 
    /// Example 
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.mget("key1", "key2", "key3")
    /// ```
    /// 
    /// Array Reply 
    /// ===========
    /// The values of all the keys. 
    /// 
    /// Time Complexity
    /// =============== 
    /// O(n) : Where n = the number of keys to retrieve.
    /// 
    /// [Read about MGET in the Redis documentation.](https://redis.io/commands/mget)
    #[args(keys="*")]
    #[text_signature = "($self, keys, /)"]
    pub fn mget(&mut self, keys: Vec<&PyAny>) -> PyResult<Vec<String>> {
        let skeys = construct_vector(keys.len(), Cow::from(&keys))?;
        Ok(route_command(self, "MGET", Some(skeys))?)
    }

    /// Sets the given keys to their respective values. 
    /// MSET replaces existing values with new values, just as regular SET. 
    /// See MSETNX if you don't want to overwrite existing values.
    /// MSET is atomic, so all given keys are set at once. 
    /// It is not possible for clients to see that some of the keys were updated while others are unchanged.
    /// 
    /// Arguments
    /// =========
    /// `keys` - A Dictionary with a `'key': 'value'` mapping.
    /// `no_overwrite` - Set to False in the case you want duplicates to be overwitten. Otherwise, True. 
    /// 
    /// Example 
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.mset({"key": "hi", "key2": "bye"}, no_overwrite=False)
    /// ```
    /// 
    /// Simple String Reply
    /// ===================
    /// `"OK"`: Since mset cannot fail.
    /// 
    /// Time Complexity 
    /// ===============
    /// `O(n)` : Where n = the number of keys to set.
    /// 
    /// [Read about MSET in the Redis documentation.](https://redis.io/commands/mset)
    #[args(no_overwrite="**")]
    #[text_signature = "($self, keys, /)"]
    pub fn mset(&mut self, keys: HashMap<&str, &PyAny>, no_overwrite: Option<&PyDict>) -> PyResult<String> {
        let command = nx_x_decider("MSET", "NX", no_overwrite);

        let mut arguments = Vec::with_capacity(keys.len() * 2);

        for (key, value) in keys.iter() {
            arguments.push(key.to_string());
            arguments.push(value.to_string());
        }

        Ok(route_command(self, &command, Some(arguments))?)
    }

    /// Set key to hold the string value and set key to timeout after a given number of seconds. 
    /// This command is equivalent to executing the following commands:
    ///    * `SET` key value
    ///    * `EXPIRE` key seconds
    /// 
    /// It is provided as a faster alternative to the given sequence of operations, 
    /// because this operation is very common when Redis is used as a cache.
    /// An error is returned when seconds is invalid.
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// `value` - The value of the key. 
    /// `seconds` - The TTL of the key, in seconds. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// # A key named "hello", with a lifespan of 5 seconds, with a value of "world!"
    /// client.setex("hello", "world!", 5)
    /// ```
    /// 
    /// Simple String Reply
    /// ===================
    /// `"OK"`: on success. 
    /// 
    /// Time Complexity
    /// ===============
    /// `O(1)`
    /// 
    /// [Read about SETEX in the Redis documentation.](https://redis.io/commands/setex)
    #[text_signature = "($self, key, value, lifespan, /)"]
    pub fn setex(&mut self, key: &str, value: &PyAny, lifespan: usize) -> PyResult<String> {
        let val = value.to_string();
        let life = lifespan.to_string();
        Ok(route_command(self, "SETEX", Some(&[key, &life, &val]))?)
    }

    // From here on documentation needs to look like above. 
    // See # Contribution in the README.

    /// PSETEX works exactly like SETEX with the sole difference that 
    /// the expire time is specified in milliseconds instead of seconds.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `value` - The value of the key. 
    /// * `milliseconds` - The TTL in milliseconds. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.psetex("hello", "5000", "world!")
    /// ```python
    /// 
    /// # Simple String Reply: 
    /// * "OK": on success.
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about PSETEX in the Redis documentation.](https://redis.io/commands/psetex)
    #[text_signature = "($self, key, value, milliseconds, /)"]
    pub fn psetex(&mut self, key: &str, value: &PyAny, milliseconds: usize) -> PyResult<String> {
        let val = value.to_string();
        let ms = milliseconds.to_string();
        Ok(route_command(self, "PSETEX", Some(&[key, &ms, &val]))?)
    }

    /// Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value. 
    /// If the offset is larger than the current length of the string at key, the string is padded with zero-bytes 
    /// to make offset fit. Non-existing keys are considered as empty strings, so this command will make sure 
    /// it holds a string large enough to be able to set value at offset.
    /// Note that the maximum offset that you can set is 229 -1 (536870911).
    /// As Redis Strings are limited to 512 megabytes. If you need to grow beyond this size, you can use multiple keys.
    /// 
    /// # Warning: 
    ///     When setting the last possible byte and the string value stored at key does not yet hold a string value, 
    ///     or holds a small string value, Redis needs to allocate all intermediate memory which can block the server 
    ///     for some time. On a 2010 MacBook Pro, setting byte number 536870911 (512MB allocation) takes ~300ms, 
    ///     setting byte number 134217728 (128MB allocation) takes ~80ms, setting bit number 33554432 (32MB allocation) 
    ///     takes ~30ms and setting bit number 8388608 (8MB allocation) takes ~8ms. Note that once this first allocation 
    ///     is done, subsequent calls to SETRANGE for the same key will not have the allocation overhead.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `offset` - The starting offset. 
    /// * `value` - The value to append after the offset. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.setrange("key", "redis!", "5") // if key's value == "world!", it now == "world!redis!"
    /// ```
    /// 
    /// # Integer Reply: 
    /// * The length of the string after the operation. 
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about SETRANGE in the Redis documentation.](https://redis.io/commands/setrange)
    #[text_signature = "($self, key, value, offset/)"]
    pub fn setrange(&mut self, key: &str, value: &PyAny, offset: usize) -> PyResult<usize> {
        let val = value.to_string();
        let off = offset.to_string();
        Ok(route_command(self, "SETRANGE", Some(&[key, &val, &off]))?)
    }

    /// Returns the length of the string value stored at key. An error is returned when key holds a non-string value.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// 
    /// # Example:
    /// ```python
    /// client = RedisClient("url")
    /// length = client.strlen("key") # if key's value == "hey", this returns 3
    /// ```
    /// 
    /// # Integer Reply: 
    /// * The length of the string.
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about STRLEN in the Redis documentation.](https://redis.io/commands/strlen)
    #[text_signature = "($self, key, /)"]
    pub fn strlen(&mut self, key: &str) -> PyResult<usize> {
        Ok(route_command(self, "STRLEN", Some(key))?)
    }
}