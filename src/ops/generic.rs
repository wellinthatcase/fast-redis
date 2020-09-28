//! Implementation of generic operations for the client. 
use crate::*;

#[pymethods]
impl RedisClient {
    /// A low-level interface for making more advanced commands to Redis.
    /// There's no real reason to use this unless you need access to a command not yet supported by suredis.
    /// 
    /// Arguments
    /// =========
    /// `cmd` - The command name. 
    /// 
    /// `args` - A list of arguments to pass to the command. Passed as rest arguments.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.manual("SET", "key", "value") # equal to client.set("key", "value")
    /// ```
    /// 
    /// Bulk String Reply
    /// ================= 
    /// The return of the Redis operation. 
    /// 
    #[args(args="*")]
    #[text_signature = "($self, cmd, *args)"]
    pub fn manual(&mut self, cmd: &str, args: Vec<&PyAny>) -> PyResult<String> {
        let args = construct_vector(args.len(), Cow::from(&args))?;
        Ok(route_command(self, cmd, Some(args))?)
    }

    /// Delete the specified keys. Keys will be ignored if they do not exist.
    /// 
    /// Use the UNLINK command to delete asynchronously.
    /// 
    /// Arguments
    /// =========
    /// `keys` - A list of keys to be deleted by name.
    ///
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.set("key", "hello world!")     # Set the key. 
    /// client.delete("key") == 1             # Delete the key. 
    /// client.delete("key1", "key2", "key3") == 0  # None of the keys exist. 
    /// ```
    ///     
    /// Time Complexity 
    /// ===============
    /// `O(n)` : Where n = the number of keys that will be removed. 
    /// 
    /// `O(m)` : Where m = the number of elements in the list, set, sorted set, or hash. **If applicable**. 
    ///
    /// Integer Reply
    /// =============
    /// The amount of keys deleted.
    /// 
    /// [Read about DEL in the Redis documentation.](https://redis.io/commands/del)
    #[args(keys="*")]
    #[text_signature = "($self, keys, /)"]
    pub fn delete(&mut self, keys: Vec<&PyAny>) -> PyResult<usize> {
        let args = construct_vector(keys.len(), Cow::from(&keys))?;
        Ok(route_command(self, "DEL", Some(args))?)
    }

    /// Check if a key exists. 
    /// 
    /// Arguments
    /// =========
    /// `keys` - A list of keys to check exists
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.set("key", "a")
    /// exists = client.exists("key")   # Returns 1 since the 1 key provided does exist.
    /// exists_more = client.exists("key", "key2", "key3")  # Also returns 1. 
    /// ```
    ///
    /// Integer Reply
    /// =============
    /// The amount of keys that exist in Redis from the passed sequence.
    ///
    /// Time Complexity
    /// ===============
    /// `O(n)` : Where n = the amount of keys to check.  
    ///
    /// [Read about EXISTS in the Redis documentation.](https://redis.io/commands/exists)
    #[args(keys="*")]
    #[text_signature = "($self, keys, /)"]
    pub fn exists(&mut self, keys: Vec<&PyAny>) -> PyResult<usize> {
        let args = construct_vector(keys.len(), Cow::from(&keys))?;
        Ok(route_command(self, "EXISTS", Some(args))?)
    }

    /// Set a timeout on a key. After the timeout expires, the key will be deleted.
    /// Keys with this behavior are refeered to as volatile keys in Redis.
    /// 
    /// It is possible to call expire using as argument a key that already has an existing expire set. 
    /// In this case the time to live of a key is updated to the new value.
    /// 
    /// If an invalid `seconds` is passed, then `seconds` is set to `0`.
    ///
    /// Arguments
    /// =========
    /// `key` - The name of the key.
    /// 
    /// `seconds` - The TTL (a.k.a Time To Live) in seconds.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.expire("key", 5)   # Set an expiration of 5 seconds.
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// `1`: The timeout was set.
    /// 
    /// `0`: The timeout was not set. Input was not an integer, key doesn't exist, etc.
    /// 
    /// Time Complexity
    /// ===============
    /// `O(1)`
    ///
    /// [Read about EXPIRE in the Redis documentation.](https://redis.io/commands/expire)
    #[text_signature = "($self, key, seconds, /)"]
    pub fn expire(&mut self, key: &str, seconds: usize) -> PyResult<u8> {
        let time = seconds.to_string();
        Ok(route_command(self, "EXPIRE", Some(&[key, &time]))?)
    }

    /// Set a timeout on a key with a UNIX timestamp. After the timeout expires, the key will be deleted.
    /// Keys with this behavior are refeered to as volatile keys in Redis.
    ///
    /// EXPIREAT has the same effect and semantic as EXPIRE, 
    /// but instead of specifying the number of seconds representing the TTL (time to live), 
    /// it takes an absolute UNIX timestamp (seconds since January 1, 1970). 
    /// A timestamp in the past will delete the key immediately.
    ///
    /// Arguments
    /// =========
    /// `key` - The name of the key key.
    /// 
    /// `timestamp` - The UNIX timestamp.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.expireat("key", 1604224870)
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// `1`: The timeout was set.
    /// 
    /// `0`: The timeout was not set. Invalid UNIX timestamp, key doesn't exist, etc.
    ///
    /// Time Complexity
    /// ===============
    /// `O(1)`
    ///
    /// [Read about EXPIREAT in the Redis documenation.](https://redis.io/commands/expireat)
    #[text_signature = "($self, key, timestamp, /)"]
    pub fn expireat(&mut self, key: &str, timestamp: usize) -> PyResult<u8> {
        let time = timestamp.to_string();
        Ok(route_command(self, "EXPIREAT", Some(&[key, &time]))?)
    }

    /// Return all the keys matching the passed pattern.
    /// While the time complexity is O(n), the constant times are quite fast. ~40ms for a 1 million key database.
    ///
    /// Arguments
    /// =========
    /// `pattern` - The pattern to search by.
    /// 
    /// Example:
    /// ```python
    /// client = RedisClient("url")
    /// 
    /// keys = {
    ///     "hello": "v",
    ///     "hallo": "v",
    ///     "hillo": "v",
    ///     "hollo": "v",
    ///     "hullo": "v"
    /// }
    /// 
    /// client.mset(keys)
    /// matches = client.keys("h[aeiou]llo")
    /// ```
    /// 
    /// Sequence Reply
    /// ==============
    /// A sequence of the keys matching the passed pattern.
    ///
    /// Time Complexity
    /// ===============
    /// `O(n)` : Where n is the number of keys in the database.
    ///
    /// [Read about KEYS in the Redis documentation.](https://redis.io/commands/keys)
    #[text_signature = "($self, pattern, /)"]
    pub fn keys(&mut self, pattern: &str) -> PyResult<Vec<String>> {
        Ok(route_command(self, "KEYS", Some(pattern))?)
    }

    /// Move the key to another database.
    ///
    /// Move key from the currently selected database (see SELECT) to the specified destination database. 
    /// When key already exists in the destination database, or it does not exist in the source database, it does nothing. 
    /// It is possible to use MOVE as a locking primitive because of this.
    ///
    /// Arguments
    /// =========
    /// `key` - The key.
    /// 
    /// `db` - The ID of the database to move to.
    /// 
    /// Example
    /// =======
    /// ```python  
    /// client = RedisClient("url")
    /// client.move("key", 1)
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// `1`: The key was moved.
    /// 
    /// `0`: The key was not moved. 
    ///
    /// Time Complexity
    /// =============== 
    /// `O(1)`
    ///
    /// [Read about MOVE in the Redis documentation.](https://redis.io/commands/move)
    #[text_signature = "($self, key, db, /)"]
    pub fn r#move(&mut self, key: &str, db: u8) -> PyResult<u8> {
        let id = db.to_string();
        Ok(route_command(self, "MOVE", Some(&[key, &id]))?)
    }

    /// Remove the existing timeout on a key, turning the key from volatile (a key with an expire set) 
    /// to persistent (a key that will never expire as no timeout is associated).
    ///
    /// Arguments
    /// =========
    /// `key` - The name of the key.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.persist("key") # Remove the expiration on a key. 
    /// ```
    ///
    /// Integer Reply
    /// =============
    /// `1`: The timeout was removed.
    /// 
    /// `0`: The timeout was not removed.
    ///
    /// Time Complexity
    /// ===============
    /// `O(1)`
    ///
    /// [Read about PERSIST in the Redis documentation.](https://redis.io/commands/persist)
    #[text_signature = "($self, key, /)"]
    pub fn persist(&mut self, key: &str) -> PyResult<i8> {
        Ok(route_command(self, "PERSIST", Some(key))?)
    }

    /// Works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of seconds.
    ///
    /// Arguments
    /// =========
    /// `key` - The key.
    /// 
    /// `timeout` - The timeout in milliseconds.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.pexpire("key", 12482394823)
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// `1`: The timeout was set.
    /// 
    /// `0`: The timeout was not set.
    ///
    /// Time Complexity
    /// ===============
    /// `O(1)`
    ///
    /// [Read about PEXPIRE in the Redis documentation.](https://redis.io/commands/pexpire)
    #[text_signature = "($self, key, timeout, /)"]
    pub fn pexpire(&mut self, key: &str, timeout: usize) -> PyResult<u8> {
        let time = timeout.to_string();
        Ok(route_command(self, "PEXPIRE", Some(&[key, &time]))?)
    }

    /// Has the same effect and semantic as EXPIREAT, 
    /// but the UNIX time at which the key will expire is specified in milliseconds instead of seconds.
    ///
    /// Arguments
    /// =========
    /// `key` - The key.
    /// 
    /// `timeout` - The expire time of the key in milliseconds.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.pexpireat("key", 4294967295)
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// `1`: The timeout was set.
    /// 
    /// `0`: The timeout was not set.
    ///
    /// Time Complexity
    /// `O(1)`
    ///
    /// [Read about PEXPIREAT in the Redis documentation.](https://redis.io/commands/pexpireat)
    #[text_signature = "($self, key, timeout, /)"]
    pub fn pexpireat(&mut self, key: &str, timeout: usize) -> PyResult<i8> {
        let time = timeout.to_string();
        Ok(route_command(self, "PEXPIREAT", Some(&[key, &time]))?)
    }

    /// Like TTL this command returns the remaining time to live of a key that has an expire set, 
    /// with the sole difference that TTL returns the amount of remaining time in seconds while PTTL 
    /// returns it in milliseconds.
    ///
    /// Arguments
    /// =========
    /// `key` - The key.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.pttl("key")
    /// ```    
    /// 
    /// Integer Reply
    /// =============
    /// `TTL in milliseconds`: success
    /// 
    /// `-1`: Key exists, but has no expiration set.
    /// 
    /// `-2`: Key does not exist.
    ///
    /// Time Complexity
    /// ===============
    /// `O(1)`
    ///
    /// [Read about PTTL in the Redis documentation.](https://redis.io/commands/pttl)
    #[text_signature = "($self, key, /)"]
    pub fn pttl(&mut self, key: &str) -> PyResult<i64> {
        Ok(route_command(self, "PTTL", Some(key))?)
    }

    /// Return a random key name. 
    /// 
    /// Arguments
    /// =========
    /// None
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// key = client.randomkey()
    /// ```    
    /// 
    /// Bulk String Reply
    /// =================
    /// The random key, or an empty string if the database is empty.
    ///
    /// Time Complexity
    /// ===============
    /// `O(1)`
    ///
    /// [Read about RANDOMKEY on the Redis documentation.](https://redis.io/commands/randomkey)
    #[text_signature = "($self)"]
    pub fn randomkey(&mut self) -> PyResult<String> {
        Ok(route_command::<Option<u8>, _>(self, "RANDOMKEY", None).unwrap_or_default())
    }

    /// Renames key to newkey. It returns "NO" when key does not exist. 
    /// If newkey already exists it is overwritten, when this happens RENAME executes an implicit DEL operation, 
    /// so if the deleted key contains a very big value it may cause high latency 
    /// even if RENAME itself is usually a constant-time operation.
    ///
    /// Arguments
    /// =========
    /// `key` - The key to rename.
    /// 
    /// `newkey` - The new name of the key.
    /// 
    /// `no_overwrite` - Set to True to do nothing if newkey already exists. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.rename("key", "key_2")
    /// ```
    /// 
    /// Simple String Reply
    /// ===================
    /// `"OK"`: The key was renamed.
    /// 
    /// `""`: The key does not exist.
    ///
    /// Time Complexity
    /// =============== 
    /// `O(1)`
    ///
    /// [Read about RENAME in the Redis documentation.](https://redis.io/commands/rename)
    #[args(no_overwrite="**")]
    #[text_signature = "($self, key, newkey, *, no_overwrite)"]
    pub fn rename(&mut self, key: &str, newkey: &str, no_overwrite: Option<&PyDict>) -> PyResult<String> {
        let command = nx_x_decider("RENAME", "NX", no_overwrite);
        Ok(route_command(self, &command, Some(&[key, newkey])).unwrap_or_default())
    }

    /// Returns the remaining time to live of a key that has a timeout. 
    /// This introspection capability allows a Redis client to check how many seconds a 
    /// given key will continue to be part of the dataset.
    ///
    /// Arguments
    /// =========
    /// `key` - The name of the key.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.ttl("key")
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// `TTL in seconds`: success
    /// 
    /// `-1`: Key exists but has no associated expire.
    /// 
    /// `-2`: Key does not exist.
    ///
    /// Time Complexity
    /// ===============
    /// `O(1)`
    ///
    /// [Read about TTL in the Redis documentation.](https://redis.io/commands/ttl)
    #[text_signature = "($self, key, /)"]
    pub fn ttl(&mut self, key: &str) -> PyResult<isize> {
        Ok(route_command(self, "TTL", Some(key))?)
    }

    /// Returns the string representation of the type of the value stored at key. 
    /// The different types that can be returned are: string, list, set, zset, hash and stream.
    ///
    /// Arguments
    /// =========
    /// `key` - The name of the key.
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// keytype = client.keytype("key")
    /// ```
    /// 
    /// Simple String Reply
    /// ===================
    /// The type of the key.
    ///
    /// Time Complexity
    /// ===============
    /// `O(1)`
    ///
    /// [Read about TYPE in the Redis documentation.](https://redis.io/commands/type)
    #[text_signature = "($self, key, /)"]
    pub fn keytype(&mut self, key: &str) -> PyResult<String> {
        Ok(route_command(self, "TYPE", Some(key))?)
    }

    /// This command is very similar to DEL: it removes the specified keys. 
    /// Just like DEL a key is ignored if it does not exist. 
    /// However the command performs the actual memory reclaiming in a different thread, so it is not blocking, 
    /// while DEL is. This is where the command name comes from: the command just unlinks the keys from the keyspace. 
    /// The actual removal will happen later asynchronously.
    /// 
    /// Arguments
    /// =========
    /// `keys` - A list of keys to unlink by name. Passed as rest arguments. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client = RedisClient("url")
    /// client.unlink("key")  // Unlinking a key.
    /// client.unlink("key1", 2)  // Unlinking multiple keys. 2 here is == "2" (a key named "2")
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// The number of keys that were unlinked.
    ///
    /// Time Complexity
    /// ===============
    /// `O(1)` : For each key removed regardless of its size. 
    /// 
    /// `O(n)` : Where n = the number of allocations the deleted objects were composed of. 
    ///
    /// [Read about UNLINK in the Redis documentation.](https://redis.io/commands/unlink)
    #[args(keys="*")]
    #[text_signature = "($self, *keys)"]
    pub fn unlink(&mut self, keys: Vec<&PyAny>) -> PyResult<usize> {
        let args = construct_vector(keys.len(), Cow::from(&keys))?;
        Ok(route_command(self, "UNLINK", Some(args))?)
    }
}