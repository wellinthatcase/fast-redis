// MIT License
//
// Copyright (c) 2020 wellinthatcase
//
// Terms are found in the LICENSE.txt file.

#![allow(dead_code, clippy::or_fun_call, irrefutable_let_patterns)]

use redis::*;
use std::borrow::Cow;
use std::collections::HashMap;
use pyo3::{
    Python, types::*,
    PyTraverseError, PyVisit,
    PyNativeType, prelude::*, exceptions,
    class::basic::PyObjectProtocol, PyGCProtocol
};

fn route_command<Args, ReturnType>(
    inst: &mut RedisClient, 
    cmd: &str, 
    args: Option<Args>
) -> PyResult<ReturnType>
where 
    Args: ToRedisArgs,
    ReturnType: FromRedisValue
{
    let mut call = redis::cmd(cmd);
    call.arg(args);

    match call.query(&mut inst.connection) as RedisResult<ReturnType> {
        Ok(v) => Ok(v),
        Err(v) => {
            let detail = Cow::from(v.detail().unwrap_or_else(|| "Unknown exception!").to_string());
            
            match v.kind() {
                ErrorKind::ExtensionError => exceptions::TypeError::into(detail),
                ErrorKind::TypeError => exceptions::TypeError::into(detail),
                ErrorKind::IoError => exceptions::IOError::into(detail),
                _ => exceptions::Exception::into(detail)
            }
        }
    }
}

/// Converts a `Cow<[&PyAny]>` into a Redis applicable format. 
fn construct_vector(capacity: usize, other: Cow<[&PyAny]>) -> PyResult<Vec<String>> {
    let mut vector = Vec::with_capacity(capacity);
    
    vector.extend(other.iter().map(|element| element.to_string()).collect::<Vec<String>>());

    Ok(vector)
}

/// Decides whether a command should be the NX/X varient.
/// 
/// # Arguments 
/// * `base` - The original command. Ex, `SET` being the original of `SETNX`. 
/// * `additive` - What to add if `no_overwrite` is true. NX/X. 
/// * `choice` - The choice the user may of passed. 
/// 
/// # Returns 
/// The corrected command. Whether NX/X or not. 
fn nx_x_decider(base: &'static str, additive: &'static str, choice: Option<&PyDict>) -> String {
    let gil = Python::acquire_gil();
    let mut res = String::with_capacity(base.len() + additive.len());
    let raw_choice = choice.unwrap_or_else(|| PyDict::new(gil.python())).get_item("no_overwrite"); 

    res.push_str(base);

    if let Some(val) = raw_choice {
        match val.to_string().to_ascii_lowercase().parse::<bool>() {
            Ok(val) => { 
                if val { res.push_str(additive); } else {};
                res
            }, 
            Err(_) => res
        }
    } else { res }
} 

/// The main client for suredis. 
/// 
/// Attributes
/// ==========
/// `db` - The DB the connection interacts with. See [SELECT](https://redis.io/commands/select).
/// `url` - The URL you passed to establish the Redis client.
/// `supports_pipelining` - Whether the connection supports pipelines. 
/// 
/// Note 
/// ====
/// 1. Unsupported Redis operations can be accessed with the `manual` method.
/// 2. It is preferred to prefix your URL with `redis://`.
#[pyclass(gc)]
struct RedisClient {
    #[pyo3(get)]                       //
    db: i64,                           // The DB the connection interacts with. 
    #[pyo3(get)]                       //
    url: String,                       // The URL used to establish the Redis client.
    o: Option<PyObject>,               // Used to support the CPython Garbage Collection protocol.
    client: redis::Client,             // The internal Redis client.
    connection: Connection,            // The internal Redis connection.
    #[pyo3(get)]                       //
    supports_pipelining: bool,         // Whether the connection supports pipelining. 
}

#[pymethods]
impl RedisClient {
    /// An low-level interface for making more advanced commands to Redis.
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

    /// Removes the specified fields from the hash stored at key. 
    /// Specified fields that do not exist within this hash are ignored. 
    /// If key does not exist, it is treated as an empty hash and this command returns 0.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key.
    /// * `fields` - The fields to be removed. Passed as rest arguments. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hdel("key", "field1", "field2")
    /// ```
    /// 
    /// # Integer Reply: 
    /// * The number of fields that were removed from the hash, not including specified but non existing fields.
    /// 
    /// # Time Complexity: 
    /// * O(n) : Where n = the number of fields to be removed.
    /// 
    /// [Read about HDEL in the Redis documentation.](https://redis.io/commands/hdel)
    #[args(fields="*")]
    #[text_signature = "($self, key, fields, /)"]
    pub fn hdel(&mut self, key: String, fields: Vec<&PyAny>) -> PyResult<usize> {
        let mut arguments = construct_vector(fields.len() + 1, Cow::from(&fields))?;
        arguments.insert(0, key);
        
        Ok(route_command(self, "HDEL", Some(arguments))?)
    }

    /// Returns if field is an existing field in the hash stored at key.
    ///
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `field`- The value of the field. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hexists("key", "field")
    /// ```
    /// 
    /// # Integer Reply: 
    /// * 1: The hash contains the field. 
    /// * 0: The hash does not contain the field, or the key does not exist. 
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about HEXISTS in the Redis documentation.](https://redis.io/commands/hexists)
    #[text_signature = "($self, key, field, /)"]
    pub fn hexists(&mut self, key: &str, field: &str) -> PyResult<u8> {
        Ok(route_command(self, "HEXISTS", Some(&[key, field]))?)
    }

    /// Returns the value associated with field in the hash stored at key.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `field` - The name of the field. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hget("key", "field")
    /// ```
    /// 
    /// # Bulk String Reply: 
    /// * The value associated with field, or nil when field is not present in the hash or key does not exist.
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about HGET in the Redis documentation.](https://redis.io/commands/hget)
    #[text_signature = "($self, key, field, /)"]
    pub fn hget(&mut self, key: &str, field: &str) -> PyResult<String> {
        Ok(route_command(self, "HGET", Some(&[key, field]))?)
    }

    /// Returns all fields and values of the hash stored at key. 
    /// In the returned value, every field name is followed by its value, 
    /// so the length of the reply is twice the size of the hash.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hgetall("key")
    /// ```
    /// 
    /// # Array Reply: 
    /// * A list of fields and their values stored in the hash, or an empty list when key does not exist.
    /// 
    /// # Time Complexity: 
    /// * O(n) : Where n = the size of the hash.
    /// 
    /// [Read about HGETALL in the Redis documentation.](https://redis.io/commands/hgetall)
    #[text_signature = "($self, key, /)"]
    pub fn hgetall(&mut self, key: &str) -> PyResult<Vec<String>> {
        Ok(route_command(self, "HGETALL", Some(key))?)
    }

    /// Increments the number stored at field in the hash stored at key by increment. 
    /// If key does not exist, a new key holding a hash is created. 
    /// If field does not exist the value is set to 0 before the operation is performed.
    /// The range of values supported is limited to 64 bit signed integers.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `field` - The name of the field. 
    /// * `amount` - The amount to increment by. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hincrby("key", "field", 5)
    /// ```
    /// 
    /// # Integer Reply: 
    /// * The value of the field after the increment. 
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about HINCRBY in the Redis documentation.](https://redis.io/commands/hincrby)
    #[text_signature = "($self, key, field, amount, /)"]
    pub fn hincrby(&mut self, key: &str, field: &str, amount: i64) -> PyResult<isize> {
        let amt = amount.to_string();
        Ok(route_command(self, "HINCRBY", Some(&[key, field, &amt]))?)
    }

    /// Increment the specified field of a hash stored at key, and representing a floating point number, 
    /// by the specified increment. If the increment value is negative, 
    /// the result is to have the hash field value decremented instead of incremented. 
    /// If the field does not exist, it is set to 0 before performing the operation. 
    /// 
    /// An error is returned if one of the following conditions occur:
    /// * The field contains a value of the wrong type (not a string).
    /// * The current field content or the specified increment are not parsable 
    ///   as a double precision floating point number.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `field` - The name of the field. 
    /// * `amount` - The amount to increment by. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hincrbyfloat("key", "field", 5.0)
    /// ```
    /// 
    /// # Integer Reply: 
    /// * The value of the field after the increment. 
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about HINCRBY in the Redis documentation.](https://redis.io/commands/hincrby)
    #[text_signature = "($self, key, field, amount, /)"]
    pub fn hincrbyfloat(&mut self, key: &str, field: &str, amount: f64) -> PyResult<f64> {
        let amt = amount.to_string();
        Ok(route_command(self, "HINCRBYFLOAT", Some(&[key, field, &amt]))?)
    }

    /// Returns all field names in the hash stored at key.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hkeys("key")
    /// ```
    /// 
    /// # Array Reply: 
    ///*  A list of fields in the hash, or an empty list when key does not exist.
    /// 
    /// # Time Complexity: 
    /// * O(n) : Where n = the size of the hash.
    /// 
    /// [Read about HKEYS in the Redis documentation.](https://redis.io/commands/hkeys)
    #[text_signature = "($self, key, /)"]
    pub fn hkeys(&mut self, key: &str) -> PyResult<Vec<String>> {
        Ok(route_command(self, "HKEYS", Some(key))?)
    }

    /// Returns the number of fields contained in the hash stored at key.
    /// 
    /// # Arguments:
    /// * `key` - The name of the key. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hlen("key")
    /// ```
    /// 
    /// # Integer Reply: 
    /// * The amount of fields on the key. 
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about HLEN in the Redis documentation.](https://redis.io/commands/hlen)
    #[text_signature = "($self, key, /)"]
    pub fn hlen(&mut self, key: &str) -> PyResult<usize> {
        Ok(route_command(self, "HLEN", Some(key))?)
    }

    /// Returns the values associated with the specified fields in the hash stored at key.
    /// For every field that does not exist in the hash, a nil value is returned. 
    /// Because non-existing keys are treated as empty hashes, 
    /// running HMGET against a non-existing key will return a list of nil values.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `fields` - The name(s) of the fields. Passed as rest arguments. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hmget("key", "field", "field2")
    /// ```
    /// 
    /// # Array Reply: 
    /// * A list of values associated with the given fields, in the same order as they are requested.
    /// 
    /// # Time Complexity: 
    /// * O(n) : Where n = the number of fields being requested.
    /// 
    /// [Read about HMGET in the Redis documentation.](https://redis.io/commands/hmget)
    #[args(fields="*")]
    #[text_signature = "($self, key, fields, /)"]
    pub fn hmget(&mut self, key: String, fields: Vec<&PyAny>) -> PyResult<Vec<String>> {
        let mut arguments = construct_vector(fields.len() + 1, Cow::from(&fields))?;
        arguments.insert(0, key);

        Ok(route_command(self, "HMGET", Some(arguments))?)
    }

    /// Sets field in the hash stored at key to value. 
    /// If key does not exist, a new key holding a hash is created. 
    /// If field already exists in the hash, it is overwritten.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `field` - A field name : field value dictionary mapping. 
    /// * `no_overwrite` - Set to True to avoid overwriting. Otherwise, False. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// fields = {
    ///     "name": "value",
    ///     "name2": "value2"
    /// }
    /// client.hset("key", fields, no_overwrite=True)
    /// ```
    /// 
    /// # Integer Reply:
    /// * The number of fields that were added. 
    /// 
    /// # Time Complexity: 
    /// * O(n) : Where n = the amount of field/value pairs to add. 
    /// 
    /// [Read about HSET in the Redis documentation.](https://redis.io/commands/hset)
    #[args(no_overwrite="**")]
    #[text_signature = "($self, key, fields, *, no_overwrite)"]
    pub fn hset(&mut self, key: String, fields: HashMap<String, &PyAny>, no_overwrite: Option<&PyDict>) -> PyResult<usize> {
        let command = nx_x_decider("HSET", "NX", no_overwrite);
        let mut args = Vec::with_capacity((fields.len() * 2) + 1);
            
        args.push(key);

        for (key, value) in fields.iter() {
            args.push(key.to_string());
            args.push(value.to_string());
        }   

        Ok(route_command(self, &command, Some(args))?)
    }

    /// Returns the string length of the value associated with field in the hash stored at key. 
    /// If the key or the field do not exist, 0 is returned.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `field` - The name of the field. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.hstrlen("key", "field")
    /// ```
    /// 
    /// # Integer Reply: 
    /// * The length of the string at the field, or 0 if the field was not found.
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about HSTRLEN in the Redis Documentation.](https://redis.io/commands/hstrlen)
    #[text_signature = "($self, key, field, /)"]
    pub fn hstrlen(&mut self, key: &str, field: &str) -> PyResult<usize> {
        Ok(route_command(self, "HSTRLEN", Some(&[key, field]))?)
    }

    /// Returns all values in the hash stored at key.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// 
    /// # Example: 
    /// ```
    /// client = RedisClient("url")
    /// client.hvals("key")
    /// ```
    /// 
    /// # Array Reply: 
    /// A list of the values in the hash. 
    /// 
    /// # Time Complexity: 
    /// * O(n) : Where n = the size of the hash.
    /// 
    /// [Read more about HVALS in the Redis documentation.](https://redis.io/commands/hvals)
    #[text_signature = "($self, key, /)"]
    pub fn hvals(&mut self, key: &str) -> PyResult<Vec<String>> {
        Ok(route_command(self, "HVALS", Some(key))?)
    }

    /// Insert all the specified values at the tail of the list stored at key. 
    /// If key does not exist, it is created as empty list before performing the push operation. 
    /// When key holds a value that is not a list, an error is returned.
    /// It is possible to push multiple elements using a single command call just specifying multiple arguments 
    /// at the end of the command. Elements are inserted one after the other to the tail of the list, from the 
    /// leftmost element to the rightmost element.
    /// 
    /// If key does not exist, a new key is created with the name along with the list datatype. 
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `no_overwrite` - Whether to overwrite duplicates or not. True to not overwrite, False otherwise.
    /// * `elements` - The elements to push. Passed as rest arguments. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.rpush("key", "1", "2", "3.0", no_overwrite=True)
    /// ```
    /// 
    /// # Integer Reply: 
    /// The length of the list after the elements have been pushed. 
    /// 
    /// # Time Complexity: 
    /// * O(1) : For each element added.
    /// * O(n) : Where n = the amount of elements.
    /// 
    /// [Read about RPUSH in the Redis documentation.](https://redis.io/commands/rpush)
    #[args(elements="*", no_overwrite="**")]
    #[text_signature = "($self, key, elements, *, no_overwrite)"]
    pub fn rpush(&mut self, key: String, elements: Vec<&PyAny>, no_overwrite: Option<&PyDict>) -> PyResult<usize> {
        let command = nx_x_decider("RPUSH", "X", no_overwrite);
        let mut args = construct_vector(elements.len() + 1, Cow::from(&elements))?; 
        args.insert(0, key);
        Ok(route_command(self, &command, Some(args))?)
    }

    /// Insert all the specified values at the start of the list stored at key. 
    /// If key does not exist, it is created as empty list before performing the push operation. 
    /// When key holds a value that is not a list, an error is returned.
    /// It is possible to push multiple elements using a single command call just specifying multiple arguments 
    /// at the end of the command. Elements are inserted one after the other to the start of the list, from the 
    /// rightmost element to the leftmost element.
    /// 
    /// If key does not exist, a new key is created with the name along with the list datatype. 
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `elements` - The elements to push. Passed as rest arguments. 
    /// * `no_overwrite` - Whether to prepend the elements to the list, only if the list exists. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.lpush("key", "1", "2", "3.0", no_overwrite=False)
    /// ```
    /// 
    /// # Integer Reply: 
    /// The length of the list after the elements have been pushed. 
    /// 
    /// # Time Complexity: 
    /// * O(1) : For each element added.
    /// * O(n) : Where n = the amount of elements.
    /// 
    /// [Read about LPUSH in the Redis documentation.](https://redis.io/commands/lpush)
    #[args(elements="*", no_overwrite="**")]
    #[text_signature = "($self, key, elements, *, no_overwrite)"]
    pub fn lpush(&mut self, key: String, elements: Vec<&PyAny>, no_overwrite: Option<&PyDict>) -> PyResult<usize> {
        let command = nx_x_decider("LPUSH", "X", no_overwrite);
        let mut args = construct_vector(elements.len() + 1, Cow::from(&elements))?;
        args.insert(0, key);
        Ok(route_command(self, &command, Some(args))?)
    }

    /// Returns the element at index index in the list stored at key.
    ///
    /// When the value at key is not a list, an error is returned.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key.
    /// * `index` - The index of the desired item. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.lindex("key", 1)
    /// ```
    /// 
    /// # Bulk Object Reply: 
    /// The element at the index, or nil if the index doesn't exist. 
    /// 
    /// # Time Complexity: 
    /// * O(n) : Where n = the number of elements to traverse to get to the element at index.
    /// * O(1) : If the desired element is the first, or last element in the collection. 
    /// 
    /// [Read about LINDEX in the Redis documentation.](https://redis.io/commands/lindex)
    #[text_signature = "($self, key, index, /)"]
    pub fn lindex(&mut self, key: &str, index: isize) -> PyResult<String> {
        let ind = index.to_string();
        Ok(route_command(self, "LINDEX", Some(&[key, &ind]))?)
    }

    /// Inserts element in the list stored at key either before or after the reference value pivot.
    /// When key does not exist, it is considered an empty list and no operation is performed.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `element` - The element to insert. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.linsert("key", "element")
    /// ```
    /// 
    /// # Integer Reply: 
    /// The length of the list after the insert operation, or -1 when the value pivot was not found.
    /// 
    /// # Time Complexity: 
    /// * O(n) : Where n = the number of elements to traverse before seeing the value pivot. 
    /// 
    /// [Read about LINSERT in the Redis documentation.](https://redis.io/commands/linsert)
    #[text_signature = "($self, key, element, /)"]
    pub fn linsert(&mut self, key: &str, element: &str) -> PyResult<isize> {
        Ok(route_command(self, "LINSERT", Some(&[key, element]))?)
    }

    /// Returns the length of the list stored at key. 
    /// If key does not exist, it is interpreted as an empty list and 0 is returned. 
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// 
    /// # Example:  
    /// ```python
    /// client = RedisClient("url")
    /// client.lpush("key", "my", "elements")
    /// client.llen("key") == 2
    /// ```
    /// 
    /// # Integer Reply:
    /// * The length of the list.
    /// * If the key does not exist, 0 is returned. 
    ///     
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about LLEN in the Redis documentation.](https://redis.io/commands/llen)
    #[text_signature = "($self, key, /)"]
    pub fn llen(&mut self, key: &str) -> PyResult<isize> {
        Ok(route_command(self, "LLEN", Some(key))?)
    }

    /// Removes and returns the first element of the list stored at key.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// 
    /// # Example: 
    /// ```python
    /// client = RedisClient("url")
    /// client.lpop("key")
    /// ```
    /// 
    /// # Bulk String Reply:
    /// The leftmost element of the list, or nil when the key does not exist. 
    /// 
    /// # Time Complexity: 
    /// * O(1)
    /// 
    /// [Read about LPOP in the Redis documentation.](https://redis.io/commands/lpop)
    #[text_signature = "($self, key, /)"]
    pub fn lpop(&mut self, key: &str) -> PyResult<String> {
        Ok(route_command(self, "LPOP", Some(key))?)
    }

    // TODO: Improve documentation. Specifially, time complexities.
    
    /// Set the value of an element in a list by its index.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `index` - The desired index. 
    /// * `element` - The new element.
    /// 
    /// # Bulk String Reply: 
    /// * "OK" on success. Otherwise, an error will raise.
    /// 
    /// [Read about LSET in the Redis documentation.](https://redis.io/commands/lset)
    #[text_signature = "($self, key, index, element, /)"]
    pub fn lset(&mut self, key: &str, index: usize, element: &PyAny) -> PyResult<String> {
        let ind = index.to_string();
        let elem = element.to_string();
        Ok(route_command(self, "LSET", Some(&[key, &ind, &elem]))?)
    }

    /// Get a range of elements from a list. 
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `beginning` - The starting index. 
    /// * `end` - The ending index.
    /// 
    /// # Array reply: 
    /// A list of the elements, or an empty list if the key was not a list,
    /// was an empty list, or the beginning/end was an invalid index.
    /// 
    /// [Read about LRANGE on the Redis documentation.](https://redis.io/commands/lrange)
    #[text_signature = "($self, key, beginning, end, /)"]
    pub fn lrange(&mut self, key: &str, beginning: usize, end: usize) -> PyResult<Vec<String>> {
        let start = beginning.to_string();
        let stop = end.to_string();
        Ok(route_command(self, "LRANGE", Some(&[key, &start, &stop]))?)
    }

    /// Remove elements from the left side of a list. 
    /// 
    /// Best suited for duplicate element removal, it seems.
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `amt` - The amount of elements to remove. 
    /// * `elems` - The elements to remove. 
    /// 
    /// # Example: 
    /// ```python
    /// client.lrem("key", 3, "element 1", "element 2", "element 3, 4, 5")
    /// ```
    /// 
    /// # Integer Reply: 
    /// The amount of elements removed. 
    /// 
    /// [Read about LREM on the Redis documentation.](https://redis.io/commands/lrem)
    #[args(elems="*")]
    #[text_signature = "($self, key, amt, elems, /)"]
    pub fn lrem(&mut self, key: String, amt: usize, elems: Vec<&PyAny>) -> PyResult<usize> {
        let mut arguments = construct_vector(elems.len() + 2, Cow::from(&elems))?;
        arguments.insert(0, amt.to_string());
        arguments.insert(0, key);

        Ok(route_command(self, "LREM", Some(arguments))?)
    }

    /// Trim a list to the specified range. 
    /// 
    /// # Arguments: 
    /// * `key` - The name of the key. 
    /// * `beginning` - The starting index. 
    /// * `end` - The stopping index.
    /// 
    /// # Example: 
    /// ```python
    /// // mylist = [1, 2, 3, 4, 5]
    /// client.ltrim("mylist", 1, -2) // trim all that are not within the index 1 to -2
    /// // mylist = [2, 3, 4]
    /// ```
    /// 
    /// # Integer Reply: 
    /// The length of the trimmed list. 
    /// 
    /// [Read about LTRIM on the Redis documentation.](https://redis.io/commands/ltrim)
    #[text_signature = "($self, key, beginning, end, /)"]
    pub fn ltrim(&mut self, key: &str, beginning: isize, end: isize) -> PyResult<usize> {
        let stop = end.to_string();
        let start = beginning.to_string();
        Ok(route_command(self, "LTRIM", Some(&[key, &start, &stop]))?)
    }

    /// Remove and return the last element in a list. 
    /// 
    /// Arguments: 
    /// * `key` - The name of the key. 
    /// 
    /// # Example: 
    /// ```
    /// client.lpush("mylist", "item", "last item")
    /// client.rpop("mylist") == "last item"
    /// client.rpop("mylist") == "item"
    /// ```
    /// 
    /// # Bulk String Reply: 
    /// The last item of the list.
    /// 
    /// [Read about RPOP in the Redis documentation.](https://redis.io/commands/rpop)
    #[text_signature = "($self, key, /)"]
    pub fn rpop(&mut self, key: &str) -> PyResult<String> {
        Ok(route_command(self, "RPOP", Some(key))?)
    }

    /// Get the elements of a list. 
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key that has the list. 
    /// 
    /// Example 
    /// =======
    /// ```python
    /// client.rpush("key", 1, 2, 3, 4, 5)
    /// client.lelements("key") == ["1", "2", "3", "4", "5"]
    /// ```
    /// 
    /// Array Reply
    /// ===========
    /// The elements from the list. 
    #[text_signature = "($self, key, /)"]
    pub fn lelements(&mut self, key: &str) -> PyResult<Vec<String>> {
        let stop = self.llen(key)?.to_string();
        Ok(route_command(self, "LRANGE", Some(&[key, "0", &stop]))?)
    }

    /// Remove the last element in a list, prepend it to another list and return it.
    /// 
    /// # Arguments: 
    /// * `source` - The source key name. 
    /// * `destination ` - The destination key name. 
    /// 
    /// # Bulk String Reply: 
    /// The object being swapped by the operation.
    /// 
    /// [Read about RPOPLPUSH in the Redis documentation.](https://redis.io/commands/rpoplpush)
    #[text_signature = "($self, source, destination, /)"]
    pub fn rpoplpush(&mut self, source: &str, destination: &str) -> PyResult<String> {
        Ok(route_command(self, "RPOPLPUSH", Some(&[source, destination]))?)
    }

    /// Add one or more members to a set. 
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// `members` - The members to add to the set. Passed as rest arguments. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client.sadd("key", 1, "mem 2", 3, "mem4")
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// The length of the set after the operation. 
    /// 
    /// [Read about SADD in the Redis documentation.](https://redis.io/commands/sadd)
    #[args(members="*")]
    #[text_signature = "($self, key, members, /)"]
    pub fn sadd(&mut self, key: String, members: Vec<&PyAny>) -> PyResult<usize> {
        let mut mems = construct_vector(members.len() + 1, Cow::from(&members))?;
        mems.insert(0, key);

        Ok(route_command(self, "SADD", Some(mems))?)
    }

    /// Get the amount of members in a set. 
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client.sadd("key", 1, 2, 3, 4, 5, 9)
    /// client.scard("key") == 6
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// The amount of members in the set. 
    /// 
    /// [Read about SCARD in the Redis documentation.](https://redis.io/commands/scard)
    #[text_signature = "($self, key, /)"]
    pub fn scard(&mut self, key: &str) -> PyResult<usize> {
        Ok(route_command(self, "SCARD", Some(key))?)
    }

    /// Subtract two or more sets. 
    /// 
    /// Arguments
    /// =========
    /// `keys` - Key names of all the sets. Passed as rest arguments. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client.sadd("key1", 1, 2, 3, 4, 5)
    /// client.sadd("key2", 1, 2, 3)
    /// 
    /// client.sdiff("key1", "key2") == ["4", "5"]
    /// ```
    /// 
    /// Array Reply
    /// ===========
    /// The difference between all of the sets.
    /// 
    /// [Read about SDIFF in the Redis documentation.](https://redis.io/commands/sdiff)
    #[args(keys="*")]
    #[text_signature = "($self, keys, /)"]
    pub fn sdiff(&mut self, keys: Vec<&PyAny>) -> PyResult<Vec<String>> {
        let arguments = construct_vector(keys.len(), Cow::from(&keys))?;
        Ok(route_command(self, "SDIFF", Some(arguments))?)
    }

    /// Subtract two or more sets and store the resulting set in a key. 
    /// 
    /// Arguments
    /// =========
    /// `destination` - The destination key. 
    /// 
    /// `keys` - The key's which have the sets to subtract. Passed as rest arguments. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client.sadd("key", 1, 2, 3, 4, 5)
    /// client.sadd("key2", 1)
    /// client.sdiff("key", "key2") == ["2", "3", "4", "5"]
    /// client.sdiffstore("destination", "key", "key2", "key3")
    /// 
    /// client.scard("destination") == 4
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// The length of the resulting set. 
    /// 
    /// [Read about SDIFFSTORE in the Redis documentation.](https://redis.io/commands/sdiffstore)
    #[args(keys="*")]
    #[text_signature = "($self, destination, keys, /)"]
    pub fn sdiffstore(&mut self, destination: String, keys: Vec<&PyAny>) -> PyResult<usize> {
        let mut args = construct_vector(keys.len() + 1, Cow::from(&keys))?;
        args.insert(0, destination);
        Ok(route_command(self, "SDIFFSTORE", Some(args))?)
    }

    /// Intersect two or more sets. 
    /// 
    /// Arguments
    /// =========
    /// `keys` - The sets to intersect. Passed as rest arguments. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client.sadd("a", 1, 2, 3, 4)
    /// client.sadd("b", 4, 5, 6, 7)
    /// client.sinter("a", "b") == ["4"]
    /// ```
    /// 
    /// Array Reply
    /// ===========
    /// The intersected set, or an empty array if the sets had no members in common. 
    /// 
    /// [Read about SINTER in the Redis documentation.](https://redis.io/commands/sinter)
    #[args(keys="*")]
    #[text_signature = "($self, keys, /)"]
    pub fn sinter(&mut self, keys: Vec<&PyAny>) -> PyResult<Vec<String>> {
        let args = construct_vector(keys.len(), Cow::from(&keys))?;
        Ok(route_command(self, "SINTER", Some(args))?)
    }

    /// Intersect two or more sets and store the resulting set in `destination`. 
    /// 
    /// Arguments
    /// =========
    /// `destination` - The name of the key to store the result in. 
    /// `keys` - The sets to intersect. Passed as rest arguments. 
    /// 
    /// Example 
    /// =======
    /// ```python
    /// client.sadd("a", 1, 2, 3, 4)
    /// client.sadd("b", 4, 5, 6, 7)
    /// client.sinterstore("dest", "a", "b")
    /// client.smembers("dest") == ["4"]
    /// ```
    /// 
    /// Integer Reply
    /// ============= 
    /// The length of the resulting set. 
    /// 
    /// [Read about SINTERSTORE in the Redis documentation.](https://redis.io/commands/sinter)
    #[args(keys="*")]
    #[text_signature = "($self, destination, keys, /)"]
    pub fn sinterstore(&mut self, destination: String, keys: Vec<&PyAny>) -> PyResult<Vec<String>> {
        let mut args = Vec::with_capacity(keys.len() + 1);
        args.push(destination);
        args.extend(construct_vector(keys.len(), Cow::from(&keys))?.into_iter());
        Ok(route_command(self, "SINTERSTORE", Some(args))?)
    }

    /// Determine if a given value is a member of a set. 
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key. 
    /// 
    /// `member` - The member to check. 
    /// 
    /// Example
    /// =======
    /// ```python 
    /// client.sadd("a", 1, 2, 3, 4)
    /// client.sismember("a", 5) == 0 # False
    /// client.sismember("a", 2) == 1 # True
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// `1`: The member is in the set. 
    /// 
    /// `0`: The member is not in the set, or the key was not a set.
    /// 
    /// [Read about SISMEMBER in the Redis documentation.](https://redis.io/commands/sismember)
    #[text_signature = "($self, key, member, /)"]
    pub fn sismember(&mut self, key: &str, member: &PyAny) -> PyResult<u8> {
        let mem = member.to_string();
        Ok(route_command(self, "SISMEMBER", Some(&[key, &mem]))?)
    }

    /// Get all of the members in a set. 
    /// 
    /// Arguments
    /// =========
    /// `key` - The name of the key with the set. 
    /// 
    /// Example
    /// =======
    /// ```python
    /// client.sadd("a", "hello", "world")
    /// client.smembers("a") == ["hello", "world"]
    /// ```
    /// 
    /// Array Reply
    /// ===========
    /// The members in the set. 
    /// 
    /// [Read about SMEMBERS in the Redis documentation.](https://redis.io/commands/smembers)
    #[text_signature = "($self, key, /)"]
    pub fn smembers(&mut self, key: &str) -> PyResult<Vec<String>> {
        Ok(route_command(self, "SMEMBERS", Some(key))?)
    }

    /// Move a member from one set to another. 
    /// 
    /// Arguments
    /// ========= 
    /// `source` - The key which has the member. 
    /// 
    /// `destination` - The destination key for the member. 
    /// 
    /// `member` - The member to move. 
    /// 
    /// Example 
    /// =======
    /// ```python 
    /// client.sadd("a", 1, 2, 3)
    /// client.sadd("b", 1, 2, 3, 4)
    /// client.smove("b", "a", 4)
    /// client.smembers("a") == ["1", "2", "3", "4"]
    /// ```
    /// 
    /// Integer Reply
    /// =============
    /// `1`: The member was moved. 
    /// 
    /// `0`: The member does not exist. 
    /// 
    /// [Read about SMOVE in the Redis documentation.](https://redis.io/commands/smove)
    #[text_signature = "($self, source, destination, member, /)"]
    pub fn smove(&mut self, source: &str, destination: &str, member: &PyAny) -> PyResult<u8> {
        let mem = member.to_string();
        Ok(route_command(self, "SMOVE", Some(&[source, destination, &mem]))?)
    }

    #[new]
    fn __new__(url: &str) -> PyResult<PyClassInitializer<Self>> {
        let protected_url = if url.starts_with("redis://") {
            url.to_string()
        } else {
            format!("redis://{}", url)
        };

        let client = Client::open(protected_url)
            .expect("could not establish a Redis client");

        let connection = client.get_connection()
            .expect("could not establish a connection. Is the server running?");

        let db = connection.get_db();
        let supports_pipelining = true;

        let instance: Self = Self {
            db,
            client,
            o: None,
            connection,
            supports_pipelining,
            url: url.to_string()
        };

        Ok(PyClassInitializer::from(instance))
    }
}

// Needed to implement GC support. 
unsafe impl PyNativeType for RedisClient {}

// GC support. 
#[pyproto]
impl PyGCProtocol for RedisClient {
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        if let Some(ref object) = self.o {
            visit.call(object)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        if let Some(obj) = self.o.take() {
            self.py().release(obj);
        }
    }
}

#[pyproto]
impl PyObjectProtocol for RedisClient {
    fn __str__(&self) -> PyResult<String> {
        Ok(format!("<RedisClient db={} pipelinable={} url={}>", self.db, self.supports_pipelining, &self.url))
    }
}

/// A speedy & simplistic library at runtime for an incredibly straightforward Redis interface.
#[pymodule]
fn suredis(_py: Python, module: &PyModule) -> PyResult<()> {
    module.add_class::<RedisClient>()?;
    Ok(())
}