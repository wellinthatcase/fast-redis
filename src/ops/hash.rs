//! Implementation of hash operations for the client. 
use crate::*; 

#[pymethods]
impl RedisClient {
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
}