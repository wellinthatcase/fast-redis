// MIT License
//
// Copyright (c) 2020 wellinthatcase
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use redis::*;
use pyo3::{
    prelude::*,
    PyGCProtocol, PyTraverseError, PyVisit, PyNativeType
};

#[pyclass]
struct RedisClient {
    client: redis::Client,             // The internal Redis client.
    connection: redis::Connection,     // The internal Redis connection.
    url: String,                       // The url used to establish the Redis client.
    o: Option<PyObject>                // Used to support the CPython Garbage Collection protocol.
}

unsafe impl PyNativeType for RedisClient {}

#[pyproto]
impl PyGCProtocol for RedisClient {
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        if let Some(ref object) = self.o {
            visit.call(object)?
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        if let Some(obj) = self.o.take() {
            self.py().release(obj);
        }
    }
}

#[pymethods]
impl RedisClient {
    #[new]
    fn __new__(url: String) -> PyResult<PyClassInitializer<Self>> {
        let client: redis::Client = redis::Client::open(url.as_ref())
            .expect("could not establish a Redis client.");
        let connection: redis::Connection = client.get_connection()
            .expect("could not establish a connection with the Redis client. Is the server running?");

        let instance: RedisClient = RedisClient {
            o: None,
            url: url,
            client: client,
            connection: connection
        };

        Ok(PyClassInitializer::from(instance))
    }

    pub(self) fn serialize_redis_input(&self, value: PyObject) -> PyResult<String> {
        let gil: GILGuard = Python::acquire_gil();
        let py: Python = gil.python();
        let builtins: &PyModule = PyModule::import(py, "builtins")?;
        let serialized: String = builtins.call1("str", (value,))?.extract()?;  
        Ok(serialized)
    }

    pub(self) fn serialize_python_output(&self, value: &str) -> PyResult<PyObject> {
        let gil: GILGuard = Python::acquire_gil();
        let py: Python = gil.python();
        let ast_module: &PyModule = PyModule::import(py, "ast")?;
        let literal_eval: PyObject = ast_module.call1("literal_eval", (value,))?.extract()?;
        Ok(literal_eval)
    }


    // Return a tuple of basic debug information.
    // [0] -> The database number.
    // [1] -> If the connection supports pipelining.
    // [2] -> The URL of the Redis client.
    #[text_signature = "($self)"]
    pub fn info(&self) -> PyResult<(i64, bool, &str)> {
        Ok((
            self.client.get_db(),
            self.connection.supports_pipelining(),
            &self.url
        ))
    }

    // Delete the specified keys. Keys will be ignored if they do not exist.
    //
    // Time Complexity: 
    //  O(N) where N is the number of keys that will be removed. 
    //  When a key to remove holds a value other than a string, the individual complexity for this key 
    //  is O(M) where M is the number of elements in the list, set, sorted set or hash. 
    //  Removing a single key that holds a string value is O(1).
    //
    // Integer Reply:
    //  The amount of keys deleted.
    // 
    // https://redis.io/commands/del
    #[text_signature = "($self, keys)"]
    pub fn delete(&mut self, keys: Vec<&str>) -> PyResult<i8> {
        let integer_reply: i8 = redis::cmd("DEL")
            .arg(keys)
            .query(&mut self.connection)
            .unwrap_or(0);
        Ok(integer_reply)
    }

    // Serialze the value stored at a key in a Redis-specific format and return it to the user.
    // The returned value can be synthesized back into a Redis key using the RESTORE command.
    //
    // Bulk string reply:
    //  The serialized value.
    //
    // Time Complexity:
    //  O(1) to access the key and additional O(N*M) to serialized it, 
    //  where N is the number of Redis objects composing the value and M their average size. 
    //  For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1).
    //
    // https://redis.io/commands/dump
    #[text_signature = "($self, key)"]
    pub fn dump(&mut self, key: &str) -> PyResult<String> {
        let serialized: String = redis::cmd("DUMP")
            .arg(key)
            .query(&mut self.connection)
            .unwrap_or(String::from(""));
        Ok(serialized)
    }

    // Check if a key exists. 
    // 
    // You must pass a Tuple, or List to this for the argument.
    //
    // Integer reply:
    //  The amount of keys that exist in Redis from the passed sequence.
    //
    // Time Complexity: O(1)
    //
    // https://redis.io/commands/exists
    #[text_signature = "($self, keys)"]
    pub fn exists(&mut self, keys: Vec<&str>) -> PyResult<i8> {
        let integer_reply: i8 = redis::cmd("EXISTS")
            .arg(keys)
            .query(&mut self.connection)
            .unwrap_or(0);
        Ok(integer_reply)
    }

    // Set a timeout on a key. After the timeout expires, the key will be deleted.
    // Keys with this behavior are refeered to as volatile keys in Redis.
    // 
    // It is possible to call expire using as argument a key that already has an existing expire set. 
    // In this case the time to live of a key is updated to the new value
    //
    // Integer reply:
    //  1: The timeout was set.
    //  0: The timeout was not set. Input was not an integer, key doesn't exist, etc.
    // 
    // Time Complexity: O(1)
    //
    // https://redis.io/commands/expire
    #[text_signature = "($self, key, seconds)"]
    pub fn expire(&mut self, key: &str, seconds: PyObject) -> PyResult<i8> {
        let integer_reply: i8 = redis::cmd("EXPIRE")
            .arg(&[key, &self.serialize_redis_input(seconds)?[..]])
            .query(&mut self.connection)
            .unwrap_or(0);
        Ok(integer_reply)
    }

    // Set a timeout on a key with a UNIX timestamp. After the timeout expires, the key will be deleted.
    // Keys with this behavior are refeered to as volatile keys in Redis.
    //
    // EXPIREAT has the same effect and semantic as EXPIRE, 
    // but instead of specifying the number of seconds representing the TTL (time to live), 
    // it takes an absolute UNIX timestamp (seconds since January 1, 1970). 
    // A timestamp in the past will delete the key immediately.
    //
    // Integer Reply:
    //  1: The timeout was set.
    //  0: The timeout was not set. Invalid UNIX timestamp, key doesn't exist, etc.
    //
    // Time Complexity: O(1)
    //
    // https://redis.io/commands/expireat
    #[text_signature = "($self, key, timestamp)"]
    pub fn expireat(&mut self, key: &str, timestamp: &str) -> PyResult<i8> {
        let integer_reply = redis::cmd("EXPIREAT")
            .arg(&[key, timestamp])
            .query(&mut self.connection)
            .unwrap_or(0);
        Ok(integer_reply)
    }

    // Return all the keys matching the passed pattern.
    // While the time complexity is O(n), the constant times are quite fast. ~40ms for a 1 million key database.
    //
    // Sequence Reply:
    //  A sequence of the keys matching the passed pattern.
    //
    // Time Complexity:
    //  O(N) with N being the number of keys in the database, 
    //  under the assumption that the key names in the database and the given pattern have limited length.
    //
    // https://redis.io/commands/keys
    #[text_signature = "($self, pattern)"]
    pub fn keys(&mut self, pattern: &str) -> PyResult<Vec<String>> {
        let sequence: Vec<String> = redis::cmd("KEYS")
            .arg(pattern)
            .query(&mut self.connection)
            .unwrap_or(vec![]);
        Ok(sequence)
    }
}

#[pymodule]
fn fastredis(_py: Python, _m: &PyModule) -> PyResult<()> {
    _m.add_class::<RedisClient>()?;
    Ok(())
}