use redis::*;
use pyo3::{
    prelude::*,
    PyGCProtocol, PyTraverseError, PyVisit, PyNativeType
};

#[pyclass]
struct HyperRedisClient {
    client: redis::Client,             // The internal Redis client.
    connection: redis::Connection,     // The internal Redis connection.
    url: String,                       // The url used to establish the Redis client.
    o: Option<PyObject>                // Used to support the CPython Garbage Collection protocol.
}

unsafe impl PyNativeType for HyperRedisClient {}

#[pyproto]
impl PyGCProtocol for HyperRedisClient {
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
impl HyperRedisClient {
    #[new]
    fn __new__(url: String) -> PyResult<PyClassInitializer<Self>> {
        let client: redis::Client = redis::Client::open(url.as_ref())
            .expect("could not establish a Redis client.");
        let connection: redis::Connection = client.get_connection()
            .expect("could not establish a connection with the Redis client. Is the server running?");

        let instance: HyperRedisClient = HyperRedisClient {
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
    pub fn info(&self) -> PyResult<(String, String, String)> {
        Ok((
            format!("Data Num#: {}", &self.client.get_db()),
            format!("Pipelined: {}", &self.connection.supports_pipelining()),
            format!("Redis URL: {}", &self.url)
        ))
    }

    // Delete the specified keys. Keys will be ignored if they do not exist.
    //
    // Integer reply:
    //  The amount of keys deleted.
    // 
    // Time Complexity: O(n)
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
    // Time Complexity: O(1)
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
    // Time Complexity: O(n)
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
fn hyperredis(_py: Python, _m: &PyModule) -> PyResult<()> {
    _m.add_class::<HyperRedisClient>()?;
    Ok(())
}