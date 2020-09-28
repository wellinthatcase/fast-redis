// MIT License
//
// Copyright (c) 2020 wellinthatcase
//
// Terms are found in the LICENSE.txt file.
#![allow(dead_code)]

use redis::*;
use std::borrow::Cow;
use std::collections::HashMap;

use pyo3::{
    Python, types::*,
    PyTraverseError, PyVisit,
    PyNativeType, prelude::*, exceptions,
    class::basic::PyObjectProtocol, PyGCProtocol
};

#[inline]
fn route_command<Args, ReturnType>(inst: &mut RedisClient, cmd: &str, args: Option<Args>) -> PyResult<ReturnType>
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
#[inline]
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
#[inline]
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

mod ops; 

/// A speedy & simplistic library at runtime for an incredibly straightforward Redis interface.
#[pymodule]
fn suredis(_py: Python, module: &PyModule) -> PyResult<()> {
    module.add_class::<RedisClient>()?;
    Ok(())
}