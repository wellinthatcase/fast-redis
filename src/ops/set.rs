//! Implementation of set operations for the client. 
use crate::*; 

#[pymethods]
impl RedisClient {
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
}