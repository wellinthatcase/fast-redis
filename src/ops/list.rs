//! Implementation of list operations for the client. 
use crate::*; 

#[pymethods]
impl RedisClient {
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
}