# MIT License
#
# Copyright (c) 2020 wellinthatcase
#
# Terms are found in the LICENSE.txt file.

try:
    from .suredis import *
except ImportError: 
    from suredis import * 

from typing import Optional, Dict, Any, List, Union

# Workaround implementation for lack of OOP. 
# Subclasses with Pyo3 prove to be a headache. They're nothing like subclasses inside of Python. 
# Until implementation, if you must pass a key around in an OOP environment, you can initalize one of these classes. 
#
# Some cool pros of this: 
#   - You have partial control of the data the structure has.
#
#   - It removes overhead other libraries give by constatnly returning newly constructed data structures. 
#
#   - So, if you don't want a whole object, only one specific thing, that's all you get.
#
#       - For example, if you only want a value of a string key. Use `RedisClient.get("key_name")`
#
#       - But, if you want to get an entire string key object. Use `StringKey(RedisClient, "key_name")`
#
#   - Constructing a Python class is faster than constructing one from Rust.
#
#       - This is a big reason I've actually pushed a little away from implementing the subclasses from Rust.
#
#       - Some Rust classes can take up to 500ns to finish constructing. 
#
#           - This would add 500ns of additional overhead to operations like GET, which are already a little up there. 
#
#           - This would also differ from the actual functionality of GET. I try to stay as close as possible to 
#             actual Redis functionalities.
#
# Some cool cons of this: 
#   - When you do need a structure, it is inconvient to construct one manually. 
#
# Note, these classes aren't *actually* keys. Instead, they're arbitrary references to a key by name. 
# This is why calling `delete` won't destruct the class. It's an arbitrary object. 
#
# With partial Python implementation comes the chance for more dynamic arguments, 
# but I don't plan to do this until suredis is pretty buff and has great cover over Redis. As of now, at least.

class GenericKey:
    """Key class holding specific attributes and operations.

        You can initalize this class if you need to pass a key around. 

        Attribute assignment is very dynamic. Any kwarg you pass will become an attribute of that class. 
        Your `RedisClient` then the `_name` are the only positionals.
        My hope with this is that you will only pass as much information as you need with the least restriction. 

        Overwriting the `client` or `_name` attributes is forbidden. 
    """

    def __init__(self, client: RedisClient, _name: str, **attributes: Dict[str, Any]) -> None:  
        try:
            client.manual("PING")
        except TypeError: 
            raise ValueError("`client` must be an instance.")

        self._name = _name
        self.client = client 

        for (name, value) in attributes.items():
            if name not in ("client", "_name"): 
                setattr(self, name, value)
    
    def delete(self) -> int:
        """Delete this key."""

        return self.client.delete(self._name)

    def rename(self, new_name: str) -> str: 
        """Rename this key."""

        return self.client.rename(self._name, new_name)

    def exists(self) -> int: 
        """Check if this key still exists."""

        return self.client.exists(self._name)

    def expire(self, seconds: int) -> int:
        """Set an expiration for this key."""

        return self.client.expire(self._name, int(seconds))

    def expireat(self, unix_timestamp: int) -> int:
        """Set an expiration for this key with a UNIX timestamp."""

        return self.client.expireat(self._name, int(unix_timestamp))

    def move(self, db: int) -> int: 
        """Move this key to another database."""

        return self.client.move(self._name, int(db))

    def persist(self) -> int: 
        """Remove the expiration on this key."""

        return self.client.persist(self._name)

    def pexpire(self, milliseconds: int) -> int: 
        """Set an expiration for this key in milliseconds."""

        return self.client.pexpire(self._name, int(milliseconds))

    def pexpireat(self, unix_timestamp_in_ms: int) -> int: 
        """Set an expiration for this key with a UNIX timestamp in milliseconds."""

        return self.client.pexpireat(self._name, int(unix_timestamp_in_ms))

    def pttl(self) -> int: 
        """The time to live for this key in milliseconds. If applicable."""

        return self.client.pttl(self._name)

    def ttl(self) -> int: 
        """The time to live for this key in seconds. If applicable."""

        return self.client.ttl(self._name)

    def keytype(self) -> str: 
        """The type of this key."""

        return self.client.keytype(self._name)

    def unlink(self) -> int: 
        """Unlink this key. Asynchronous deletion."""

        return self.client.unlink(self._name)

    # I thought this was cool. Might remove it later.  
    def __matmul__(self, identifier: int) -> int: 
        """`GenericKey @ 1` == `GenericKey.move(1)`."""
        return self.move(int(identifier))

class StringKey(GenericKey):
    __doc__ = GenericKey.__doc__

    def __init__(self, *args: List[Union[RedisClient, str]], **attributes: Dict[str, Any]) -> None:
        super().__init__(*args, **attributes)

    def append(self, value: str) -> int:
        """Append a substring to this key's value."""

        return self.client.append(self._name, value)

    def bitcount(self, start: int, stop: int) -> int: 
        """Count the number of set bits in a string."""

        return self.client.bitcount(self._name, int(start), int(stop))

    def get(self) -> str: 
        """Get the value of this key."""

        return self.client.get(self._name)

    def sset(self, value, no_overwrite=True) -> str: 
        """Set the value of this key."""

        return self.client.sset(self._name, value, no_overwrite=no_overwrite)

    def getset(self, value: str) -> str: 
        """Set the value of this key and return the old value. Atomic."""

        return self.client.getset(self._name, value)

    def decr(self) -> int: 
        """Decrement the value at this key. If applicable."""

        return self.client.decr(self._name)

    def decrby(self, value: int) -> int: 
        """Decrement the value at this key by `value`. If applicable."""

        return self.client.decrby(self._name, int(value))

    def incr(self) -> int: 
        """Increment the value at this key. If applicable."""

        return self.client.incr(self._name)

    def incrby(self, value: int) -> int: 
        """Increment the value at this key by `value`. If applicable."""

        return self.client.incrby(self._name, int(value))

    def incrbyfloat(self, value: float) -> float: 
        """Increment the value at this key with a float. If the value is already a double-point float."""

        return self.client.incrbyfloat(self._name, float(value))

    def getrange(self, start: int, stop: int) -> str: 
        """Substring the value stored at this key. """

        return self.client.getrange(self._name, int(start), int(stop))

    def setrange(self, value: str, offset: int) -> int: 
        """Overwrites part of the string stored at key, starting at the specified offset."""

        return self.client.setrange(self._name, value, int(offset))

    def strlen(self) -> int: 
        """The length of the string at this key."""

        return self.client.strlen(self._name)

    def __len__(self) -> int: 
        return self.strlen()

class HashKey(GenericKey):
    __doc__ = GenericKey.__doc__

    def __init__(self, *args: List[Union[RedisClient, str]], **attributes: Dict[str, Any]) -> None:
        super().__init__(*args, **attributes)

    def hdel(self, *fields: List[str]) -> int: 
        """Remove the specified fields from the hash stored at this key."""

        return self.client.hdel(self._name, *fields)

    def hexists(self, field: str) -> int: 
        """Check if this hash contains the field."""

        return self.client.hexists(self._name, field)

    def hget(self, field: str) -> str: 
        """Get the value of this field."""

        return self.client.hget(self._name, field)

    def hgetall(self) -> List[str]: 
        """Get a list of fields and their values."""

        return self.client.hgetall(self._name)

    def hincrby(self, field: str, value: int) -> int: 
        """Increments the number stored at field in the hash stored at key by `value`."""

        return self.client.hincrby(self._name, field, int(value))

    def hincrbyfloat(self, field: str, value: float) -> float: 
        """Same as hincrby, just with a double-point float instead of an integer."""

        return self.client.hincrbyfloat(self._name, field, float(value))

    def hkeys(self) -> List[str]:
        """The fields in this hash."""

        return self.client.hkeys(self._name)

    def hlen(self) -> int: 
        """The amount of fields in this hash."""

        return self.client.hlen(self._name)

    def hmget(self, *fields: List[str]) -> List[str]: 
        """The values in the fields at this key."""

        return self.client.hmget(self._name, *fields)

    def hset(self, **fields: Dict[str, Any]) -> int: 
        """Set some fields and values on this hash."""

        return self.client.hset(self._name, **fields, no_overwrite=fields.get('no_overwrite', True))

    def hstrlen(self, field: str) -> int: 
        """The length of the value at the field."""

        return self.client.hstrlen(self._name, field)

    def hvals(self) -> List[str]: 
        """A list of all the values at this hash."""

        return self.client.hvals(self._name)

    def __len__(self) -> int: 
        return self.hlen()

class ListKey(GenericKey): 
    __doc__ = GenericKey.__doc__

    def __init__(self, *args: List[Union[RedisClient, str]], **attributes: Dict[str, Any]) -> None:
        super().__init__(*args, **attributes)

    def rpush(self, *elements: List[Any], no_overwrite: bool=True) -> int: 
        """Push some elements to the right-side of this list."""

        return self.client.rpush(self._name, *elements, no_overwrite=no_overwrite)

    def lpush(self, *elements: List[Any], no_overwrite: bool=True) -> int: 
        """Push some elements to the left-side of this list."""

        return self.client.lpush(self._name, *elements, no_overwrite=no_overwrite)

    def lindex(self, index: int) -> str: 
        """Get the element at `index`. Empty string if there's no element."""

        return self.client.lindex(self._name, int(index))

    def linsert(self, element: Any) -> int: 
        """Inserts `element` in the list stored at key either before or after the reference value pivot."""

        return self.client.linsert(self._name, element)

    def llen(self) -> int: 
        """The length of this list."""

        return self.client.llen(self._name)
    
    def lpop(self) -> str: 
        """Remove and return the last element of this list."""

        return self.client.lpop(self._name)

    def lset(self, index: int, element: Any) -> int: 
        """Set `element` at `index`."""

        return self.client.lset(self._name, int(index), element)

    def lrange(self, start: int, stop: int) -> List[str]: 
        """Get a range of elements from this list."""

        return self.client.lrange(self._name, int(start), int(stop))

    def lrem(self, amt: int, *elements: List[Any]) -> int: 
        """Remove elements from the left side of a list. Best suited for duplicate element removal, it seems."""

        return self.client.lrem(self._name, int(amt), *elements)

    def ltrim(self, start: int, stop: int) -> int: 
        """Trim a list from the left-side."""

        return self.client.ltrim(self._name, int(start), int(stop))

    def rpop(self) -> str: 
        """Remove and return the right-most element on this list."""

        return self.client.rpop(self._name)

    def rpoplpush(self, destination: str) -> str: 
        """Remove the last element in a list, prepend it to another list and return it."""

        return self.client.rpoplpush(self._name, destination)

    def lelements(self) -> List[str]: 
        """Get the elements of this list."""

        return self.client.lelements(self._name)

    def __len__(self) -> int: 
        return self.llen()

    def __iter__(self) -> List[str]: 
        return iter(self.lelements())

class SetKey(GenericKey):
    __doc__ = GenericKey.__doc__

    def __init__(self, *args: List[Union[RedisClient, str]], **attributes: Dict[str, Any]) -> None:
        super().__init__(*args, **attributes)

    def sadd(self, *members: List[Any]) -> int: 
        """Add members to this set."""

        return self.client.sadd(self._name, *members)

    def scard(self) -> int: 
        """The amount of members in this set."""

        return self.client.scard(self._name)

    def sdiff(self, *sets: List[str]) -> List[str]:
        """Calculate the difference between this set and `sets`."""

        return self.client.sdiff(self._name, *sets)

    def sdiffstore(self, destination: str, *sets: List[str]) -> int: 
        """Subtract this set and other sets, and store the result in a key called `destination`."""

        return self.client.sdiffstore(self._name, destination, *sets)

    def sinter(self, *sets: List[str]) -> List[str]: 
        """Intersect this set with others."""

        return self.client.sinter(self._name, *sets)

    def sinterstore(self, destination: str, *sets: List[str]) -> List[str]: 
        """Same as `sinter`, except the result is stored in a key named `destination`."""

        return self.client.sinterstore(self._name, destination, *sets)

    def sismember(self, member: Any) -> int: 
        """Determine if a given value is a member of this set."""

        return self.client.sismember(self._name, member)

    def smembers(self) -> List[str]:
        """Get the members of this set."""

        return self.client.smembers(self._name)

    def smove(self, destination: str, member: Any) -> int: 
        """Move a member from this set to `destination` set."""

        return self.client.smove(self._name, destination, member)

    def __len__(self) -> int: 
        return self.scard()

    def __iter__(self) -> List[str]: 
        return iter(self.smembers())
