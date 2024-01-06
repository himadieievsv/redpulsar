# It's a Jedis client implementation of RedPulsar library

## Getting started

Creating client is simple as:
```kotlin
// Create Jedis Client
val poolConfig = GenericObjectPoolConfig<Connection>()
val client =  JedisPooled(poolConfig, "localhost", 6379, 100)
```
Creating lock:
```kotlin
// Create lock
val lock = LockFactory.createSimpleLock(client)
lock.lock("myResource", 1.seconds)
// do something
lock.unlock("myResource")
```
