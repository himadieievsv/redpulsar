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
lock.lock("myResource", Duration.ofSeconds(1))
// do something
lock.unlock("myResource")
```

### Getting started with Java
```java
// Create Jedis Client
var poolConfig = new GenericObjectPoolConfig<Connection>();
var client = new JedisPooled(poolConfig, "localhost", 6381, 100);
```
Creating lock:
```java
// Create lock
var lock = LockFactory.createSimpleLock(client, Duration.ofSeconds(1), 3);
lock.lock("myResource", Duration.ofSeconds(1));
// do something
lock.unlock("myResource");
```
