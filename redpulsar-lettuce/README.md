# It's a Lettuce client implementation of RedPulsar library

## Getting started

Creating client is simple as:
```kotlin
// Create Lettuce Pooled Client
val poolConfig = GenericObjectPoolConfig<StatefulRedisConnection<String, String>>()
val client =  LettucePooled(poolConfig) { RedisClient.create("redis://localhost:6379").connect() }
```
Or if you need Pub/Sub support:
```kotlin
// Create Lettuce Pooled Pub/Sub Client
val poolConfig = GenericObjectPoolConfig<StatefulRedisConnection<String, String>>()
val pubSubClient = LettucePubSubPooled(poolConfig) { RedisClient.create("redis://localhost:6379").connectPubSub() }
```
Creating lock:
```kotlin
// Create lock
val lock = LockFactory.createSimpleLock(client)
lock.lock("myResource", 1.seconds)
// do something
lock.unlock("myResource")
```

## Lettuce pooled
This module provides Lettuce pooled client that simplify connection management. 
Instead of creating new connection each time or manage connection pool manually you can use this module.

Available options:
- **[LettucePooled](./src/main/kotlin/me/himadieiev/redpulsar/lettuce/LettucePooled.kt)** - Lettuce pooled client.
- **[LettucePubSubPooled](./src/main/kotlin/me/himadieiev/redpulsar/lettuce/LettucePubSubPooled.kt)** - Lettuce pooled client with Pub/Sub support.
Both clients support synchronous, asynchronous and reactive APIs.
