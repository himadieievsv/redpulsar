# It's a Lettuce client implementation of RedPulsar library

## Getting started

Gradle dependency:
```kotlin
implementation("com.himadieiev:redpulsar-core:0.1.1")
implementation("com.himadieiev:redpulsar-lettuce:0.1.1")
implementation("io.lettuce:lettuce-core:6.3.0.RELEASE")
```

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
lock.lock("myResource", Duration.ofSeconds(1))
// do something
lock.unlock("myResource")
```

### Getting started with Java
```java
// Create Lettuce Pooled Client
var client = RedisClient.create("redis://localhost:6379");
var poolConfig = new GenericObjectPoolConfig<StatefulRedisConnection<String, String>>();
var lettucePooled = new LettucePooled<>(poolConfig, client::connect);
```
Or if you need Pub/Sub support:
```java
// Create Lettuce Pooled Pub/Sub Client
var client = RedisClient.create("redis://localhost:6379");
var poolConfig = new GenericObjectPoolConfig<StatefulRedisPubSubConnection<String, String>>();
var lettucePubSubPooled = new LettucePubSubPooled<>(poolConfig, client::connectPubSub);
```
Creating lock:
```java
// Create lock
var lock = LockFactory.createSimpleLock(lettucePooled, Duration.ofSeconds(1), 3);
lock.lock("myResource", Duration.ofSeconds(1));
// do something
lock.unlock("myResource");
```

## Lettuce pooled
This module provides Lettuce pooled client that simplify connection management. 
Instead of creating new connection each time or manage connection pool manually you can use this module.

Available options:
- **[LettucePooled](./src/main/kotlin/me/himadieiev/redpulsar/lettuce/LettucePooled.kt)** - Lettuce pooled client.
- **[LettucePubSubPooled](./src/main/kotlin/me/himadieiev/redpulsar/lettuce/LettucePubSubPooled.kt)** - Lettuce pooled client with Pub/Sub support.

Both clients support synchronous, asynchronous and reactive APIs.
