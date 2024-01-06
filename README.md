# RedPulsar

[![Apache 2.0 licensed](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)

## What is RedPulsar?
RedPulsar provides Distributed Locks with Redis and other utilities for cloud computing or different kinds of distributed systems.
It is minimalistic, lightweight, and easy to use library written in Kotlin and currently can be used with both Jedis or Lettuce clients.

## Features

- **[RedLock](./redpulsar-core/src/main/kotlin/me/himadieiev/redpulsar/core/locks/RedLock.kt)**: Distributed lock mechanism on a resource, that uses consensus of the majority of data storage instances to determine if check obtained successfully.
- **[Semaphore](./redpulsar-core/src/main/kotlin/me/himadieiev/redpulsar/core/locks/Semaphore.kt)**: Distributed semaphore implementation allowing multiple number of lock on a resource. It also uses consensus of the majority of data storage instances to determine if check obtained successfully.
- **[SimpleLock](./redpulsar-core/src/main/kotlin/me/himadieiev/redpulsar/core/locks/SimpleLock.kt)**: Simplified distributed lock mechanism on a resource. Unlike [RedLock](./redpulsar-core/src/main/kotlin/me/himadieiev/redpulsar/core/locks/RedLock.kt) it uses only single data storages instance.
- **[ListeningCountDownLatch](./redpulsar-core/src/main/kotlin/me/himadieiev/redpulsar/core/locks/ListeningCountDownLatch.kt)**: Implementation of distributed Count Down Latch, it uses that uses consensus of the majority of data storage instances ensuring count down consistency. 
[ListeningCountDownLatch](./redpulsar-core/src/main/kotlin/me/himadieiev/redpulsar/core/locks/ListeningCountDownLatch.kt) utilized [Redis Pub/Sub](https://redis.io/topics/pubsub) mechanism to notify waiting workloads about count reaching zero.

## Supporting data storages
Currently, RedPulsar supports Redis as a data storage. It can be used with both Jedis or Lettuce clients.
- See [redpulsar-jedis](./redpulsar-jedis/README.md) module for details.
- See [redpulsar-lettuce](./redpulsar-lettuce/README.md) module for details.

## Getting started 
- See [redpulsar-jedis](./redpulsar-jedis/README.md#getting-started) for getting started with Jedis.
- See [redpulsar-lettuce](./redpulsar-lettuce/README.md#getting-started) for getting started with Lettuce.

### Development
To build RedPulsar locally, you need to have JDK 11+ installed.
To build or test RedPulsar, run the following command:
```bash
git clone git@github.com:himadieievsv/redpulsar.git
cd redpulsar
# Run all tests
docker-compose up -d
./gradlew test 
# Run only unit tests
./gradlew test -DexcludeTags="integration"
# Build
./gradlew build -x test
# Code formatting
./gradlew ktlintFormat
```

## Further development 

### Extending RedPulsar to use other data stores 
Currently, all features are implemented with Redis. However, it is possible to extend RedPulsar to use other distributed data stores like AWS DynamoDB / Casandra / ScyllaDB etc. Even it could be implemented with RDBMS like MySQL or PostgreSQL.
RedPulsar project have an abstraction level for data storage called [Backend](./redpulsar-core/src/main/kotlin/me/himadieiev/redpulsar/core/locks/abstracts/Backend.kt). See package [me.himadieiev.redpulsar.core.locks.abstracts.backends](./redpulsar-core/src/main/kotlin/me/himadieiev/redpulsar/core/locks/abstracts/backends) for details what particular operation should be implemented.
New data storage should use a new module and implement same abstractions as current Redis implementations. 

### Contributing
Contributions are welcome! Please feel free to submit a Pull Request from project fork .

## Furthers plans
- [ ] Add ListenerLock using Pub/Sub mechanism instead of polling.
- [ ] Add FairLock implementation. It supposed to be a lock that grants lock to the longest waiting thread.
- [ ] Leader election mechanisms.
- [ ] Service discovery service.
- etc.

