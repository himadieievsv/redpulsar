# RedPulsar

[![Release](https://img.shields.io/github/release/himadieievsv/redpulsar.svg?sort=semver&color=blue)](https://github.com/himadieievsv/redpulsar/releases/latest)
![Unit Tests](https://github.com/himadieievsv/redpulsar/actions/workflows/unit-tests.yml/badge.svg)
![Integration Tests](https://github.com/himadieievsv/redpulsar/actions/workflows/integration-tests.yml/badge.svg)
[![Apache 2.0 licensed](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![codecov](https://codecov.io/gh/himadieievsv/redpulsar/graph/badge.svg?token=ME7ESPRABQ)](https://codecov.io/gh/himadieievsv/redpulsar)
[![Maven Central](https://img.shields.io/maven-central/v/com.himadieiev/redpulsar-core.svg)](https://search.maven.org/artifact/com.himadieiev/redpulsar-core)


## What is RedPulsar?
RedPulsar provides Distributed Locks with Redis and other utilities for cloud computing or different kinds of distributed systems.
It is minimalistic, lightweight, and easy to use library written in Kotlin and currently can be used with both Jedis or Lettuce clients.

## Features

- **[Mutex](./redpulsar-core/src/main/kotlin/com/himadieiev/redpulsar/core/locks/Mutex.kt)**: Distributed lock mechanism on a resource, that uses consensus of the majority of data storage nodes to determine if check obtained successfully.
- **[Semaphore](./redpulsar-core/src/main/kotlin/com/himadieiev/redpulsar/core/locks/Semaphore.kt)**: Distributed semaphore implementation allowing multiple number of lock on a resource. It also uses consensus of the majority of data storage nodes to determine if check obtained successfully.
- **[SimplifiedMutex](./redpulsar-core/src/main/kotlin/com/himadieiev/redpulsar/core/locks/SimplifiedMutex.kt)**: Simplified distributed lock mechanism on a resource. Unlike [Mutex](./redpulsar-core/src/main/kotlin/com/himadieiev/redpulsar/core/locks/Mutex.kt) it uses single data storage node.
- **[ListeningCountDownLatch](./redpulsar-core/src/main/kotlin/com/himadieiev/redpulsar/core/locks/ListeningCountDownLatch.kt)**: Implementation of distributed Count Down Latch, it uses that uses consensus of the majority of data storage instances ensuring count down consistency.
  [ListeningCountDownLatch](./redpulsar-core/src/main/kotlin/com/himadieiev/redpulsar/core/locks/ListeningCountDownLatch.kt) utilized [Redis Pub/Sub](https://redis.io/topics/pubsub) mechanism to notify waiting workloads about count reaching zero.

## Supporting data storages
Currently, RedPulsar supports Redis as a data storage. It can be used with both Jedis or Lettuce clients.
- See [redpulsar-jedis](./redpulsar-jedis/README.md) module for details.
- See [redpulsar-lettuce](./redpulsar-lettuce/README.md) module for details. This module also provides convenient [lettuce pooled](./redpulsar-lettuce/README.md#lettuce-pooled) clients.

## Java compatibility
Minimal required Java version is 11.
RedPulsar project is written in Kotlin, but can be easily used in Java projects too.

## Getting started

Gradle dependency:
```kotlin
implementation("com.himadieiev:redpulsar-jedis:1.2.0")
// OR
implementation("com.himadieiev:redpulsar-lettuce:1.2.0")
```

- See [Getting started with redpulsar-jedis](./redpulsar-jedis/README.md#getting-started) for getting started with Jedis client.
- See [Getting started with redpulsar-lettuce](./redpulsar-lettuce/README.md#getting-started) for getting started with Lettuce client.

### Development
To build RedPulsar locally, you need to have JDK 11+ installed.
To build or test RedPulsar, run the following command:
```bash
git clone git@github.com:himadieievsv/redpulsar.git
cd redpulsar

# Code formatting
./gradlew ktlintFormat

# Run all tests
docker-compose up -d
./gradlew test 

# Run only unit tests
./gradlew test -DexcludeTags="integration"

# Build
./gradlew build -x test

# Publish to local maven repository
./gradlew publishToMavenLocal \
  -Psigning.secretKeyRingFile=... \
  -Psigning.password=... \
  -Psigning.keyId=...
```

## Further development

### Extending RedPulsar to use other data stores
Currently, all features are implemented with Redis. However, it is possible to extend RedPulsar to use other distributed data stores like AWS DynamoDB / Casandra / ScyllaDB etc. Even it could be implemented with RDBMS like MySQL or PostgreSQL.
RedPulsar project have an abstraction level for data storage called [Backend](./redpulsar-core/src/main/kotlin/com/himadieiev/redpulsar/core/locks/abstracts/Backend.kt). See package [com.himadieiev.redpulsar.core.locks.abstracts.backends](./redpulsar-core/src/main/kotlin/com/himadieiev/redpulsar/core/locks/abstracts/backends) for details what particular operation should be implemented.
New data storage should use a new module and implement same abstractions as current Redis implementations.

### Contributing
Contributions are welcome! Please make sure to create Issue first before working on improvements o new features, feel free to submit a Pull Request from a project fork.

## Furthers plans
- [ ] Add ListenerLock using Pub/Sub mechanism instead of polling.
- [ ] Add FairLock implementation. It supposed to be a lock that grants lock to the longest waiting thread.
- [ ] Leader election mechanisms.
- [ ] Service discovery service.
- etc.
