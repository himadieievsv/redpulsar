# It's a Lettuce client implementation of RedPulsar library

## Usage

TBD

## Lettuce pooled
This module provides Lettuce pooled client that simplify connection management. 
Instead of creating new connection each time or manage connection pool manually you can use this module.

Available options:
- **[LettucePooled](./src/main/kotlin/me/himadieiev/redpulsar/lettuce/LettucePooled.kt)]** - Lettuce pooled client.
- **[LettucePubSubPooled](./src/main/kotlin/me/himadieiev/redpulsar/lettuce/LettucePubSubPooled.kt)** - Lettuce pooled client with Pub/Sub support.
Both clients support synchronous, asynchronous and reactive APIs.
