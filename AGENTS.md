# AGENTS.md — RedPulsar

> Distributed locks (Redis) library in Kotlin. Java 11+, Gradle Kotlin DSL multi-module. Usable from Kotlin and Java.

## Modules

| Module | Path | Purpose | Key deps |
|---|---|---|---|
| `redpulsar-core` | `redpulsar-core/` | All lock logic, abstractions, executors, utils. Redis-client agnostic. | `kotlin-logging`, `kotlinx-coroutines-core`, JUnit5 + MockK (test) |
| `redpulsar-jedis` | `redpulsar-jedis/` | Jedis binding: `LockFactory` + `JedisLocksBackend`, `JedisCountDownLatchBackend` | `redis.clients:jedis`, `commons-pool2`, `:redpulsar-core` (versions in `redpulsar-jedis/build.gradle.kts`) |
| `redpulsar-lettuce` | `redpulsar-lettuce/` | Lettuce binding: `LockFactory` + `LettuceLocksBackend`, `LettuceCountDownLatchBackend`, pooled clients | `io.lettuce:lettuce-core`, `commons-pool2`, `:redpulsar-core` (versions in `redpulsar-lettuce/build.gradle.kts`) |

Root: `settings.gradle.kts` includes all three. Group and project version are defined in root `build.gradle.kts` (`allprojects { group, version }`) — check there for current values, do not hardcode.

## Primitives (core, `com.himadieiev.redpulsar.core.locks`)

- `Mutex` — RedLock quorum lock over N instances/clusters. `ttl.toMillis() > 3 * backendSize()` is enforced via `require`.
- `Semaphore` — quorum semaphore, `maxLeases` permits.
- `SimplifiedMutex` — single-node lock (no quorum). Use for single Redis.
- `ListeningCountDownLatch` — quorum latch + Redis Pub/Sub notification on count → 0.
- API: `locks/api/Lock.kt` (`lock(resource, ttl=10s): Boolean`, `unlock(resource): Boolean`), `locks/api/CountDownLatch.kt`.

## Key abstractions — extend here, not around

- `locks/abstracts/AbstractLock.kt` — base; per-instance `lockInstance`/`unlockInstance`, per-lock `clientId = UUID`.
- `locks/abstracts/AbstractMultiInstanceLock.kt` — RedLock via `backends.executeWithRetry(...)` (`locks/excecutors/MultiInstanceExecutor.kt`, `WaitStrategy.ALL`). `require(backends.isNotEmpty())`.
- `locks/abstracts/Backend.kt` — marker base with `convertToString`.
- `locks/abstracts/backends/LocksBackend.kt`, `CountDownLatchBackend.kt` — **the port for new datastores** (DynamoDB/Cassandra/RDBMS). New store = new module implementing these, mirroring `redpulsar-jedis`/`redpulsar-lettuce` structure.
- `utils/Failsafe.kt` (`failsafe`), `WithRetry.kt`, `WithTimeoutInThread.kt`, `Strings.kt` — shared helpers.
- Client entry points: `jedis/locks/LockFactory.kt`, `lettuce/locks/LockFactory.kt` — all `@JvmStatic` for Java interop. Lettuce also has `LettucePooled`, `LettucePubSubPooled` (Pub/Sub required for latch), `abstracts/LettuceUnified.kt`.

## Toolchain

- JDK 11 minimum (toolchain enforced in root build: `kotlin { jvmToolchain(11) }`, `java.toolchain.languageVersion 11`; exact version in `.java-version`). CI integration matrix: see `.github/workflows/integration-tests.yml` `matrix.version`.
- Kotlin, Gradle wrapper, ktlint, kover plugin versions: see root `build.gradle.kts` `plugins { }` block and `gradle/wrapper/gradle-wrapper.properties` (`distributionUrl`). Use `./gradlew`, never system `gradle`.
- Shared test/library versions (kotlin-logging, coroutines, JUnit BOM, MockK): see root `build.gradle.kts` `subprojects.dependencies`. Module-specific versions: see each module's `build.gradle.kts`.
- Redis for integration tests: `docker-compose.yml` → 3 `himadieievsv/redis-cluster` nodes (image tag in that file) on `7010-7012`, `7020-7022`, `7030-7032` (`INITIAL_PORT` 7010/7020/7030). Test helpers: `redpulsar-jedis/src/test/kotlin/TestCommons.kt`, `redpulsar-lettuce/src/test/kotlin/TestCommons.kt`.

## Commands (run from repo root)

```bash
./gradlew ktlintFormat                    # format (do before commit)
./gradlew ktlintCheck                     # CI gate — must pass

./gradlew test -DexcludeTags="integration"  # unit only, no Redis needed

docker-compose up -d
./gradlew test -DexcludeTags="unit"       # integration only, needs Redis clusters up

./gradlew test                            # everything (needs Redis)
./gradlew :redpulsar-core:test :redpulsar-jedis:test :redpulsar-lettuce:test -DexcludeTags="integration"

./gradlew build -x test
```

Notes:
- Tag filtering is wired in root `build.gradle.kts` (`tasks.test { useJUnitPlatform { excludeTags(...) } }`). Property name is `excludeTags` (plural, comma-separated).
- Unit tag constant: `redpulsar-core/src/test/kotlin/TestTags.kt` (`UNIT = "unit"`). Integration tests use tag `"integration"` and live under `*/integrationtests/*IntegrationTest.kt`.
- JUnit XML per-test-case output is enabled; don't disable.

## Code conventions

- Kotlin coding conventions + ktlint. Run `ktlintFormat` — CI fails otherwise.
- KDoc on all public classes/methods (CONTRIBUTING.md requirement). Keep `@param`/`@return` style already in codebase.
- Constructor validation with `require(...)` (see `Mutex`: positive `retryDelay`/`retryCount`, ttl vs clock drift; `AbstractMultiInstanceLock`: non-empty backends).
- Backend methods **must not throw** — wrap in `failsafe { }`. Exception: methods returning `Flow` — wrap the *collect* site, not the construction (see `Backend.kt` KDoc).
- Logging via `mu.KotlinLogging` (`KotlinLogging.logger {}`), never `println`.
- Coroutines: quorum fan-out uses `CoroutineScope(CoroutineName("redLock") + Dispatchers.IO)` + `runBlocking` at `lock()` boundary. Don't switch to suspend API without updating both bindings + Java callers.
- Java interop is first-class: keep `@JvmStatic` on factories, avoid Kotlin-only signatures in public API (defaults are fine — they generate overloads via companions used from Java with explicit args).
- Typo alert: package `locks.excecutors` is misspelled in-repo. **Do not rename** without a repo-wide migration; reference it as-is.

## Tests

- JUnit 5 + MockK for unit tests (versions in root `build.gradle.kts` `subprojects.dependencies`; mock `LocksBackend`/`CountDownLatchBackend`, verify `setLock`/`removeLock` counts, `@ParameterizedTest` for TTLs — follow `MutexTest.kt`).
- New feature/bugfix **must** include tests (unit + integration where Redis behavior matters).
- Name integration tests `*IntegrationTest.kt`, tag `"integration"`, reuse `TestCommons.kt` ports — never hardcode other ports.
- Run unit tests without Docker; always start `docker-compose` before integration tests.

## CI (`.github/workflows/`)

- `unit-tests.yml` (every push): `ktlintCheck` + unit tests (Java version in that file's `setup-java` step).
- `integration-tests.yml` (main push/PR, Java matrix in that file's `matrix.version`): spins up the 3 Redis services with health checks, runs `-DexcludeTags="unit"`.
- `build-on-tag.yml`, `codecov.yml`. CODEOWNERS: `* @himadieievsv`.

## Common pitfalls

- `Mutex.lock` with small TTL throws `IllegalArgumentException` by design (`ttl > 3 * N ms`) — clock-drift guard, not a bug.
- `Mutex`/`Semaphore`/latch need a backend **per** Redis instance/cluster; `SimplifiedMutex` takes exactly one. Don't pass a list of one to `Mutex` expecting single-node semantics.
- `ListeningCountDownLatch` needs Pub/Sub-capable client (Lettuce: `LettucePubSubPooled` / `connectPubSub()`), not plain `LettucePooled`.
- Quorum failures clean up partial locks via `cleanUp = unlockInstance` — preserve this in executor changes.
- Publishing: `publishToMavenLocal` requires `signing.*` + `ossrh*` props (`gradle.properties` placeholders only — never commit real keys).

## Contributing flow

Create an Issue first, then fork → branch → commit → PR (per `CONTRIBUTING.md` + Code of Conduct). Keep PRs scoped; include tests + KDoc.