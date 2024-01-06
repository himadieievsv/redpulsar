package me.himadieiev.redpulsar.lettuce.integrationtests

import TestTags
import getPooledInstances
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import me.himadieiev.redpulsar.core.locks.Semaphore
import me.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import me.himadieiev.redpulsar.lettuce.LettucePooled
import me.himadieiev.redpulsar.lettuce.locks.backends.LettuceLocksBackend
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.random.Random.Default.nextInt
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.INTEGRATIONS)
class SemaphoreIntegrationTest {
    private lateinit var instances: List<LettucePooled<String, String>>
    private lateinit var backends: List<LocksBackend>

    @BeforeEach
    fun setUp() {
        instances = getPooledInstances()
        instances.forEach { lettuce -> lettuce.sync { redis -> redis.flushall() } }
        backends = instances.map { LettuceLocksBackend(it) }
    }

    @Test
    fun `obtain lock`() {
        val semaphore = Semaphore(backends, 3)
        val permit = semaphore.lock("test", 10.seconds)

        assertTrue(permit)

        val clients = instances.map { it.sync { redis -> redis.smembers("semaphore:lasers:test") } }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])

        clients[0].forEach { leaser ->
            instances.forEach {
                assertTrue(it.sync { redis -> redis.exists("semaphore:test:$leaser") == 1L })
            }
        }
    }

    @Test
    fun `release lock`() {
        val semaphore = Semaphore(backends, 3)
        semaphore.lock("test", 10.seconds)

        val clients = instances.map { it.sync { redis -> redis.smembers("semaphore:lasers:test") } }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
        clients[0].forEach { leaser ->
            instances.forEach {
                assertTrue(it.sync { redis -> redis.exists("semaphore:test:$leaser") == 1L })
            }
        }

        semaphore.unlock("test")
        assertTrue(
            instances.map { it.sync { redis -> redis.smembers("semaphore:lasers:test") } }
                .none { it.isNotEmpty() },
        )
        clients[0].forEach { leaser ->
            instances.forEach {
                assertFalse(it.sync { redis -> redis.exists("semaphore:test:$leaser") == 1L })
            }
        }
    }

    @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
    @ValueSource(ints = [1, 2, 3, 5, 7, 10])
    fun `another client can re-acquire lock`(maxLeases: Int) {
        val semaphores = mutableListOf<Semaphore>()
        (1..maxLeases + 1)
            .forEach {
                semaphores.add(
                    Semaphore(
                        backends = backends,
                        maxLeases = it,
                        retryCount = 2,
                        retryDelay = 30.milliseconds,
                    ),
                )
            }

        (1..maxLeases)
            .forEach {
                assertTrue(semaphores[it - 1].lock("test", 10.seconds))
            }
        val semaphore =
            Semaphore(
                backends = backends,
                maxLeases = maxLeases,
                retryCount = 2,
                retryDelay = 15.milliseconds,
            )
        assertFalse(semaphore.lock("test", 100.milliseconds))

        if (maxLeases > 1) {
            semaphores[nextInt(0, maxLeases - 1)].unlock("test")
        } else {
            semaphores[0].unlock("test")
        }

        assertTrue(semaphore.lock("test", 100.milliseconds))
    }

    @ParameterizedTest(name = "lock acquired with {0} max leases")
    @ValueSource(ints = [1, 2, 3, 5, 7, 10])
    fun `another client can re-acquire lock due to expiration`(maxLeases: Int) {
        val semaphores = mutableListOf<Semaphore>()
        (1..maxLeases + 1)
            .forEach {
                semaphores.add(
                    Semaphore(
                        backends = backends,
                        maxLeases = it,
                        retryCount = 2,
                        retryDelay = 30.milliseconds,
                    ),
                )
            }

        (1..maxLeases)
            .forEach {
                assertTrue(semaphores[it - 1].lock("test", 1.seconds))
            }
        val semaphores2 = mutableListOf<Semaphore>()
        (1..maxLeases)
            .forEach {
                semaphores2.add(
                    Semaphore(
                        backends = backends,
                        maxLeases = it,
                        retryCount = 2,
                        retryDelay = 30.milliseconds,
                    ),
                )
            }
        (1..maxLeases)
            .forEach {
                assertFalse(semaphores2[it - 1].lock("test", 20.milliseconds))
            }

        runBlocking {
            delay(1000)
        }
        (1..maxLeases)
            .forEach {
                assertTrue(semaphores2[it - 1].lock("test", 20.milliseconds))
            }
    }
}
