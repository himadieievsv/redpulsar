package com.himadieiev.redpulsar.jedis.integrationtests

import TestTags
import com.himadieiev.redpulsar.core.locks.Semaphore
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.jedis.locks.backends.JedisLocksBackend
import getInstances
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import redis.clients.jedis.UnifiedJedis
import java.time.Duration
import kotlin.random.Random.Default.nextInt

@Tag(TestTags.INTEGRATIONS)
class SemaphoreIntegrationTest {
    private lateinit var instances: List<UnifiedJedis>
    private lateinit var backends: List<LocksBackend>

    @BeforeEach
    fun setUp() {
        instances = getInstances()
        instances.forEach { it.flushAll() }
        backends = instances.map { JedisLocksBackend(it) }
    }

    @Test
    fun `obtain lock`() {
        val semaphore = Semaphore(backends, 3)
        val permit = semaphore.lock("test", Duration.ofSeconds(10))

        assertTrue(permit)

        val clients = instances.map { it.smembers("{semaphore:test}:leasers") }
        clients.forEach { client -> assertTrue(client.isNotEmpty()) }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])

        clients[0].forEach { leaser ->
            instances.forEach {
                assertTrue(it.exists("{semaphore:test}:$leaser"))
            }
        }
    }

    @Test
    fun `release lock`() {
        val semaphore = Semaphore(backends, 3)
        semaphore.lock("test", Duration.ofSeconds(10))

        val clients = instances.map { it.smembers("{semaphore:test}:leasers") }
        clients.forEach { client -> assertTrue(client.isNotEmpty()) }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
        clients[0].forEach { leaser ->
            instances.forEach {
                assertTrue(it.exists("{semaphore:test}:$leaser"))
            }
        }

        semaphore.unlock("test")
        assertTrue(instances.map { it.smembers("{semaphore:test}:leasers") }.none { it.isNotEmpty() })
        clients[0].forEach { leaser ->
            instances.forEach {
                assertFalse(it.exists("{semaphore:test}:$leaser"))
            }
        }
    }

    @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
    @ValueSource(ints = [1, 2, 3, 5, 7, 10])
    fun `another client can re-acquire lock`(maxLeases: Int) {
        val semaphores = mutableListOf<Semaphore>()
        (1..maxLeases)
            .forEach {
                semaphores.add(
                    Semaphore(
                        backends = backends,
                        maxLeases = maxLeases,
                        retryCount = 2,
                        retryDelay = Duration.ofMillis(30),
                    ),
                )
            }

        (1..maxLeases)
            .forEach {
                assertTrue(semaphores[it - 1].lock("test", Duration.ofSeconds(10)))
            }
        val semaphore =
            Semaphore(
                backends = backends,
                maxLeases = maxLeases,
                retryCount = 2,
                retryDelay = Duration.ofMillis(15),
            )
        assertFalse(semaphore.lock("test", Duration.ofMillis(100)))

        if (maxLeases > 1) {
            semaphores[nextInt(0, maxLeases - 1)].unlock("test")
        } else {
            semaphores[0].unlock("test")
        }

        assertTrue(semaphore.lock("test", Duration.ofMillis(100)))
    }

    @ParameterizedTest(name = "lock acquired with {0} max leases")
    @ValueSource(ints = [1, 2, 3, 5, 7, 10])
    fun `another client can re-acquire lock due to expiration`(maxLeases: Int) {
        val semaphores = mutableListOf<Semaphore>()
        (1..maxLeases)
            .forEach {
                semaphores.add(
                    Semaphore(
                        backends = backends,
                        maxLeases = maxLeases,
                        retryCount = 2,
                        retryDelay = Duration.ofMillis(30),
                    ),
                )
            }

        (1..maxLeases)
            .forEach {
                assertTrue(semaphores[it - 1].lock("test", Duration.ofSeconds(2)))
            }
        val semaphores2 = mutableListOf<Semaphore>()
        (1..maxLeases)
            .forEach {
                semaphores2.add(
                    Semaphore(
                        backends = backends,
                        maxLeases = it,
                        retryCount = 2,
                        retryDelay = Duration.ofMillis(30),
                    ),
                )
            }
        (1..maxLeases)
            .forEach {
                assertFalse(semaphores2[it - 1].lock("test", Duration.ofMillis(20)))
            }

        runBlocking {
            delay(3000)
        }
        (1..maxLeases)
            .forEach {
                assertTrue(semaphores2[it - 1].lock("test", Duration.ofMillis(20)))
            }
    }
}
