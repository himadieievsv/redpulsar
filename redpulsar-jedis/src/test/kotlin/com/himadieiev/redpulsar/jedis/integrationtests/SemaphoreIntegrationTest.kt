package com.himadieiev.redpulsar.jedis.integrationtests

import TestTags
import com.himadieiev.redpulsar.core.locks.Semaphore
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.jedis.locks.backends.JedisLocksBackend
import getInstances
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
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
        assertTrue(semaphore.lock("test", Duration.ofSeconds(10)))

        val clients = instances.map { it.smembers("{semaphore:test}:leasers") }
        clients.forEach { client -> assertTrue(client.isNotEmpty()) }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
        clients[0].forEach { leaser ->
            instances.forEach {
                assertTrue(it.exists("{semaphore:test}:$leaser"))
            }
        }

        assertTrue(semaphore.unlock("test"))
        assertTrue(instances.map { it.smembers("{semaphore:test}:leasers") }.none { it.isNotEmpty() })
        clients[0].forEach { leaser ->
            instances.forEach {
                assertFalse(it.exists("{semaphore:test}:$leaser"))
            }
        }
    }

    @Test
    fun `lock sets ttl on every instance`() {
        val ttl = Duration.ofSeconds(2)
        val semaphore = Semaphore(backends, 3)

        assertTrue(semaphore.lock("test", ttl))

        instances.forEach {
            val leasers = it.smembers("{semaphore:test}:leasers")
            val pttls =
                leasers.map { leaser ->
                    it.pttl("{semaphore:test}:$leaser")
                }
            pttls.forEach { pttl -> assertTrue(pttl in 1..ttl.toMillis(), "expected pttl in (0, ${ttl.toMillis()}] but was $pttl") }
        }
    }

    @Test
    fun `non-owner unlock leaves owner lease intact`() {
        val owner = Semaphore(backends, 3)
        val other = Semaphore(backends = backends, maxLeases = 3, retryCount = 1, retryDelay = Duration.ofMillis(10))

        assertTrue(owner.lock("test", Duration.ofSeconds(10)))
        val ownerLeasers = instances.map { it.smembers("{semaphore:test}:leasers") }
        assertTrue(ownerLeasers.all { it.isNotEmpty() })

        // semaphore unlock is not ownership guarded, but it must not remove a foreign lease
        other.unlock("test")

        instances.map { it.smembers("{semaphore:test}:leasers") }.forEachIndexed { index, leasers ->
            assertEquals(ownerLeasers[index], leasers)
        }

        assertTrue(owner.unlock("test"))
        assertTrue(instances.map { it.smembers("{semaphore:test}:leasers") }.none { it.isNotEmpty() })
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
