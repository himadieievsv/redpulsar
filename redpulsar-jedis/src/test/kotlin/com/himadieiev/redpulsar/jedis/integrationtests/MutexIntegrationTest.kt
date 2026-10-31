package com.himadieiev.redpulsar.jedis.integrationtests

import TestTags
import com.himadieiev.redpulsar.core.locks.Mutex
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.jedis.locks.backends.JedisLocksBackend
import getInstances
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import redis.clients.jedis.UnifiedJedis
import java.time.Duration

@Tag(TestTags.INTEGRATIONS)
class MutexIntegrationTest {
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
        val mutex = Mutex(backends)
        val permit = mutex.lock("test", Duration.ofSeconds(10))

        assertTrue(permit)

        val clients = instances.map { it.get("test") }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
    }

    @Test
    fun `release lock`() {
        val mutex = Mutex(backends)
        mutex.lock("test", Duration.ofSeconds(10))
        mutex.unlock("test")

        instances.map { it.get("test") }.forEach { assertNull(it) }
    }

    @Test
    fun `another client can re-acquire lock`() {
        val mutex = Mutex(backends)
        val mutex2 = Mutex(backends = backends, retryCount = 2, retryDelay = Duration.ofMillis(50))

        assertTrue(mutex.lock("test", Duration.ofSeconds(10)))
        assertFalse(mutex2.lock("test", Duration.ofMillis(100)))

        mutex.unlock("test")
        assertTrue(mutex2.lock("test", Duration.ofMillis(100)))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val mutex = Mutex(backends)
        val mutex2 = Mutex(backends = backends, retryCount = 2, retryDelay = Duration.ofMillis(30))

        assertTrue(mutex.lock("test", Duration.ofMillis(200)))
        assertFalse(mutex2.lock("test", Duration.ofMillis(100)))

        runBlocking { delay(200) }
        assertTrue(mutex2.lock("test", Duration.ofMillis(100)))
    }

    @Test
    fun `dont allow to lock again`() {
        val mutex = Mutex(backends)
        val mutex2 = Mutex(backends)

        assertTrue(mutex.lock("test", Duration.ofSeconds(10)))
        assertFalse(mutex2.lock("test", Duration.ofMillis(100)))
    }
}
