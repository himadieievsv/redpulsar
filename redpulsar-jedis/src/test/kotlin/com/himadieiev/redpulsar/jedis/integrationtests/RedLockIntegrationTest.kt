package com.himadieiev.redpulsar.jedis.integrationtests

import TestTags
import com.himadieiev.redpulsar.core.locks.RedLock
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
class RedLockIntegrationTest {
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
        val redLock = RedLock(backends)
        val permit = redLock.lock("test", Duration.ofSeconds(10))

        assertTrue(permit)

        val clients = instances.map { it.get("test") }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
    }

    @Test
    fun `release lock`() {
        val redLock = RedLock(backends)
        redLock.lock("test", Duration.ofSeconds(10))
        redLock.unlock("test")

        instances.map { it.get("test") }.forEach { assertNull(it) }
    }

    @Test
    fun `another client can re-acquire lock`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends = backends, retryCount = 2, retryDelay = Duration.ofMillis(50))

        assertTrue(redLock.lock("test", Duration.ofSeconds(10)))
        assertFalse(redLock2.lock("test", Duration.ofMillis(100)))

        redLock.unlock("test")
        assertTrue(redLock2.lock("test", Duration.ofMillis(100)))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends = backends, retryCount = 2, retryDelay = Duration.ofMillis(30))

        assertTrue(redLock.lock("test", Duration.ofMillis(200)))
        assertFalse(redLock2.lock("test", Duration.ofMillis(100)))

        runBlocking { delay(200) }
        assertTrue(redLock2.lock("test", Duration.ofMillis(100)))
    }

    @Test
    fun `dont allow to lock again`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends)

        assertTrue(redLock.lock("test", Duration.ofSeconds(10)))
        assertFalse(redLock2.lock("test", Duration.ofMillis(100)))
    }
}
