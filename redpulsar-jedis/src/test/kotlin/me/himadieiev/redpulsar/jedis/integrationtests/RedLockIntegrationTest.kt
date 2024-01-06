package me.himadieiev.redpulsar.jedis.integrationtests

import TestTags
import getInstances
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import me.himadieiev.redpulsar.core.locks.RedLock
import me.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import me.himadieiev.redpulsar.jedis.locks.backends.JedisLocksBackend
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import redis.clients.jedis.UnifiedJedis
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

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
        val permit = redLock.lock("test", 10.seconds)

        assertTrue(permit)

        val clients = instances.map { it.get("test") }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
    }

    @Test
    fun `release lock`() {
        val redLock = RedLock(backends)
        redLock.lock("test", 10.seconds)
        redLock.unlock("test")

        instances.map { it.get("test") }.forEach { assertNull(it) }
    }

    @Test
    fun `another client can re-acquire lock`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends = backends, retryCount = 2, retryDelay = 50.milliseconds)

        assertTrue(redLock.lock("test", 10.seconds))
        assertFalse(redLock2.lock("test", 10.milliseconds))

        redLock.unlock("test")
        assertTrue(redLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends = backends, retryCount = 2, retryDelay = 30.milliseconds)

        assertTrue(redLock.lock("test", 200.milliseconds))
        assertFalse(redLock2.lock("test", 10.milliseconds))

        runBlocking { delay(200) }
        assertTrue(redLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `dont allow to lock again`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends)

        assertTrue(redLock.lock("test", 10.seconds))
        assertFalse(redLock2.lock("test", 10.milliseconds))
    }
}
