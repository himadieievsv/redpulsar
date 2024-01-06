package me.himadieiev.redpulsar.jedis.integrationtests

import TestTags
import getInstances
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import me.himadieiev.redpulsar.core.locks.SimpleLock
import me.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import me.himadieiev.redpulsar.jedis.locks.backends.JedisLocksBackend
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import redis.clients.jedis.UnifiedJedis
import java.time.Duration

@Tag(TestTags.INTEGRATIONS)
class SimpleLockIntegrationTest {
    private lateinit var redis: UnifiedJedis
    private lateinit var backend: LocksBackend

    @BeforeEach
    fun setUp() {
        redis = getInstances()[0]
        redis.flushAll()
        backend = JedisLocksBackend(redis)
    }

    @Test
    fun `obtain lock`() {
        val simpleLock = SimpleLock(backend)
        val permit = simpleLock.lock("test", Duration.ofSeconds(10))

        assertTrue(permit)

        assertNotNull(redis.get("test"))
    }

    @Test
    fun `release lock`() {
        val simpleLock = SimpleLock(backend)
        simpleLock.lock("test", Duration.ofSeconds(10))
        simpleLock.unlock("test")

        assertNull(redis.get("test"))
    }

    @Test
    fun `another client can re-acquire lock`() {
        val simpleLock = SimpleLock(backend)
        val simpleLock2 = SimpleLock(backend, retryDelay = Duration.ofMillis(50), retryCount = 2)

        assertTrue(simpleLock.lock("test", Duration.ofSeconds(10)))
        assertFalse(simpleLock2.lock("test", Duration.ofMillis(10)))

        simpleLock.unlock("test")
        assertTrue(simpleLock2.lock("test", Duration.ofMillis(10)))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val simpleLock = SimpleLock(backend)
        val simpleLock2 = SimpleLock(backend, retryDelay = Duration.ofMillis(30), retryCount = 2)

        assertTrue(simpleLock.lock("test", Duration.ofMillis(200)))
        assertFalse(simpleLock2.lock("test", Duration.ofMillis(10)))

        runBlocking { delay(200) }
        assertTrue(simpleLock2.lock("test", Duration.ofMillis(10)))
    }

    @Test
    fun `dont allow to lock again`() {
        val simpleLock = SimpleLock(backend)
        val simpleLock2 = SimpleLock(backend)

        assertTrue(simpleLock.lock("test", Duration.ofSeconds(10)))
        assertFalse(simpleLock2.lock("test", Duration.ofMillis(10)))
    }
}
