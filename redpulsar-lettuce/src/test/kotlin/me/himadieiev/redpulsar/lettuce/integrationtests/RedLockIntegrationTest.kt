package me.himadieiev.redpulsar.lettuce.integrationtests

import TestTags
import getPooledInstances
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import me.himadieiev.redpulsar.core.locks.RedLock
import me.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import me.himadieiev.redpulsar.lettuce.LettucePooled
import me.himadieiev.redpulsar.lettuce.locks.backends.LettuceLocksBackend
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Duration

@Tag(TestTags.INTEGRATIONS)
class RedLockIntegrationTest {
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
        val redLock = RedLock(backends)
        val permit = redLock.lock("test", Duration.ofSeconds(10))

        assertTrue(permit)

        val clients = instances.map { it.sync { redis -> redis.get("test") } }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
    }

    @Test
    fun `release lock`() {
        val redLock = RedLock(backends)
        redLock.lock("test", Duration.ofSeconds(10))
        redLock.unlock("test")

        instances.map { it.sync { redis -> redis.get("test") } }.forEach { assertNull(it) }
    }

    @Test
    fun `another client can re-acquire lock`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends = backends, retryCount = 2, retryDelay = Duration.ofMillis(50))

        assertTrue(redLock.lock("test", Duration.ofSeconds(10)))
        assertFalse(redLock2.lock("test", Duration.ofMillis(10)))

        redLock.unlock("test")
        assertTrue(redLock2.lock("test", Duration.ofMillis(10)))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends = backends, retryCount = 2, retryDelay = Duration.ofMillis(30))

        assertTrue(redLock.lock("test", Duration.ofMillis(200)))
        assertFalse(redLock2.lock("test", Duration.ofMillis(10)))

        runBlocking { delay(200) }
        assertTrue(redLock2.lock("test", Duration.ofMillis(10)))
    }

    @Test
    fun `dont allow to lock again`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends)

        assertTrue(redLock.lock("test", Duration.ofSeconds(10)))
        assertFalse(redLock2.lock("test", Duration.ofMillis(10)))
    }
}
