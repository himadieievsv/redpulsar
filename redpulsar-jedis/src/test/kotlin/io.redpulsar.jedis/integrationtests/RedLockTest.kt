package io.redpulsar.jedis.integrationtests

import TestTags
import getInstances
import io.redpulsar.core.locks.RedLock
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
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
class RedLockTest {
    private lateinit var instances: List<UnifiedJedis>

    @BeforeEach
    fun setUp() {
        instances = getInstances()
        instances.forEach { it.flushAll() }
    }

    @Test
    fun `obtain lock`() {
        val redLock = RedLock(instances)
        val permit = redLock.lock("test", 10.seconds)

        assertTrue(permit)

        val clients = instances.map { it.get("test") }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
    }

    @Test
    fun `release lock`() {
        val redLock = RedLock(instances)
        redLock.lock("test", 10.seconds)
        redLock.unlock("test")

        instances.map { it.get("test") }.forEach { assertNull(it) }
    }

    @Test
    fun `another client can re-acquire lock`() {
        val redLock = RedLock(instances)
        val redLock2 = RedLock(instances = instances, retryDelay = 50.milliseconds, retryCount = 2)

        assertTrue(redLock.lock("test", 10.seconds))
        assertFalse(redLock2.lock("test", 10.milliseconds))

        redLock.unlock("test")
        assertTrue(redLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val redLock = RedLock(instances)
        val redLock2 = RedLock(instances = instances, retryDelay = 30.milliseconds, retryCount = 2)

        assertTrue(redLock.lock("test", 200.milliseconds))
        assertFalse(redLock2.lock("test", 10.milliseconds))

        runBlocking { delay(200) }
        assertTrue(redLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `dont allow to lock again`() {
        val redLock = RedLock(instances)
        val redLock2 = RedLock(instances)

        assertTrue(redLock.lock("test", 10.seconds))
        assertFalse(redLock2.lock("test", 10.milliseconds))
    }
}
