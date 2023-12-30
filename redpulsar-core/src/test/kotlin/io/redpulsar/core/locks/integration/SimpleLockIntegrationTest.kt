package io.redpulsar.core.locks.integration

import TestTags
import getInstances
import io.redpulsar.core.locks.SimpleLock
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import redis.clients.jedis.UnifiedJedis
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.INTEGRATIONS)
class SimpleLockIntegrationTest {
    private lateinit var redis: UnifiedJedis

    @BeforeEach
    fun setUp() {
        redis = getInstances()[0]
        redis.flushAll()
    }

    @Test
    fun `obtain lock`() {
        val simpleLock = SimpleLock(redis)
        val permit = simpleLock.lock("test", 10.seconds)

        assertTrue(permit)

        assertNotNull(redis.get("test"))
    }

    @Test
    fun `release lock`() {
        val simpleLock = SimpleLock(redis)
        simpleLock.lock("test", 10.seconds)
        simpleLock.unlock("test")

        assertNull(redis.get("test"))
    }

    @Test
    fun `another client can re-acquire lock`() {
        val simpleLock = SimpleLock(redis)
        val simpleLock2 = SimpleLock(redis, retryDelay = 50.milliseconds, retryCount = 2)

        assertTrue(simpleLock.lock("test", 10.seconds))
        assertFalse(simpleLock2.lock("test", 10.milliseconds))

        simpleLock.unlock("test")
        assertTrue(simpleLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val simpleLock = SimpleLock(redis)
        val simpleLock2 = SimpleLock(redis, retryDelay = 30.milliseconds, retryCount = 2)

        assertTrue(simpleLock.lock("test", 200.milliseconds))
        assertFalse(simpleLock2.lock("test", 10.milliseconds))

        runBlocking { delay(200) }
        assertTrue(simpleLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `dont allow to lock again`() {
        val simpleLock = SimpleLock(redis)
        val simpleLock2 = SimpleLock(redis)

        assertTrue(simpleLock.lock("test", 10.seconds))
        assertFalse(simpleLock2.lock("test", 10.milliseconds))
    }
}
