package integrationtests

import TestTags
import getInstances
import io.redpulsar.core.locks.SimpleLock
import io.redpulsar.core.locks.abstracts.LocksBackend
import io.redpulsar.lettuce.LettucePooled
import io.redpulsar.lettuce.locks.LettuceLocksBackend
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.INTEGRATIONS)
class SimpleLockIntegrationTest {
    private lateinit var redis: LettucePooled<String, String>
    private lateinit var backend: LocksBackend

    @BeforeEach
    fun setUp() {
        redis = getInstances()[0]
        redis.sync { redis -> redis.flushall() }
        backend = LettuceLocksBackend(redis)
    }

    @Test
    fun `obtain lock`() {
        val simpleLock = SimpleLock(backend)
        val permit = simpleLock.lock("test", 10.seconds)

        assertTrue(permit)

        assertNotNull(redis.sync { redis -> redis.get("test") })
    }

    @Test
    fun `release lock`() {
        val simpleLock = SimpleLock(backend)
        simpleLock.lock("test", 10.seconds)
        simpleLock.unlock("test")

        assertNull(redis.sync { redis -> redis.get("test") })
    }

    @Test
    fun `another client can re-acquire lock`() {
        val simpleLock = SimpleLock(backend)
        val simpleLock2 = SimpleLock(backend, retryDelay = 50.milliseconds, retryCount = 2)

        assertTrue(simpleLock.lock("test", 10.seconds))
        assertFalse(simpleLock2.lock("test", 10.milliseconds))

        simpleLock.unlock("test")
        assertTrue(simpleLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val simpleLock = SimpleLock(backend)
        val simpleLock2 = SimpleLock(backend, retryDelay = 30.milliseconds, retryCount = 2)

        assertTrue(simpleLock.lock("test", 200.milliseconds))
        assertFalse(simpleLock2.lock("test", 10.milliseconds))

        runBlocking { delay(200) }
        assertTrue(simpleLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `dont allow to lock again`() {
        val simpleLock = SimpleLock(backend)
        val simpleLock2 = SimpleLock(backend)

        assertTrue(simpleLock.lock("test", 10.seconds))
        assertFalse(simpleLock2.lock("test", 10.milliseconds))
    }
}
