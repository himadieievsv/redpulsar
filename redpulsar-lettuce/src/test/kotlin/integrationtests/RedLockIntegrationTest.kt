package integrationtests

import TestTags
import getInstances
import io.redpulsar.core.locks.RedLock
import io.redpulsar.core.locks.abstracts.backends.LocksBackend
import io.redpulsar.lettuce.LettucePooled
import io.redpulsar.lettuce.locks.LettuceLocksBackend
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.INTEGRATIONS)
class RedLockIntegrationTest {
    private lateinit var instances: List<LettucePooled<String, String>>
    private lateinit var backends: List<LocksBackend>

    @BeforeEach
    fun setUp() {
        instances = getInstances()
        instances.forEach { lettuce -> lettuce.sync { redis -> redis.flushall() } }
        backends = instances.map { LettuceLocksBackend(it) }
    }

    @Test
    fun `obtain lock`() {
        val redLock = RedLock(backends)
        val permit = redLock.lock("test", 10.seconds)

        assertTrue(permit)

        val clients = instances.map { it.sync { redis -> redis.get("test") } }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
    }

    @Test
    fun `release lock`() {
        val redLock = RedLock(backends)
        redLock.lock("test", 10.seconds)
        redLock.unlock("test")

        instances.map { it.sync { redis -> redis.get("test") } }.forEach { assertNull(it) }
    }

    @Test
    fun `another client can re-acquire lock`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends = backends, retryDelay = 50.milliseconds, retryCount = 2)

        assertTrue(redLock.lock("test", 10.seconds))
        assertFalse(redLock2.lock("test", 10.milliseconds))

        redLock.unlock("test")
        assertTrue(redLock2.lock("test", 10.milliseconds))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val redLock = RedLock(backends)
        val redLock2 = RedLock(backends = backends, retryDelay = 30.milliseconds, retryCount = 2)

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
