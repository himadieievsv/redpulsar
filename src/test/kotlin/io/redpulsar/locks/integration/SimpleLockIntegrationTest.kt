package io.redpulsar.locks.integration

import io.redpulsar.locks.core.SimpleLock
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import redis.clients.jedis.Connection
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisPooled
import redis.clients.jedis.UnifiedJedis
import java.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Tag(TestTags.INTEGRATIONS)
class SimpleLockIntegrationTest {
    private lateinit var redis: UnifiedJedis

    @BeforeEach
    fun setUp() {
        redis = getInstance()
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

    private fun getInstance(): UnifiedJedis {
        val poolConfig =
            GenericObjectPoolConfig<Connection>().apply {
                maxTotal = 64
                maxIdle = 8
                minIdle = 2
                setMaxWait(Duration.ofMillis(100))
                blockWhenExhausted = true
            }

        val hostPort1 = getHostPort()
        return JedisPooled(poolConfig, hostPort1.host, hostPort1.port, 10)
    }

    private fun getHostPort() =
        HostAndPort(
            System.getenv("REDIS_HOST1") ?: "localhost",
            (System.getenv("REDIS_PORT1")?.toInt() ?: 6381),
        )
}
