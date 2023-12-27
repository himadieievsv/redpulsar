package io.redpulsar.locks.core

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.junit.jupiter.api.Assertions.assertFalse
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
class RedLockIntegrationTest {
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

    private fun getInstances(): List<UnifiedJedis> {
        val poolConfig =
            GenericObjectPoolConfig<Connection>().apply {
                maxTotal = 64
                maxIdle = 8
                minIdle = 2
                setMaxWait(Duration.ofMillis(100))
                blockWhenExhausted = true
            }

        val hostPort1 = getHostPort(1)
        val jedis1 = JedisPooled(poolConfig, hostPort1.host, hostPort1.port, 10)
        val hostPort2 = getHostPort(2)
        val jedis2 = JedisPooled(poolConfig, hostPort2.host, hostPort2.port, 10)
        val hostPort3 = getHostPort(3)
        val jedis3 = JedisPooled(poolConfig, hostPort3.host, hostPort3.port, 10)
        return listOf(jedis1, jedis2, jedis3)
    }

    private fun getHostPort(number: Int) =
        HostAndPort(
            System.getenv("REDIS_HOST$number") ?: "localhost",
            (System.getenv("REDIS_PORT$number")?.toInt() ?: (6380 + number)),
        )
}
