package com.himadieiev.redpulsar.lettuce.integrationtests

import TestTags
import com.himadieiev.redpulsar.core.locks.Mutex
import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import com.himadieiev.redpulsar.lettuce.abstracts.LettuceUnified
import com.himadieiev.redpulsar.lettuce.locks.backends.LettuceLocksBackend
import getPooledInstances
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Duration

@Tag(TestTags.INTEGRATIONS)
class MutexIntegrationTest {
    private lateinit var instances: List<LettuceUnified<String, String>>
    private lateinit var backends: List<LocksBackend>

    @BeforeEach
    fun setUp() {
        instances = getPooledInstances()
        instances.forEach { lettuce -> lettuce.sync { redis -> redis.flushall() } }
        backends = instances.map { LettuceLocksBackend(it) }
    }

    @Test
    fun `obtain lock`() {
        val mutex = Mutex(backends)
        val permit = mutex.lock("test", Duration.ofSeconds(10))

        assertTrue(permit)

        val clients = instances.map { it.sync { redis -> redis.get("test") } }
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])
    }

    @Test
    fun `release lock`() {
        val mutex = Mutex(backends)
        assertTrue(mutex.lock("test", Duration.ofSeconds(10)))
        assertTrue(mutex.unlock("test"))

        instances.map { it.sync { redis -> redis.get("test") } }.forEach { assertNull(it) }
    }

    @Test
    fun `lock sets ttl on every instance`() {
        val ttl = Duration.ofSeconds(2)
        val mutex = Mutex(backends)

        assertTrue(mutex.lock("test", ttl))

        instances.forEach {
            val pttl = it.sync { redis -> redis.pttl("test") }
            assertTrue(pttl in 1..ttl.toMillis(), "expected pttl in (0, ${ttl.toMillis()}] but was $pttl")
        }
    }

    @Test
    fun `unlock after expiration fails`() {
        val mutex = Mutex(backends)

        assertTrue(mutex.lock("test", Duration.ofMillis(300)))
        runBlocking { delay(400) }

        assertFalse(mutex.unlock("test"))
    }

    @Test
    fun `non-owner cannot unlock`() {
        val owner = Mutex(backends)
        val other = Mutex(backends = backends, retryCount = 1, retryDelay = Duration.ofMillis(10))

        assertTrue(owner.lock("test", Duration.ofSeconds(10)))
        assertFalse(other.unlock("test"))

        val clients = instances.map { it.sync { redis -> redis.get("test") } }
        assertNotNull(clients[0])
        assertTrue(clients[0] == clients[1] && clients[1] == clients[2])

        assertTrue(owner.unlock("test"))
        instances.map { it.sync { redis -> redis.get("test") } }.forEach { assertNull(it) }
    }

    @Test
    fun `another client can re-acquire lock`() {
        val mutex = Mutex(backends)
        val mutex2 = Mutex(backends = backends, retryCount = 2, retryDelay = Duration.ofMillis(50))

        assertTrue(mutex.lock("test", Duration.ofSeconds(10)))
        assertFalse(mutex2.lock("test", Duration.ofMillis(100)))

        mutex.unlock("test")
        assertTrue(mutex2.lock("test", Duration.ofMillis(100)))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val mutex = Mutex(backends)
        val mutex2 = Mutex(backends = backends, retryCount = 2, retryDelay = Duration.ofMillis(30))

        assertTrue(mutex.lock("test", Duration.ofMillis(200)))
        assertFalse(mutex2.lock("test", Duration.ofMillis(100)))

        runBlocking { delay(200) }
        assertTrue(mutex2.lock("test", Duration.ofMillis(100)))
    }

    @Test
    fun `dont allow to lock again`() {
        val mutex = Mutex(backends)
        val mutex2 = Mutex(backends)

        assertTrue(mutex.lock("test", Duration.ofSeconds(10)))
        assertFalse(mutex2.lock("test", Duration.ofMillis(100)))
    }
}
