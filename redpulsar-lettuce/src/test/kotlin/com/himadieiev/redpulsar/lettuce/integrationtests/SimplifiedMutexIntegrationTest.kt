package com.himadieiev.redpulsar.lettuce.integrationtests

import TestTags
import com.himadieiev.redpulsar.core.locks.SimplifiedMutex
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
class SimplifiedMutexIntegrationTest {
    private lateinit var redis: LettuceUnified<String, String>
    private lateinit var backend: LocksBackend

    @BeforeEach
    fun setUp() {
        redis = getPooledInstances()[0]
        redis.sync { redis -> redis.flushall() }
        backend = LettuceLocksBackend(redis)
    }

    @Test
    fun `obtain lock`() {
        val simplifiedMutex = SimplifiedMutex(backend)
        val permit = simplifiedMutex.lock("test", Duration.ofSeconds(10))

        assertTrue(permit)

        assertNotNull(redis.sync { redis -> redis.get("test") })
    }

    @Test
    fun `release lock`() {
        val simplifiedMutex = SimplifiedMutex(backend)
        assertTrue(simplifiedMutex.lock("test", Duration.ofSeconds(10)))
        assertTrue(simplifiedMutex.unlock("test"))

        assertNull(redis.sync { redis -> redis.get("test") })
    }

    @Test
    fun `lock sets ttl`() {
        val ttl = Duration.ofSeconds(2)
        val simplifiedMutex = SimplifiedMutex(backend)

        assertTrue(simplifiedMutex.lock("test", ttl))

        val pttl = redis.sync { redis -> redis.pttl("test") }
        assertTrue(pttl in 1..ttl.toMillis(), "expected pttl in (0, ${ttl.toMillis()}] but was $pttl")
    }

    @Test
    fun `unlock after expiration fails`() {
        val simplifiedMutex = SimplifiedMutex(backend)

        assertTrue(simplifiedMutex.lock("test", Duration.ofMillis(300)))
        runBlocking { delay(400) }

        assertFalse(simplifiedMutex.unlock("test"))
    }

    @Test
    fun `non-owner cannot unlock`() {
        val owner = SimplifiedMutex(backend)
        val other = SimplifiedMutex(backend, retryDelay = Duration.ofMillis(10), retryCount = 1)

        assertTrue(owner.lock("test", Duration.ofSeconds(10)))
        assertFalse(other.unlock("test"))
        assertNotNull(redis.sync { redis -> redis.get("test") })

        assertTrue(owner.unlock("test"))
        assertNull(redis.sync { redis -> redis.get("test") })
    }

    @Test
    fun `another client can re-acquire lock`() {
        val simplifiedMutex = SimplifiedMutex(backend)
        val simplifiedMutex2 = SimplifiedMutex(backend, retryDelay = Duration.ofMillis(50), retryCount = 2)

        assertTrue(simplifiedMutex.lock("test", Duration.ofSeconds(10)))
        assertFalse(simplifiedMutex2.lock("test", Duration.ofMillis(10)))

        simplifiedMutex.unlock("test")
        assertTrue(simplifiedMutex2.lock("test", Duration.ofMillis(10)))
    }

    @Test
    fun `another client can re-acquire lock due to expiration`() {
        val simplifiedMutex = SimplifiedMutex(backend)
        val simplifiedMutex2 = SimplifiedMutex(backend, retryDelay = Duration.ofMillis(30), retryCount = 2)

        assertTrue(simplifiedMutex.lock("test", Duration.ofMillis(200)))
        assertFalse(simplifiedMutex2.lock("test", Duration.ofMillis(10)))

        runBlocking { delay(200) }
        assertTrue(simplifiedMutex2.lock("test", Duration.ofMillis(10)))
    }

    @Test
    fun `dont allow to lock again`() {
        val simplifiedMutex = SimplifiedMutex(backend)
        val simplifiedMutex2 = SimplifiedMutex(backend)

        assertTrue(simplifiedMutex.lock("test", Duration.ofSeconds(10)))
        assertFalse(simplifiedMutex2.lock("test", Duration.ofMillis(10)))
    }
}
