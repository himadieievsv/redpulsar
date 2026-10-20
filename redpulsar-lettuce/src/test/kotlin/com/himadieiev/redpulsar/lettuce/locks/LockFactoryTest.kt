package com.himadieiev.redpulsar.lettuce.locks

import com.himadieiev.redpulsar.core.locks.ListeningCountDownLatch
import com.himadieiev.redpulsar.core.locks.Mutex
import com.himadieiev.redpulsar.core.locks.Semaphore
import com.himadieiev.redpulsar.core.locks.SimplifiedMutex
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class LockFactoryTest {
    @Test
    fun createSimplifiedMutex() {
        assertNotNull(LockFactory.createSimplifiedMutex(client = mockk()))
        assertTrue(LockFactory.createSimplifiedMutex(client = mockk()) is SimplifiedMutex)
        assertThrows<IllegalArgumentException> {
            LockFactory.createSimplifiedMutex(client = mockk(), retryDelay = Duration.ofSeconds(0))
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createSimplifiedMutex(client = mockk(), retryCount = -1)
        }
    }

    @Test
    fun createMutex() {
        assertTrue(LockFactory.createMutex(clients = listOf(mockk())) is Mutex)
        assertTrue(LockFactory.createMutex(clients = listOf(mockk(), mockk())) is Mutex)
        assertThrows<IllegalArgumentException> {
            LockFactory.createMutex(clients = listOf())
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createMutex(clients = listOf(mockk()), retryDelay = Duration.ofSeconds(0))
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createMutex(clients = listOf(mockk()), retryCount = -1)
        }
    }

    @Test
    fun createSemaphore() {
        val semaphore = LockFactory.createSemaphore(clients = listOf(mockk()), maxLeases = 3)
        assertNotNull(semaphore)
        assertTrue(semaphore is Semaphore)
        assertThrows<IllegalArgumentException> {
            LockFactory.createSemaphore(clients = listOf(), maxLeases = 3)
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createSemaphore(clients = listOf(mockk()), maxLeases = 0)
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createSemaphore(clients = listOf(mockk()), maxLeases = 3, retryDelay = Duration.ofSeconds(0))
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createSemaphore(clients = listOf(mockk()), maxLeases = 3, retryCount = 0)
        }
    }

    @Test
    fun createCountDownLatch() {
        val latch = LockFactory.createCountDownLatch(clients = listOf(mockk()), name = "test", count = 3)
        assertNotNull(latch)
        assertTrue(latch is ListeningCountDownLatch)
        assertThrows<IllegalArgumentException> {
            LockFactory.createCountDownLatch(clients = listOf(), name = "test", count = 3)
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createCountDownLatch(clients = listOf(mockk()), name = "", count = 3)
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createCountDownLatch(clients = listOf(mockk()), name = "test", count = 0)
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createCountDownLatch(
                clients = listOf(mockk()),
                name = "test",
                count = 3,
                retryDelay = Duration.ofSeconds(0),
            )
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createCountDownLatch(
                clients = listOf(mockk()),
                name = "test",
                count = 3,
                retryCount = 0,
            )
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createCountDownLatch(
                clients = listOf(mockk()),
                name = "test",
                count = 3,
                maxDuration = Duration.ofSeconds(0),
            )
        }
    }
}
