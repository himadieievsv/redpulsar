package com.himadieiev.redpulsar.lettuce.locks

import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

class LockFactoryTest {
    @Test
    fun createSimpleLock() {
        assertThrows<IllegalArgumentException> {
            LockFactory.createSimplifiedMutex(client = mockk(), retryDelay = Duration.ofSeconds(0))
        }
        assertThrows<IllegalArgumentException> {
            LockFactory.createSimplifiedMutex(client = mockk(), retryCount = -1)
        }
    }

    @Test
    fun createRedLock() {
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
