package com.himadieiev.redpulsar.core.locks

import TestTags
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration

@Tag(TestTags.UNIT)
class LockRaceTest {
    private lateinit var backend: FakeLocksBackend

    @BeforeEach
    fun setUp() {
        backend = FakeLocksBackend()
    }

    @ParameterizedTest(name = "single winner among {0} concurrent mutex contenders")
    @ValueSource(ints = [5, 10, 25])
    fun `only one contender wins the mutex`(contenders: Int) {
        val results =
            runBlocking {
                coroutineScope {
                    (1..contenders)
                        .map {
                            async(Dispatchers.Default) {
                                val mutex = Mutex(listOf(backend), retryCount = 1, retryDelay = Duration.ofMillis(1))
                                mutex.lock("test", Duration.ofSeconds(10))
                            }
                        }.awaitAll()
                }
            }

        assertEquals(1, results.count { it }, "exactly one contender must acquire the lock")
        assertTrue(backend.peek("test") != null, "winning key must exist")
    }

    @ParameterizedTest(name = "single winner among {0} concurrent simplified mutex contenders")
    @ValueSource(ints = [5, 10, 25])
    fun `only one contender wins the simplified mutex`(contenders: Int) {
        val results =
            runBlocking {
                coroutineScope {
                    (1..contenders)
                        .map {
                            async(Dispatchers.Default) {
                                val mutex =
                                    SimplifiedMutex(backend, retryCount = 1, retryDelay = Duration.ofMillis(1))
                                mutex.lock("test", Duration.ofSeconds(10))
                            }
                        }.awaitAll()
                }
            }

        assertEquals(1, results.count { it }, "exactly one contender must acquire the lock")
        assertTrue(backend.peek("test") != null, "winning key must exist")
    }

    @ParameterizedTest(name = "single lease among {0} concurrent semaphore contenders on one node")
    @ValueSource(ints = [2, 3, 5])
    fun `semaphore caps leases per node`(maxLeases: Int) {
        val contenders = maxLeases + 2
        val results =
            runBlocking {
                coroutineScope {
                    (1..contenders)
                        .map {
                            async(Dispatchers.Default) {
                                val semaphore =
                                    Semaphore(
                                        listOf(backend),
                                        maxLeases,
                                        retryCount = 1,
                                        retryDelay = Duration.ofMillis(1),
                                    )
                                semaphore.lock("test", Duration.ofSeconds(10))
                            }
                        }.awaitAll()
                }
            }

        // the single node is the only quorum member, so it enforces maxLeases by itself
        assertEquals(
            maxLeases,
            results.count { it },
            "expected exactly $maxLeases leases on a single node, got ${results.count { it }}",
        )
    }

    @Test
    fun `owner can release its own mutex lock`() {
        val owner = Mutex(listOf(backend), retryCount = 1, retryDelay = Duration.ofMillis(1))
        val other = Mutex(listOf(backend), retryCount = 1, retryDelay = Duration.ofMillis(1))

        assertTrue(owner.lock("test", Duration.ofSeconds(10)))

        // non-owner cannot release, value stays
        assertTrue(!other.unlock("test"))
        assertTrue(backend.peek("test") != null)

        assertTrue(owner.unlock("test"))
        assertTrue(backend.peek("test") == null)
    }
}
