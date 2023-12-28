package io.redpulsar.locks

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import redis.clients.jedis.Pipeline
import redis.clients.jedis.UnifiedJedis
import java.io.IOException
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class SemaphoreTest {
    @Nested
    inner class SingleRedisInstance {
        private lateinit var redis: UnifiedJedis

        @BeforeEach
        fun setUp() {
            redis = mockk<UnifiedJedis>()
        }

        @ParameterizedTest(name = "lock acquired with {0} seconds ttl")
        @ValueSource(ints = [1, 2, 5, 7, 10])
        fun `lock acquired`(ttl: Int) {
            every { redis.eval(any<String>(), any<List<String>>(), any<List<String>>()) } returns "OK"

            val semaphore = Semaphore(listOf(redis), 3)
            val permit = semaphore.lock("test", ttl.seconds)

            assertTrue(permit)
            verify(exactly = 1) {
                redis.eval(
                    any<String>(),
                    match<List<String>> {
                        it.size == 2 &&
                            it[0] == "semaphore:leasers:test" &&
                            it[1].startsWith("semaphore:test:")
                    },
                    any<List<String>>(),
                )
            }
        }

        @Test
        fun `instance is down`() {
            val pipelined = mockk<Pipeline>()
            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } throws IOException()
            every { redis.pipelined() } returns pipelined
            every { pipelined.srem(any<String>(), any()) } returns mockk()
            every { pipelined.del(any<String>()) } returns mockk()
            every { pipelined.sync() } throws IOException()

            val semaphore = Semaphore(listOf(redis), 3, 15.milliseconds, 4)
            val permit = semaphore.lock("test")

            assertFalse(permit)

            verify(exactly = 4) {
                redis.eval(
                    any<String>(),
                    match<List<String>> {
                        it.size == 2 &&
                            it[0] == "semaphore:leasers:test" &&
                            it[1].startsWith("semaphore:test:")
                    },
                    any<List<String>>(),
                )
                pipelined.srem(eq("semaphore:leasers:test"), any())
                pipelined.del(match<String> { it.startsWith("semaphore:test:") })
                pipelined.sync()
            }
        }

        @Test
        fun `instance is down only for the first command`() {
            val pipelined = mockk<Pipeline>()
            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } throws IOException()
            every { redis.eval(any(), eq(listOf("semaphore:leasers:test")), eq(listOf("semaphore:test"))) } returns "OK"
            every { redis.pipelined() } returns pipelined
            every { pipelined.srem(any<String>(), any()) } returns mockk()
            every { pipelined.del(any<String>()) } returns mockk()
            every { pipelined.sync() } returns Unit
            every { pipelined.close() } returns Unit

            val semaphore = Semaphore(listOf(redis), 3, 15.milliseconds, 2)
            val permit = semaphore.lock("test")

            assertFalse(permit)

            verify(exactly = 2) {
                redis.eval(
                    any<String>(),
                    match<List<String>> {
                        it.size == 2 &&
                            it[0] == "semaphore:leasers:test" &&
                            it[1].startsWith("semaphore:test:")
                    },
                    any<List<String>>(),
                )
                // unlocking
                pipelined.srem(eq("semaphore:leasers:test"), any())
                pipelined.del(match<String> { it.startsWith("semaphore:test:") })
                pipelined.sync()
                // cleaning up
                redis.eval(any(), eq(listOf("semaphore:leasers:test")), eq(listOf("semaphore:test")))
            }
        }

        @Test
        fun `lock already taken`() {
            val pipelined = mockk<Pipeline>()
            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns null
            every { redis.pipelined() } returns pipelined
            every { pipelined.srem(any<String>(), any()) } returns mockk()
            every { pipelined.del(any<String>()) } returns mockk()
            every { pipelined.sync() } returns Unit
            every { pipelined.close() } returns Unit

            val semaphore = Semaphore(listOf(redis), 3, 15.milliseconds, 2)
            val permit = semaphore.lock("test", 1.seconds)

            assertFalse(permit)

            verify(exactly = 2) {
                redis.eval(
                    any<String>(),
                    match<List<String>> {
                        it.size == 2 &&
                            it[0] == "semaphore:leasers:test" &&
                            it[1].startsWith("semaphore:test:")
                    },
                    any<List<String>>(),
                )
                // unlocking
                pipelined.srem(eq("semaphore:leasers:test"), any())
                pipelined.del(match<String> { it.startsWith("semaphore:test:") })
                pipelined.sync()
                // cleaning up
                redis.eval(any(), eq(listOf("semaphore:leasers:test")), eq(listOf("semaphore:test")))
            }
        }

        @Test
        fun `unlock resource`() {
            val pipelined = mockk<Pipeline>()
            every { redis.eval(any(), eq(listOf("semaphore:leasers:test")), eq(listOf("semaphore:test"))) } returns null
            every { redis.pipelined() } returns pipelined
            every { pipelined.srem(any<String>(), any()) } returns mockk()
            every { pipelined.del(any<String>()) } returns mockk()
            every { pipelined.sync() } returns Unit
            every { pipelined.close() } returns Unit

            val semaphore = Semaphore(listOf(redis), 3)
            semaphore.unlock("test")

            verify(exactly = 1) {
                // unlocking
                pipelined.srem(eq("semaphore:leasers:test"), any())
                pipelined.del(match<String> { it.startsWith("semaphore:test:") })
                pipelined.sync()
                // cleaning up
                redis.eval(any(), eq(listOf("semaphore:leasers:test")), eq(listOf("semaphore:test")))
            }
            verify(exactly = 0) {
                redis.eval(
                    any<String>(),
                    match<List<String>> {
                        it.size == 2 &&
                            it[0] == "semaphore:leasers:test" &&
                            it[1].startsWith("semaphore:test:")
                    },
                    any<List<String>>(),
                )
            }
        }

        @ParameterizedTest(name = "Validated with retry count - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry count`(retryCount: Int) {
            if (retryCount > 0) {
                Assertions.assertDoesNotThrow { Semaphore(listOf(redis), 3, retryCount = retryCount) }
            } else {
                assertThrows<IllegalArgumentException> { Semaphore(listOf(redis), 3, retryCount = retryCount) }
            }
        }

        @ParameterizedTest(name = "Validated with retry delly - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate retry delly`(retryDelly: Int) {
            if (retryDelly > 0) {
                Assertions.assertDoesNotThrow { Semaphore(listOf(redis), 3, retryDelay = retryDelly.milliseconds) }
            } else {
                assertThrows<IllegalArgumentException> {
                    Semaphore(
                        listOf(redis),
                        3,
                        retryDelay = retryDelly.milliseconds,
                    )
                }
            }
        }

        @ParameterizedTest(name = "Validated with max leases - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate max leases`(maxLeases: Int) {
            if (maxLeases > 0) {
                Assertions.assertDoesNotThrow { Semaphore(listOf(redis), maxLeases) }
            } else {
                assertThrows<IllegalArgumentException> { Semaphore(listOf(redis), maxLeases) }
            }
        }

        @Test
        fun `validate instance count`() {
            Assertions.assertDoesNotThrow { Semaphore(listOf(redis), 3) }
            assertThrows<IllegalArgumentException> { Semaphore(listOf(), 3) }
        }

        @ParameterizedTest(name = "lock acquired with ttl - {0}")
        @ValueSource(ints = [-123, -1, 0, 1, 2, 5, 7, 10])
        fun `validate ttl`(ttl: Int) {
            every { redis.eval(any(), any<List<String>>(), any<List<String>>()) } returns "OK"

            val semaphore = Semaphore(listOf(redis), 3)
            if (ttl > 10) {
                Assertions.assertDoesNotThrow { semaphore.lock("test", ttl.milliseconds) }
            } else {
                assertThrows<IllegalArgumentException> { semaphore.lock("test", ttl.milliseconds) }
            }
        }
    }

    @Nested
    inner class MultipleRedisInstance {
        private lateinit var redis1: UnifiedJedis
        private lateinit var redis2: UnifiedJedis
        private lateinit var redis3: UnifiedJedis
        private lateinit var instances: List<UnifiedJedis>

        @BeforeEach
        fun setUp() {
            redis1 = mockk<UnifiedJedis>()
            redis2 = mockk<UnifiedJedis>()
            redis3 = mockk<UnifiedJedis>()
            instances = listOf(redis1, redis2, redis3)
        }

        @Test
        fun `all instances are in quorum`() {
            instances.forEach { redis ->
                every { redis.eval(any<String>(), any<List<String>>(), any<List<String>>()) } returns "OK"
            }

            val semaphore = Semaphore(instances, 3)
            val permit = semaphore.lock("test")

            assertTrue(permit)
            instances.forEach { redis ->
                verify(exactly = 1) {
                    redis.eval(
                        any<String>(),
                        match<List<String>> {
                            it.size == 2 &&
                                it[0] == "semaphore:leasers:test" &&
                                it[1].startsWith("semaphore:test:")
                        },
                        any<List<String>>(),
                    )
                }
            }
        }

        @Test
        fun `two instances are in quorum`() {
            every { redis1.eval(any<String>(), any<List<String>>(), any<List<String>>()) } returns "OK"
            every { redis2.eval(any<String>(), any<List<String>>(), any<List<String>>()) } returns null
            every { redis3.eval(any<String>(), any<List<String>>(), any<List<String>>()) } returns "OK"

            val semaphore = Semaphore(instances, 3)
            val permit = semaphore.lock("test")

            assertTrue(permit)

            instances.forEach { redis ->
                verify(exactly = 1) {
                    redis.eval(
                        any<String>(),
                        match<List<String>> {
                            it.size == 2 &&
                                it[0] == "semaphore:leasers:test" &&
                                it[1].startsWith("semaphore:test:")
                        },
                        any<List<String>>(),
                    )
                }
            }
        }

        @Test
        fun `quorum wasn't reach`() {
            val pipelines = listOf<Pipeline>(mockk(), mockk(), mockk())
            every { redis1.eval(any<String>(), any<List<String>>(), any<List<String>>()) } returns null
            every { redis2.eval(any<String>(), any<List<String>>(), any<List<String>>()) } returns "OK"
            every { redis3.eval(any<String>(), any<List<String>>(), any<List<String>>()) } returns null
            val pipelineIterator = pipelines.iterator()
            instances.forEach { redis ->
                val pipelined = pipelineIterator.next()
                every {
                    redis.eval(
                        any(),
                        eq(listOf("semaphore:leasers:test")),
                        eq(listOf("semaphore:test")),
                    )
                } returns "OK"
                every { redis.pipelined() } returns pipelined
                every { pipelined.srem(any<String>(), any()) } returns mockk()
                every { pipelined.del(any<String>()) } returns mockk()
                every { pipelined.sync() } returns Unit
                every { pipelined.close() } returns Unit
            }

            val semaphore = Semaphore(listOf(redis1, redis2, redis3), 3)
            val permit = semaphore.lock("test")

            assertFalse(permit)

            instances.forEach { redis ->
                verify(exactly = 3) {
                    redis.eval(
                        any<String>(),
                        match<List<String>> {
                            it.size == 2 &&
                                it[0] == "semaphore:leasers:test" &&
                                it[1].startsWith("semaphore:test:")
                        },
                        any<List<String>>(),
                    )
                    redis.eval(any(), eq(listOf("semaphore:leasers:test")), eq(listOf("semaphore:test")))
                }
            }
            pipelines.forEach { pipelined ->
                verify(exactly = 3) {
                    pipelined.srem(eq("semaphore:leasers:test"), any())
                    pipelined.del(match<String> { it.startsWith("semaphore:test:") })
                    pipelined.sync()
                }
            }
        }
    }
}
