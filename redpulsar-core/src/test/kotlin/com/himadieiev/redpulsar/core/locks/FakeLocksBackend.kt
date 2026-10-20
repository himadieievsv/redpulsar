package com.himadieiev.redpulsar.core.locks

import com.himadieiev.redpulsar.core.locks.abstracts.backends.LocksBackend
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

/**
 * In-memory [LocksBackend] with setNX/PX semantics for race testing.
 * Unlike a mock, it models real Redis atomicity so concurrent lock attempts can be observed.
 */
class FakeLocksBackend : LocksBackend() {
    private data class Entry(val value: String, val expiresAt: Long)

    private val store = ConcurrentHashMap<String, Entry>()

    override fun setLock(
        resourceName: String,
        clientId: String,
        ttl: Duration,
    ): String? =
        synchronized(store) {
            val existing = store[resourceName]
            if (existing != null && existing.expiresAt > System.currentTimeMillis()) {
                null
            } else {
                store[resourceName] = Entry(clientId, System.currentTimeMillis() + ttl.toMillis())
                "OK"
            }
        }

    override fun removeLock(
        resourceName: String,
        clientId: String,
    ): String? =
        synchronized(store) {
            val existing = store[resourceName]
            if (existing != null && existing.value == clientId) {
                store.remove(resourceName)
                "OK"
            } else {
                null
            }
        }

    override fun setSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
        maxLeases: Int,
        ttl: Duration,
    ): String? =
        synchronized(store) {
            val leasers = store[leasersKey]?.value?.split(",")?.filter { it.isNotEmpty() }?.toMutableSet() ?: mutableSetOf()
            val validity = store["$leasersKey.$clientId"]
            val expired = validity == null || validity.expiresAt <= System.currentTimeMillis()
            if (expired) {
                leasers.removeIf { store["$leasersKey.$it"].let { e -> e == null || e.expiresAt <= System.currentTimeMillis() } }
            }
            if (leasers.size < maxLeases) {
                leasers.add(clientId)
                store[leasersKey] = Entry(leasers.joinToString(","), System.currentTimeMillis() + ttl.toMillis())
                store["$leasersKey.$clientId"] = Entry(clientId, System.currentTimeMillis() + ttl.toMillis())
                "OK"
            } else {
                null
            }
        }

    override fun removeSemaphoreLock(
        leasersKey: String,
        leaserValidityKey: String,
        clientId: String,
    ): String? =
        synchronized(store) {
            val current = store[leasersKey]?.value ?: ""
            val remaining = current.split(",").filter { it.isNotEmpty() && it != clientId }
            store[leasersKey] = Entry(remaining.joinToString(","), System.currentTimeMillis() + 60_000)
            store.remove("$leasersKey.$clientId")
            "OK"
        }

    override fun cleanUpExpiredSemaphoreLocks(
        leasersKey: String,
        leaserValidityKeyPrefix: String,
    ): String? =
        synchronized(store) {
            val current = store[leasersKey]?.value ?: ""
            val remaining =
                current
                    .split(",")
                    .filter { it.isNotEmpty() }
                    .filter { clientId ->
                        val validity = store["$leasersKey.$clientId"]
                        validity != null && validity.expiresAt > System.currentTimeMillis()
                    }
            store[leasersKey] = Entry(remaining.joinToString(","), System.currentTimeMillis() + 60_000)
            "OK"
        }

    fun peek(key: String): String? {
        val entry = store[key]
        return if (entry != null && entry.expiresAt > System.currentTimeMillis()) entry.value else null
    }
}
