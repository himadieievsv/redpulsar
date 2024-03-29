import com.himadieiev.redpulsar.lettuce.LettuceClusterPooled
import com.himadieiev.redpulsar.lettuce.LettucePubSubPooled
import com.himadieiev.redpulsar.lettuce.abstracts.LettuceUnified
import io.lettuce.core.SetArgs
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.protocol.CommandArgs
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.time.Duration

/** Interface for testing tags. Avoiding manual string typing. */
interface TestTags {
    companion object {
        const val INTEGRATIONS = "integration"
        const val UNIT = "unit"
    }
}

/**  Extension for ease of comparing [SetArgs] objects. */
fun SetArgs.equalsTo(other: SetArgs): Boolean {
    val args1 = CommandArgs(StringCodec())
    val args2 = CommandArgs(StringCodec())
    this.build(args1)
    other.build(args2)
    return args1.toString() == args2.toString()
}

/** Integration tests instance initiations */
fun getInstances(): List<LettucePubSubPooled<String, String>> {
    val poolConfig =
        GenericObjectPoolConfig<StatefulRedisPubSubConnection<String, String>>().apply {
            maxTotal = 8
            maxIdle = 2
            minIdle = 1
            setMaxWait(Duration.ofMillis(100))
            blockWhenExhausted = true
        }

    return listOf(
        LettucePubSubPooled(poolConfig) { RedisClusterClient.create(getHostPort(1)).connectPubSub() },
        LettucePubSubPooled(poolConfig) { RedisClusterClient.create(getHostPort(2)).connectPubSub() },
        LettucePubSubPooled(poolConfig) { RedisClusterClient.create(getHostPort(3)).connectPubSub() },
    )
}

fun getPooledInstances(): List<LettuceUnified<String, String>> {
    val poolConfig =
        GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>>().apply {
            maxTotal = 8
            maxIdle = 2
            minIdle = 1
            setMaxWait(Duration.ofMillis(100))
            blockWhenExhausted = true
        }

    return listOf(
        LettuceClusterPooled(poolConfig) { RedisClusterClient.create(getHostPort(1)).connect() },
        LettuceClusterPooled(poolConfig) { RedisClusterClient.create(getHostPort(2)).connect() },
        LettuceClusterPooled(poolConfig) { RedisClusterClient.create(getHostPort(3)).connect() },
    )
}

private fun getHostPort(number: Int): String {
    val host = System.getenv("REDIS_HOST$number") ?: "localhost"
    val port = System.getenv("REDIS_PORT$number")?.toInt() ?: (7010 + 10 * (number - 1))
    return "redis://$host:$port"
}
