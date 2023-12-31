import io.lettuce.core.RedisClient
import io.lettuce.core.SetArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.protocol.CommandArgs
import io.redpulsar.lettuce.LettucePooled
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.time.Duration

/** Interface for testing tags. Avoiding manual string typing. */
interface TestTags {
    companion object {
        const val INTEGRATIONS = "integration"
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
fun getInstances(): List<LettucePooled<String, String>> {
    val poolConfig =
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>>().apply {
            maxTotal = 64
            maxIdle = 8
            minIdle = 2
            setMaxWait(Duration.ofMillis(100))
            blockWhenExhausted = true
        }

    return listOf(
        LettucePooled(poolConfig) { RedisClient.create(getHostPort(1)).connect() },
        LettucePooled(poolConfig) { RedisClient.create(getHostPort(2)).connect() },
        LettucePooled(poolConfig) { RedisClient.create(getHostPort(3)).connect() },
    )
}

private fun getHostPort(number: Int): String {
    val host = System.getenv("REDIS_HOST$number") ?: "localhost"
    val port = System.getenv("REDIS_PORT$number")?.toInt() ?: (6380 + number)
    return "redis://$host:$port"
}
