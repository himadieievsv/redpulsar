import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.CommandArguments
import redis.clients.jedis.Connection
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisPooled
import redis.clients.jedis.UnifiedJedis
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.params.SetParams
import java.time.Duration

/** Interface for testing tags. Avoiding manual string typing. */
interface TestTags {
    companion object {
        const val INTEGRATIONS = "integration"
        const val UNIT = "unit"
    }
}

/** Integration tests instance initiations */
fun getInstances(): List<UnifiedJedis> {
    val poolConfig =
        GenericObjectPoolConfig<Connection>().apply {
            maxTotal = 64
            maxIdle = 8
            minIdle = 2
            setMaxWait(Duration.ofMillis(100))
            blockWhenExhausted = true
        }

    val hostPort1 = getHostPort(1)
    val jedis1 = JedisPooled(poolConfig, hostPort1.host, hostPort1.port, 20)
    val hostPort2 = getHostPort(2)
    val jedis2 = JedisPooled(poolConfig, hostPort2.host, hostPort2.port, 20)
    val hostPort3 = getHostPort(3)
    val jedis3 = JedisPooled(poolConfig, hostPort3.host, hostPort3.port, 20)
    return listOf(jedis1, jedis2, jedis3)
}

private fun getHostPort(number: Int) =
    HostAndPort(
        System.getenv("REDIS_HOST$number") ?: "localhost",
        (System.getenv("REDIS_PORT$number")?.toInt() ?: (6380 + number)),
    )
