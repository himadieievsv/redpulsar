import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.CommandArguments
import redis.clients.jedis.Connection
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
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

/**  Extension for ease of comparing [SetParams] objects. */
fun SetParams.equalsTo(other: SetParams): Boolean {
    val thisArgs = TestCommandArguments().also { this.addParams(it) }
    val otherArgs = TestCommandArguments().also { other.addParams(it) }
    return thisArgs == otherArgs
}

private class TestCommandArguments : CommandArguments(ProtocolCommand { byteArrayOf() }) {
    private var args = mutableListOf<Any?>()

    override fun add(arg: Any?): CommandArguments {
        args.add(arg)
        return this
    }

    override fun hashCode(): Int {
        return args.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TestCommandArguments) return false

        if (args != other.args) return false

        return true
    }
}

/** Integration tests instance initiations */
fun getInstances(): List<JedisCluster> {
    val poolConfig =
        GenericObjectPoolConfig<Connection>().apply {
            maxTotal = 64
            maxIdle = 8
            minIdle = 2
            setMaxWait(Duration.ofMillis(100))
            blockWhenExhausted = true
        }

    val hostPort1 = getHostPort(1)
    val jedis1 = JedisCluster(hostPort1, 50, poolConfig)
    val hostPort2 = getHostPort(2)
    val jedis2 = JedisCluster(hostPort2, 50, poolConfig)
    val hostPort3 = getHostPort(3)
    val jedis3 = JedisCluster(hostPort3, 50, poolConfig)
    return listOf(jedis1, jedis2, jedis3)
}

private fun getHostPort(number: Int) =
    HostAndPort(
        System.getenv("REDIS_HOST$number") ?: "localhost",
        (System.getenv("REDIS_PORT$number")?.toInt() ?: (7010 + 10 * (number - 1))),
    )
