import redis.clients.jedis.CommandArguments
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.params.SetParams

/** Interface for testing tags. Avoiding manual string typing. */
interface TestTags {
    companion object {
        const val INTEGRATIONS = "integration"
        const val UNIT = "unit"
    }
}
/** End. */

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
/** End. */
