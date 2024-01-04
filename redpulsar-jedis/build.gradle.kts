group = "me.himadieiev.redpulsar-jedis"

dependencies {
    api("redis.clients:jedis:5.1.0")
    implementation(project(mapOf("path" to ":redpulsar-core")))
}
