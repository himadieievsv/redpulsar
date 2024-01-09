dependencies {
    api("redis.clients:jedis:5.1.0")
    api("org.apache.commons:commons-pool2:2.12.0")
    api(project(mapOf("path" to ":redpulsar-core")))
}
