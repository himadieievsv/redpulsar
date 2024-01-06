dependencies {
    api("redis.clients:jedis:5.1.0")
    implementation("org.apache.commons:commons-pool2:2.12.0")
    implementation(project(mapOf("path" to ":redpulsar-core")))
}
