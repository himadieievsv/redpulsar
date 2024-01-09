dependencies {
    api("io.lettuce:lettuce-core:6.3.0.RELEASE")
    api("org.apache.commons:commons-pool2:2.12.0")
    api(project(mapOf("path" to ":redpulsar-core")))
}
