group = "me.himadieiev.redpulsar-lettuce"

dependencies {
    api("io.lettuce:lettuce-core:6.3.0.RELEASE")
    implementation("org.apache.commons:commons-pool2:2.12.0")
    implementation(project(mapOf("path" to ":redpulsar-core")))
}
