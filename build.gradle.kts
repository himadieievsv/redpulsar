import java.net.URI

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    kotlin("jvm") version "2.0.20"
    `java-library`
    id("org.jlleitschuh.gradle.ktlint") version "12.1.1"
    id("org.jetbrains.kotlinx.kover") version "0.8.3"
    `maven-publish`
    signing
    idea
}

allprojects {
    group = "com.himadieiev"
    version = "1.3.2"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "kotlin")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "idea")
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")
    apply(plugin = "org.jetbrains.kotlinx.kover")

    kotlin {
        jvmToolchain(11)
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(11))
        }

        withJavadocJar()
        withSourcesJar()
    }

    val artifacts =
        mapOf(
            "redpulsar-core" to
                mapOf(
                    "name" to "RedPulsar Core",
                    "description" to "Provides core functionality for RedPulsar Distributed locks and utilities.",
                    "url" to "https://github.com/himadieievsv/redpulsar/tree/main/redpulsar-core",
                ),
            "redpulsar-jedis" to
                mapOf(
                    "name" to "RedPulsar Jedis",
                    "description" to "RedPulsar Distributed locks and utilities for Redis with Jedis client.",
                    "url" to "https://github.com/himadieievsv/redpulsar/tree/main/redpulsar-jedis",
                ),
            "redpulsar-lettuce" to
                mapOf(
                    "name" to "RedPulsar Lettuce",
                    "description" to "RedPulsar Distributed locks and utilities for Redis with Lettuce client",
                    "url" to "https://github.com/himadieievsv/redpulsar/tree/main/redpulsar-lettuce",
                ),
        )

    dependencies {
        implementation("io.github.microutils:kotlin-logging:3.0.5")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
        implementation("org.slf4j:slf4j-simple:2.0.9")

        testImplementation(platform("org.junit:junit-bom:5.10.1"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testImplementation("io.mockk:mockk:1.13.8")
    }

    publishing {
        repositories {
            maven {
                url = URI.create("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
                credentials {
                    username = providers.gradleProperty("ossrhUsername").get()
                    password = providers.gradleProperty("ossrhPassword").get()
                }
            }
        }

        publications {
            create<MavenPublication>("mavenJava") {
                groupId = group.toString()
                artifactId = project.name
                version = version.toString()
                from(components["java"])

                pom {
                    name.set(artifacts[project.name]?.get("name"))
                    description.set(artifacts[project.name]?.get("description"))
                    url.set(artifacts[project.name]?.get("url"))
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("himadieievsv")
                            name.set("Serhii Himadieiev")
                        }
                    }
                    scm {
                        connection.set("scm:git:git://github.com/himadieievsv/redpulsar.git")
                        developerConnection.set("scm:git:ssh://github.com/himadieievsv/redpulsar.git")
                        url.set("https://github.com/himadieievsv/redpulsar")
                    }
                }
            }
        }
    }

    signing {
        sign(publishing.publications["mavenJava"])
    }

    tasks.test {
        useJUnitPlatform {
            excludeTags(*System.getProperty("excludeTags", "no-tag").split(",").toTypedArray())
        }
        reports {
            junitXml.apply {
                isOutputPerTestCase = true
            }
        }
    }
}
