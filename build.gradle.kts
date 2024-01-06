buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    kotlin("jvm") version "1.9.22"
    id("org.jlleitschuh.gradle.ktlint") version "12.0.3"
    `java-library`
    idea
}

allprojects {
    group = "me.himadieiev.redpulsar"
    version = "0.1.1"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "kotlin")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "idea")
    apply(plugin = "java-library")

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

    kotlin {
        jvmToolchain(11)
    }

    dependencies {
        api("io.github.microutils:kotlin-logging:3.0.5")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

        testImplementation(platform("org.junit:junit-bom:5.10.1"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testImplementation("io.mockk:mockk:1.13.8")
        testImplementation("org.slf4j:slf4j-simple:2.0.9")
    }
}
