package com.himadieiev.redpulsar.core.utils

import java.security.MessageDigest

fun String.sha1(): String {
    val md = MessageDigest.getInstance("SHA-1")
    return md.digest(this.toByteArray()).joinToString("") { "%02x".format(it) }
}
