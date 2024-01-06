package com.himadieiev.redpulsar.lettuce.exceptions

class LettucePooledException(e: Exception, message: String) : RuntimeException(message, e)
