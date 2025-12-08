package ru.quipy.common.utils

class RateLimitException(
    message: String = "Rate limit exception"
) : RuntimeException(message)