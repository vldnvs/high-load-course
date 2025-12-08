package ru.quipy.common.utils

class RateLimitException(message: String = "Rate limit exceeded") : RuntimeException(message)