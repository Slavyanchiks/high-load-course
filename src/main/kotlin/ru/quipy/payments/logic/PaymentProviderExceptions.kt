package ru.quipy.payments.logic

internal class RetryablePaymentException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

internal class PermanentPaymentException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
