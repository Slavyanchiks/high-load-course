package ru.quipy.payments.logic

import ru.quipy.common.utils.DeadlineAwareLeakyBucket

internal class PaymentRetryPolicy(
    private val maxAttempts: Int,
    private val bucket: DeadlineAwareLeakyBucket,
) {
    fun reserveRetry(attempt: Int, deadline: Long): RetryDecision {
        if (attempt >= maxAttempts) return RetryDecision.REJECTED_ATTEMPT_LIMIT
        if (bucket.tryReserve(deadline) != null) return RetryDecision.REJECTED_DEADLINE
        return RetryDecision.RETRY_RESERVED
    }
}

internal enum class RetryDecision {
    RETRY_RESERVED,
    REJECTED_DEADLINE,
    REJECTED_ATTEMPT_LIMIT,
}
