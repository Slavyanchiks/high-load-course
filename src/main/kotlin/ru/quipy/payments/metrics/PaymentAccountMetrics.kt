package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import ru.quipy.common.utils.DeadlineAwareLeakyBucket
import ru.quipy.common.utils.OngoingWindow
import java.time.Duration

class PaymentAccountMetrics(
    val registry: MeterRegistry,
    val serviceName: String,
    val accountName: String,
    rateLimitPerSec: Int,
    bucket: DeadlineAwareLeakyBucket,
    window: OngoingWindow,
) {
    private val queueWaitTimer = Timer.builder("payment.queue.wait")
        .tag("service", serviceName)
        .tag("account", accountName)
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry)

    init {
        Gauge.builder("payment.external.inflight", window) { it.inFlight().toDouble() }
            .tag("service", serviceName)
            .tag("account", accountName)
            .register(registry)

        Gauge.builder("payment.external.waiting", bucket) { it.waiting().toDouble() }
            .tag("service", serviceName)
            .tag("account", accountName)
            .register(registry)

        Gauge.builder("payment.external.rate.limit") { rateLimitPerSec.toDouble() }
            .tag("service", serviceName)
            .tag("account", accountName)
            .register(registry)
    }

    fun recordQueueWait(duration: Duration) = queueWaitTimer.record(duration)

    private val successfulAttempts = counter("payment.external.attempt", "success")
    private val retryableAttemptFailures = counter("payment.external.attempt", "retryable_failure")
    private val permanentAttemptFailures = counter("payment.external.attempt", "permanent_failure")
    private val attemptsRejectedByDeadline = counter("payment.external.attempt", "deadline_exceeded")

    private val enqueuedRetries = counter("payment.external.retry", "enqueued")
    private val retriesRejectedByDeadline = counter("payment.external.retry", "rejected_deadline")
    private val retriesRejectedByAttemptLimit = counter("payment.external.retry", "rejected_attempt_limit")

    fun recordSuccessfulAttempt() = successfulAttempts.increment()

    fun recordRetryableAttemptFailure() = retryableAttemptFailures.increment()

    fun recordPermanentAttemptFailure() = permanentAttemptFailures.increment()

    fun recordAttemptRejectedByDeadline() = attemptsRejectedByDeadline.increment()

    fun recordRetryEnqueued() = enqueuedRetries.increment()

    fun recordRetryRejectedByDeadline() = retriesRejectedByDeadline.increment()

    fun recordRetryRejectedByAttemptLimit() = retriesRejectedByAttemptLimit.increment()

    private fun counter(name: String, outcome: String) = Counter.builder(name)
        .tag("service", serviceName)
        .tag("account", accountName)
        .tag("outcome", outcome)
        .register(registry)
}
