package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger

class PaymentAdmissionMetrics(
    registry: MeterRegistry,
    paymentsInProgress: AtomicInteger,
    paymentExecutor: ThreadPoolExecutor,
) {
    private val acceptedPayments = Counter.builder("payment.incoming").tag("result", "accepted").register(registry)
    private val rejectedPayments = Counter.builder("payment.incoming").tag("result", "rejected").register(registry)

    private val deadlineBudget = percentileTimer("payment.deadline.budget", registry)

    private val estimatedLatency = percentileTimer("payment.estimated.latency", registry)

    private val retryAfterDelay = percentileTimer("payment.retry_after.delay", registry)

    init {
        Gauge.builder("payment.inprogress", paymentsInProgress) { it.get().toDouble() }.register(registry)
        Gauge.builder("payment.executor.queue", paymentExecutor) { it.queue.size.toDouble() }.register(registry)
    }

    fun recordAccepted(deadlineBudget: Duration, estimatedLatency: Duration) {
        acceptedPayments.increment()
        this.deadlineBudget.record(deadlineBudget)
        this.estimatedLatency.record(estimatedLatency)
    }

    fun recordRejected(deadlineBudget: Duration, estimatedLatency: Duration, retryAfterDelay: Duration) {
        rejectedPayments.increment()
        this.deadlineBudget.record(deadlineBudget)
        this.estimatedLatency.record(estimatedLatency)
        this.retryAfterDelay.record(retryAfterDelay)
    }
}

internal val UPPER_PERCENTILES = doubleArrayOf(0.5, 0.95, 0.99)

internal fun percentileTimer(
    name: String,
    registry: MeterRegistry,
    vararg tags: String,
    percentiles: DoubleArray = UPPER_PERCENTILES,
): Timer =
    Timer.builder(name)
        .tags(*tags)
        .publishPercentiles(*percentiles)
        .register(registry)
