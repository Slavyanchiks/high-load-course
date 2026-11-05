package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import ru.quipy.common.utils.OngoingWindow
import java.time.Duration

class PaymentAccountMetrics(
    registry: MeterRegistry,
    accountName: String,
    window: OngoingWindow,
) {
    enum class Outcome(val tag: String) {
        SUCCESS("success"),
        FAIL("fail"),
        TIMEOUT("timeout"),
        ERROR("error"),
        DEADLINE_SKIPPED("deadline_skipped"),
    }

    private val windowWaitTimer = percentileTimer("payment.semaphore.wait", registry, "account", accountName)

    private val rateLimiterWaitTimer = percentileTimer("payment.ratelimiter.wait", registry, "account", accountName)

    private val deadlineMargin = percentileTimer(
        "payment.deadline.margin", registry, "account", accountName,
        percentiles = doubleArrayOf(0.01, 0.05, 0.5),
    )

    private val deadlineOverrun = percentileTimer("payment.deadline.overrun", registry, "account", accountName)

    private val outcomes = Outcome.values().associateWith {
        Counter.builder("payment.outcome")
            .tag("account", accountName)
            .tag("result", it.tag)
            .register(registry)
    }

    init {
        Gauge.builder("payment.external.inflight", window) { it.inFlight().toDouble() }
            .tag("account", accountName)
            .register(registry)

        Gauge.builder("payment.external.waiting", window) { it.awaitingQueueSize().toDouble() }
            .tag("account", accountName)
            .register(registry)
    }

    fun recordWindowWait(duration: Duration) = windowWaitTimer.record(duration)

    fun recordRateLimiterWait(duration: Duration) = rateLimiterWaitTimer.record(duration)

    fun recordFinished(outcome: Outcome, marginToDeadline: Duration) {
        outcomes.getValue(outcome).increment()
        if (marginToDeadline.isNegative) deadlineOverrun.record(marginToDeadline.negated())
        else deadlineMargin.record(marginToDeadline)
    }
}
