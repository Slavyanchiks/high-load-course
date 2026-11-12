package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import ru.quipy.common.utils.DeadlineAwareLeakyBucket
import ru.quipy.common.utils.OngoingWindow
import java.time.Duration

class PaymentAccountMetrics(
    registry: MeterRegistry,
    accountName: String,
    rateLimitPerSec: Int,
    bucket: DeadlineAwareLeakyBucket,
    window: OngoingWindow,
) {
    private val queueWaitTimer = Timer.builder("payment.queue.wait")
        .tag("account", accountName)
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry)

    init {
        Gauge.builder("payment.external.inflight", window) { it.inFlight().toDouble() }
            .tag("account", accountName)
            .register(registry)

        Gauge.builder("payment.external.waiting", bucket) { it.waiting().toDouble() }
            .tag("account", accountName)
            .register(registry)

        Gauge.builder("payment.external.rate.limit") { rateLimitPerSec.toDouble() }
            .tag("account", accountName)
            .register(registry)
    }

    fun recordQueueWait(duration: Duration) = queueWaitTimer.record(duration)
}
