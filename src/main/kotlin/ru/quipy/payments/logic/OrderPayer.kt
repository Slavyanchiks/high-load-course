package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.metrics.PaymentAdmissionMetrics
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Service
class OrderPayer(meterRegistry: MeterRegistry) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private const val PAYMENT_WORKER_THREADS = 330

        private const val PAYMENT_QUEUE_CAPACITY = 8_000

        private val WORKER_KEEP_ALIVE: Duration = Duration.ZERO
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        PAYMENT_WORKER_THREADS,
        PAYMENT_WORKER_THREADS,
        WORKER_KEEP_ALIVE.toMillis(),
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(PAYMENT_QUEUE_CAPACITY),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val paymentsInProgress = AtomicInteger()

    private val metrics = PaymentAdmissionMetrics(meterRegistry, paymentsInProgress, paymentExecutor)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadlineEpochMs: Long): Long {
        val createdAtEpochMs = nowEpochMs()

        val estimatedFinishEpochMs = paymentService.estimatedFinishEpochMs(paymentsInProgress.getAndIncrement())
        val deadlineBudget = Duration.ofMillis(deadlineEpochMs - createdAtEpochMs)
        val estimatedLatency = Duration.ofMillis(estimatedFinishEpochMs - createdAtEpochMs)

        if (estimatedFinishEpochMs > deadlineEpochMs) {
            paymentsInProgress.decrementAndGet()
            val retryAfterDelay = Duration.ofMillis(estimatedFinishEpochMs - deadlineEpochMs)
            safeRecordMetrics { metrics.recordRejected(deadlineBudget, estimatedLatency, retryAfterDelay) }
            throw TooManyRequestsException(retryAfterEpochMs = createdAtEpochMs + retryAfterDelay.toMillis())
        }
        safeRecordMetrics { metrics.recordAccepted(deadlineBudget, estimatedLatency) }

        paymentExecutor.submit {
            try {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                paymentService.submitPaymentRequest(paymentId, amount, createdAtEpochMs, deadlineEpochMs)
            } finally {
                paymentsInProgress.decrementAndGet()
            }
        }
        return createdAtEpochMs
    }

    private inline fun safeRecordMetrics(record: () -> Unit) {
        try {
            record()
        } catch (e: Exception) {
            logger.error("Failed to record admission metrics", e)
        }
    }
}

/**
 * Payment can't be processed before its deadline. [retryAfterEpochMs] is epoch millis when it makes sense to retry.
 */
class TooManyRequestsException(val retryAfterEpochMs: Long) : RuntimeException("Payment can't be processed in time")
