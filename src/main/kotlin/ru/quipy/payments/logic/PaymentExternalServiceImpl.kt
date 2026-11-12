package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.DeadlineAwareLeakyBucket
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.metrics.PaymentAccountMetrics
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread


// Совет: время всегда храните в Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    paymentProviderHostPort: String,
    token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        private val WINDOW_MARGIN = Duration.ofMillis(10)

        private val ADMISSION_MARGIN = Duration.ofMillis(50)

        private const val MAX_ATTEMPTS = 3

    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val maxProcessingTime = requestAverageProcessingTime.multipliedBy(2)

    private val bucket = DeadlineAwareLeakyBucket(
        rate = rateLimitPerSec,
        window = Duration.ofSeconds(1) + WINDOW_MARGIN,
        processingTime = maxProcessingTime + ADMISSION_MARGIN,
    )
    private val ongoingWindow = OngoingWindow(parallelRequests)
    private val retryPolicy = PaymentRetryPolicy(MAX_ATTEMPTS, bucket)
    private val lifecycleRecorder = PaymentLifecycleRecorder(paymentESService)
    private val providerClient = PaymentProviderClient(
        hostPort = paymentProviderHostPort,
        serviceName = serviceName,
        token = token,
        accountName = accountName,
    )

    private data class Payment(
        val id: UUID,
        val amount: Int,
        val startedAt: Long,
        val deadline: Long,
        val enqueuedAt: Long,
        val transactionId: UUID,
        val attempt: Int,
    )

    private val queue = LinkedBlockingQueue<Payment>()
    private val senders = Executors.newCachedThreadPool(NamedThreadFactory("payment-sender-$accountName"))

    private val metrics = PaymentAccountMetrics(
        registry = meterRegistry,
        serviceName = serviceName,
        accountName = accountName,
        rateLimitPerSec = rateLimitPerSec,
        bucket = bucket,
        window = ongoingWindow,
    )

    init {
        thread(isDaemon = true, name = "payment-dispatcher-$accountName") {
            while (true) {
                try {
                    dispatchNext()
                } catch (e: Exception) {
                    logger.error("[$accountName] Failed to dispatch payment", e)
                }
            }
        }
    }

    override fun tryReserve(deadline: Long): Long? = bucket.tryReserve(deadline)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        queue.put(
            Payment(
                id = paymentId,
                amount = amount,
                startedAt = paymentStartedAt,
                deadline = deadline,
                enqueuedAt = now(),
                transactionId = UUID.randomUUID(),
                attempt = 1,
            )
        )
    }

    private fun dispatchNext() {
        val payment = queue.take()
        ongoingWindow.acquire()
        val sentAt = bucket.awaitSlot()
        metrics.recordQueueWait(Duration.ofMillis(sentAt - payment.enqueuedAt))

        senders.execute { executePayment(payment, sentAt) }
    }

    private fun executePayment(payment: Payment, sentAt: Long) {
        try {
            submitPayment(payment, sentAt)
        } catch (e: RetryablePaymentException) {
            metrics.recordRetryableAttemptFailure()
            handleRetryableFailure(payment, e)
        } catch (e: PermanentPaymentException) {
            metrics.recordPermanentAttemptFailure()
            completePayment(payment, success = false, reason = e.message)
        } finally {
            ongoingWindow.release()
        }
    }

    private fun submitPayment(payment: Payment, sentAt: Long) {
        val skipped = sentAt >= payment.deadline
        lifecycleRecorder.recordAttemptStarted(
            paymentId = payment.id,
            transactionId = payment.transactionId,
            sentAt = sentAt,
            startedAt = payment.startedAt,
            success = !skipped,
        )

        if (skipped) {
            metrics.recordAttemptRejectedByDeadline()
            completePayment(payment, success = false, reason = "Deadline exceeded.")
            return
        }

        providerClient.process(payment.id, payment.amount, payment.transactionId, payment.deadline)
        metrics.recordSuccessfulAttempt()
        completePayment(payment, success = true)
    }

    private fun handleRetryableFailure(payment: Payment, error: RetryablePaymentException) {
        when (retryPolicy.reserveRetry(payment.attempt, payment.deadline)) {
            RetryDecision.RETRY_RESERVED -> {
                metrics.recordRetryEnqueued()
                logger.info(
                    "[$accountName] Retrying payment ${payment.id}, txId: ${payment.transactionId}, " +
                        "next attempt: ${payment.attempt + 1}"
                )
                queue.put(payment.copy(attempt = payment.attempt + 1, enqueuedAt = now()))
            }

            RetryDecision.REJECTED_DEADLINE -> {
                metrics.recordRetryRejectedByDeadline()
                completePayment(payment, success = false, reason = "Retry cannot finish before deadline.")
            }

            RetryDecision.REJECTED_ATTEMPT_LIMIT -> {
                metrics.recordRetryRejectedByAttemptLimit()
                completePayment(payment, success = false, reason = error.message)
            }
        }
    }

    private fun completePayment(payment: Payment, success: Boolean, reason: String? = null) {
        lifecycleRecorder.recordPaymentCompleted(payment.id, payment.transactionId, success, reason)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
