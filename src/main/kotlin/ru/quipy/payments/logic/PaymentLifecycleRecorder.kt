package ru.quipy.payments.logic

import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID

internal class PaymentLifecycleRecorder(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
) {
    fun recordAttemptStarted(
        paymentId: UUID,
        transactionId: UUID,
        sentAt: Long,
        startedAt: Long,
        success: Boolean,
    ) {
        paymentESService.update(paymentId) {
            it.logSubmission(
                success,
                transactionId,
                sentAt,
                Duration.ofMillis(sentAt - startedAt),
            )
        }
    }

    fun recordPaymentCompleted(
        paymentId: UUID,
        transactionId: UUID,
        success: Boolean,
        reason: String? = null,
    ) {
        paymentESService.update(paymentId) {
            it.logProcessing(
                success = success,
                processedAt = now(),
                transactionId = transactionId,
                reason = reason,
            )
        }
    }
}
