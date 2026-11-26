package ru.quipy.payments.logic

import java.time.Duration
import java.util.*

interface PaymentService {
    fun tryReserve(deadline: Long): Long?

    /**
     * Отправить запрос на оплату во внешний сервис.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)
}

/**
 * Адаптер внешней платежной системы. Представляет аккаунт во внешней системе.
 *
 * !!! При необходимости интерфейс можно расширять дополнительными методами. !!!

 */
interface PaymentExternalSystemAdapter {
    fun tryReserve(deadline: Long): Long?

    fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)

    fun name(): String

    fun price(): Int

    fun isEnabled(): Boolean
}

/**
 * Свойства аккаунтов провайдера оплаты.
 */
data class PaymentAccountProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val price: Int,
    val averageProcessingTime: Duration = Duration.ofSeconds(11),
    val enabled: Boolean,
)

/**
 * Ответ внешнего сервиса.
 */
class ExternalSysResponse(
    val transactionId: String,
    val paymentId: String,
    val result: Boolean,
    val message: String? = null,
)
