package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.DeadlineAwareLeakyBucket
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.metrics.PaymentAccountMetrics
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread


// Совет: время всегда храните в Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private val WINDOW_MARGIN = Duration.ofMillis(10)

        private val ADMISSION_MARGIN = Duration.ofMillis(50)
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

    private class Payment(val id: UUID, val amount: Int, val startedAt: Long, val deadline: Long, val enqueuedAt: Long)

    private val queue = LinkedBlockingQueue<Payment>()
    private val senders = Executors.newCachedThreadPool(NamedThreadFactory("payment-sender-$accountName"))

    private val client = OkHttpClient.Builder()
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    private val metrics = PaymentAccountMetrics(meterRegistry, accountName, rateLimitPerSec, bucket, ongoingWindow)

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
        queue.put(Payment(paymentId, amount, paymentStartedAt, deadline, enqueuedAt = now()))
    }

    private fun dispatchNext() {
        val payment = queue.take()
        ongoingWindow.acquire()
        val sentAt = bucket.awaitSlot()
        metrics.recordQueueWait(Duration.ofMillis(sentAt - payment.enqueuedAt))

        senders.execute {
            try {
                submitPayment(payment, sentAt)
            } finally {
                ongoingWindow.release()
            }
        }
    }

    private fun submitPayment(payment: Payment, sentAt: Long) {
        val transactionId = UUID.randomUUID()
        val skipped = sentAt >= payment.deadline
        val (success, reason) = if (skipped) false to "Deadline exceeded." else callProvider(payment, transactionId)

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования
        paymentESService.update(payment.id) {
            it.logSubmission(success = !skipped, transactionId, sentAt, Duration.ofMillis(sentAt - payment.startedAt))
        }

        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
        paymentESService.update(payment.id) {
            it.logProcessing(success, now(), transactionId, reason = reason)
        }
    }

    private fun callProvider(payment: Payment, transactionId: UUID): Pair<Boolean, String?> {
        logger.info("[$accountName] Submit: ${payment.id} , txId: $transactionId")

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=${payment.id}&amount=${payment.amount}")
            post(emptyBody)
        }.build()

        return try {
            client.newCall(request).execute().use { response ->
                val body = mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                logger.info("[$accountName] Payment processed for txId: $transactionId, payment: ${payment.id}, succeeded: ${body.result}, message: ${body.message}")
                body.result to body.message
            }
        } catch (e: SocketTimeoutException) {
            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: ${payment.id}", e)
            false to "Request timeout."
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: ${payment.id}", e)
            false to e.message
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
