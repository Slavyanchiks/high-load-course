package ru.quipy.payments.logic

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.SocketTimeoutException
import java.util.UUID
import java.util.concurrent.TimeUnit

internal class PaymentProviderClient(
    private val hostPort: String,
    private val serviceName: String,
    private val token: String,
    private val accountName: String,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentProviderClient::class.java)
        private val emptyBody: RequestBody = ByteArray(0).toRequestBody(null)
        private val mapper = ObjectMapper().registerKotlinModule()

        private const val TEMPORARY_ERROR = "Temporary error"
        private val RETRYABLE_HTTP_STATUSES = setOf(429, 500, 502, 503, 504)
    }

    private val client = OkHttpClient.Builder()
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    fun process(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
    ): ExternalSysResponse {
        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

        val request = Request.Builder()
            .url(
                "http://$hostPort/external/process" +
                    "?serviceName=$serviceName" +
                    "&token=$token" +
                    "&accountName=$accountName" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount"
            )
            .post(emptyBody)
            .build()

        return try {
            executeHttpRequest(request, deadline).use { response ->
                val responseBody = response.body?.string()
                if (!response.isSuccessful) {
                    val message = "HTTP ${response.code}: $responseBody"
                    if (response.code in RETRYABLE_HTTP_STATUSES) {
                        throw RetryablePaymentException(message)
                    }
                    throw PermanentPaymentException(message)
                }

                val body = mapper.readValue(
                    responseBody ?: throw PermanentPaymentException("Empty payment provider response."),
                    ExternalSysResponse::class.java,
                )
                logger.info(
                    "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                        "succeeded: ${body.result}, message: ${body.message}"
                )

                if (!body.result) {
                    if (body.message == TEMPORARY_ERROR) {
                        throw RetryablePaymentException(body.message)
                    }
                    throw PermanentPaymentException(body.message ?: "Payment rejected.")
                }

                body
            }
        } catch (e: JsonProcessingException) {
            throw PermanentPaymentException("Invalid payment provider response.", e)
        } catch (e: SocketTimeoutException) {
            throw RetryablePaymentException("Request timeout.", e)
        } catch (e: IOException) {
            throw RetryablePaymentException(e.message ?: "Transport error.", e)
        }
    }

    private fun executeHttpRequest(request: Request, deadline: Long): Response {
        val call = client.newCall(request)
        call.timeout().timeout((deadline - now()).coerceAtLeast(1), TimeUnit.MILLISECONDS)

        return call.execute()
    }
}
