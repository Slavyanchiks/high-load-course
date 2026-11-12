package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.*


@Service
class PaymentSystemImpl(
    paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    private val account = paymentAccounts.single()

    override fun tryReserve(deadline: Long) = account.tryReserve(deadline)

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) =
        account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
}
