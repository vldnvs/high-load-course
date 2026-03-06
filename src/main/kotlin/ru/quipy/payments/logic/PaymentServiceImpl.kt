package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    @Value("\${payment.dispatch.core-pool-size:64}")
    private var dispatchCorePoolSize: Int = 64

    @Value("\${payment.dispatch.max-pool-size:512}")
    private var dispatchMaxPoolSize: Int = 512

    @Value("\${payment.dispatch.queue-capacity:100000}")
    private var dispatchQueueCapacity: Int = 100000

    private val dispatchExecutor by lazy {
        ThreadPoolExecutor(
            dispatchCorePoolSize,
            dispatchMaxPoolSize,
            60L,
            TimeUnit.SECONDS,
            LinkedBlockingQueue(dispatchQueueCapacity),
            NamedThreadFactory("payment-dispatch-executor"),
            CallerBlockingRejectedExecutionHandler()
        )
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            dispatchExecutor.submit {
                account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            }
        }
    }
}
