package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.suspendCancellableCoroutine

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val limiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofMillis(
            rateLimitPerSec * 1000 / (parallelRequests * 1000 / requestAverageProcessingTime.toMillis())
        )
    )

    // корутинный семафор
    private val semaphore = Semaphore(parallelRequests)

    override suspend fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        // здесь пока блокирующий лимитер, можно будет сделать suspend-версию
        limiter.tickBlocking()

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(
                success = true,
                transactionId,
                now(),
                Duration.ofMillis(now() - paymentStartedAt)
            )
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        semaphore.withPermit {
            try {
                val request = Request.Builder()
                    .url(
                        "http://$paymentProviderHostPort/external/process" +
                                "?serviceName=$serviceName" +
                                "&token=$token" +
                                "&accountName=$accountName" +
                                "&transactionId=$transactionId" +
                                "&paymentId=$paymentId" +
                                "&amount=$amount"
                    )
                    .post(emptyBody)
                    .build()

                val response = withContext(Dispatchers.IO) {
                    client.newCall(request).await()
                }

                response.use {
                    val body = try {
                        mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, " +
                                    "payment: $paymentId, result code: ${it.code}, reason: ${it.body?.string()}"
                        )
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message
                        )
                    }

                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, " +
                                "payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()

suspend fun Call.await(): Response = suspendCancellableCoroutine { cont ->
    enqueue(object : Callback {
        override fun onFailure(call: Call, e: java.io.IOException) {
            if (cont.isActive) cont.resumeWithException(e)
        }

        override fun onResponse(call: Call, response: Response) {
            if (cont.isActive) cont.resume(response)
        }
    })

    cont.invokeOnCancellation { cancel() }
}
