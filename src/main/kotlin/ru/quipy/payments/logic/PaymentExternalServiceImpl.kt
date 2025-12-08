package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.pow

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

    private val semaphore = Semaphore(properties.parallelRequests)

    private val requestLatency = Timer.builder("payment_request_latency_seconds")
        .tags("adapter", "payment")
        .publishPercentiles(0.5, 0.85, 0.9, 0.95, 0.99)
        .register(Metrics.globalRegistry)

    private val paymentRetryCounter = Counter.builder("payment_requests_retries_total")
        .tag("adapter", "payment")
        .register(Metrics.globalRegistry)

    private val paymentSuccessCounter = Counter.builder("payment_requests_processed_total")
        .tag("outcome", "success")
        .register(Metrics.globalRegistry)

    private val paymentErrorCounter = Counter.builder("payment_requests_processed_total")
        .tag("outcome", "error")
        .register(Metrics.globalRegistry)

    private val client = OkHttpClient.Builder()
        .connectTimeout(2500, TimeUnit.MILLISECONDS)
        .readTimeout(2000, TimeUnit.MILLISECONDS)
        .writeTimeout(2000, TimeUnit.MILLISECONDS)
        .build()

    private val timeoutTime = 1010 * 1.4

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )

    private val maxAttempts = 3

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logSubmission(paymentId, paymentStartedAt, transactionId)

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var attempt = 0

        while (attempt < maxAttempts) {
            attempt++
            if (attempt > 1) paymentRetryCounter.increment()

            if (isDeadlineExpired(deadline)) {
                failWithReason(paymentId, transactionId, "Deadline expired after $attempt attempts)")
                return
            }

            val success = executePaymentAttempt(
                paymentId = paymentId,
                transactionId = transactionId,
                amount = amount,
                deadline = deadline
            )

            if (success) return
            if (attempt == maxAttempts) {
                failWithReason(paymentId, transactionId, "All $maxAttempts attempts failed")
                return
            }

            val delay = exponentialBackoffDelay(attempt)
            if (delay <= 0) {
                failWithReason(paymentId, transactionId, "No time for retry delay")
                return
            }

            Thread.sleep(delay)
        }
    }

    private fun executePaymentAttempt(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        deadline: Long
    ): Boolean {
        return try {
            if (!tryConsumeRateLimit(deadline)) {
                failWithReason(paymentId, transactionId, "Rate limit exceed")
                return false
            }

            val request = buildRequest(paymentId, transactionId, amount)

            val acquired = semaphore.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)
            if (!acquired) {
                failWithReason(paymentId, transactionId, "Semaphore timeout")
                return false
            }

            val success = executeHttpCall(paymentId, transactionId, request, deadline)
            semaphore.release()
            success
        } catch (e: SocketTimeoutException) {
            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
            false
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            failWithReason(paymentId, transactionId, "Non-retriable exception: ${e.message}")
            false
        }
    }

    private fun executeHttpCall(
        paymentId: UUID,
        transactionId: UUID,
        request: Request,
        deadline: Long
    ): Boolean {
        val startTime = now()
        return try {
            val timeoutMs = minOf(timeoutTime.toLong(), deadline - startTime)

            val call = client.newCall(request)
            call.timeout().timeout(timeoutMs, TimeUnit.MILLISECONDS)

            call.execute().use { response ->
                val body = parseExternalResponse(response)

                logger.warn(
                    "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                            "succeeded: ${body.result}, message: ${body.message}"
                )

                if (body.result) {
                    paymentSuccessCounter.increment()
                    logProcessingSuccess(paymentId, transactionId, body.message)
                    true
                } else {
                    handleErrorResponse(paymentId, transactionId, response.code, body.message)
                    false
                }
            }
        } finally {
            requestLatency.record(now() - startTime, TimeUnit.MILLISECONDS)
        }
    }

    private fun handleErrorResponse(paymentId: UUID, transactionId: UUID, code: Int, message: String?) {
        paymentErrorCounter.increment()

        val retriable = code == 429 || code in 500..504
        if (!retriable) {
            logProcessingFailure(paymentId, transactionId, message)
        }
    }

    private fun parseExternalResponse(response: okhttp3.Response): ExternalSysResponse {
        return try {
            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
        } catch (e: Exception) {
            logger.error(
                "[$accountName] [ERROR] Payment processed with error code: ${response.code}, reason: ${e.message}"
            )
            ExternalSysResponse("", "", false, e.message)
        }
    }

    private fun buildRequest(paymentId: UUID, transactionId: UUID, amount: Int): Request =
        Request.Builder()
            .url(
                "http://$paymentProviderHostPort/external/process" +
                        "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                        "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            )
            .post(emptyBody)
            .build()

    private fun tryConsumeRateLimit(deadline: Long): Boolean {
        val remaining = deadline - now()
        return slidingWindowRateLimiter.tickBlocking(Duration.ofMillis(remaining))
    }

    private fun isDeadlineExpired(deadline: Long) =
        now() > deadline

    private fun logSubmission(paymentId: UUID, startedAt: Long, txId: UUID) {
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, txId, now(), Duration.ofMillis(now() - startedAt))
        }
    }

    private fun logProcessingSuccess(paymentId: UUID, txId: UUID, message: String?) {
        paymentESService.update(paymentId) {
            it.logProcessing(true, now(), txId, reason = message)
        }
    }

    private fun logProcessingFailure(paymentId: UUID, txId: UUID, message: String?) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), txId, reason = message)
        }
    }

    private fun failWithReason(paymentId: UUID, txId: UUID, reason: String) {
        paymentErrorCounter.increment()
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), txId, reason)
        }
    }

    private fun exponentialBackoffDelay(attempt: Int): Long {
        val maxDelayMs = 2000L
        val delayBaseMs = 200L

        return minOf((delayBaseMs * 2.0.pow(attempt - 1)).toLong(), maxDelayMs)
    }


    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
