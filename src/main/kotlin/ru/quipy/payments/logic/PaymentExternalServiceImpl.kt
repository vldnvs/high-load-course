package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val semaphore = java.util.concurrent.Semaphore(properties.parallelRequests)
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val paymentSuccessCounter = Counter.builder("payment_requests_processed_total")
        .tag("outcome", "success")
        .register(Metrics.globalRegistry)

    private val paymentErrorCounter = Counter.builder("payment_requests_processed_total")
        .tag("outcome", "error")
        .register(Metrics.globalRegistry)

    private val requestLatency = Timer.builder("payment_request_latency_seconds")
        .tags("adapter", "payment")
        .publishPercentiles(0.5, 0.85, 0.9, 0.95, 0.99)
        .register(Metrics.globalRegistry)

    private val paymentRetryCounter = Counter.builder("payment_requests_retries_total")
        .tag("adapter", "payment")
        .register(Metrics.globalRegistry)

    private val clientExecutorThreads = properties.parallelRequests
        .coerceAtMost(512)
        .coerceAtLeast(128)

    private val client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(clientExecutorThreads))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val retryScheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(
        4,
        { runnable ->
            val thread = Thread(runnable)
            thread.isDaemon = true
            thread.name = "payment-retry-$accountName"
            thread
        }
    )

    val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val maxAttempts = 100
    private val maxDelayMs = 50L
    private val delayBaseMs = requestAverageProcessingTime.toMillis().coerceIn(5L, 10L)
    private val schedulingSlackMs = 20L
    private val retrySafetyMarginMs = max(50L, requestAverageProcessingTime.toMillis() * 2)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()


        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
        performRequestWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, 1)
    }

    private fun performRequestWithRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        if (!canStartAttempt(deadline, attempt)) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded or max attempts reached")
            }
            return
        }

        val timeLeftForAcquire = timeLeftForAcquire(deadline)
        if (timeLeftForAcquire <= 0) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before request scheduling")
            }
            return
        }

        if (!slidingWindowRateLimiter.tickBlocking(Duration.ofMillis(timeLeftForAcquire))) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit exceed")
            }
            return
        }

        val timeToBlock = timeLeftForAcquire(deadline)
        if (timeToBlock <= 0) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before semaphore acquire")
            }
            return
        }

        val acquired = semaphore.tryAcquire(timeToBlock, TimeUnit.MILLISECONDS)
        if (!acquired) {
            logger.warn("[$accountName] Timeout acquiring semaphore for payment $paymentId")
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout")
            }
            return
        }

        val request = HttpRequest.newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val startTime = now()
        val permitReleased = AtomicBoolean(false)
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }
            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
            requestLatency.record(now() - startTime, TimeUnit.MILLISECONDS)

            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }

            if (body.result) {
                paymentSuccessCounter.increment()
                releasePermitOnce(permitReleased)
            } else {
                paymentErrorCounter.increment()
                releasePermitOnce(permitReleased)
                scheduleRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt, body.message)
            }

        }.exceptionally { ex ->
            when (ex) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", ex)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }
                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", ex)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = ex.message)
                    }
                }
            }
            releasePermitOnce(permitReleased)
            scheduleRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt, ex.message)
            null
        }
    }

    private fun exponentialBackoffDelay(attempt: Int): Long {
        return minOf((delayBaseMs * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelayMs)
    }

    private fun canStartAttempt(deadline: Long, attempt: Int): Boolean {
        if (attempt > maxAttempts) {
            return false
        }

        return now() + requestAverageProcessingTime.toMillis() + retrySafetyMarginMs < deadline
    }

    private fun timeLeftForAcquire(deadline: Long): Long {
        return deadline - now() - retrySafetyMarginMs
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int,
        reason: String?
    ) {
        val nextAttempt = attempt + 1
        if (!canStartAttempt(deadline, nextAttempt)) {
            return
        }

        if (attempt >= 1) {
            paymentRetryCounter.increment()
        }

        val currentDelay = exponentialBackoffDelay(attempt)
        val remainingTime = deadline - now() - retrySafetyMarginMs
        val delay = min(currentDelay, remainingTime - schedulingSlackMs)
        if (delay <= 0) {
            performRequestWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, nextAttempt)
            return
        }

        logger.debug("[$accountName] Scheduling retry for payment $paymentId in ${delay}ms, reason=$reason")
        retryScheduler.schedule(
            { performRequestWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, nextAttempt) },
            delay,
            TimeUnit.MILLISECONDS
        )
    }

    private fun releasePermitOnce(released: AtomicBoolean) {
        if (released.compareAndSet(false, true)) {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun rateLimitPerSec() = properties.rateLimitPerSec
}

public fun now() = System.currentTimeMillis()
