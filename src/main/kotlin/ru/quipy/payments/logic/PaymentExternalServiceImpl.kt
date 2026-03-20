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

    private val hedgeScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { runnable ->
        val thread = Thread(runnable)
        thread.isDaemon = true
        thread.name = "payment-hedge-$accountName"
        thread
    }

    val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val estimatedProcessingTimeMs = min(
        (requestAverageProcessingTime.toMillis() * 0.65).toLong(),
        1_000L
    )
    private val maxAttempts = 2
    private val maxDelayMs = 5L
    private val delayBaseMs = 1L
    private val hedgeDelayMs = 50L
    private val maxHedgeRequests = 6
    private val hedgeTimeoutReserveMs = 50L
    private val hedgedRequestEnabled = requestAverageProcessingTime.toMillis() >= 1_000L

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
        if (hedgedRequestEnabled && attempt == 1) {
            performHedgedRequest(paymentId, amount, transactionId, deadline)
            return
        }

        if (now() + estimatedProcessingTimeMs > deadline || attempt > maxAttempts) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded or max attempts reached")
            }
            return
        }

        if (!slidingWindowRateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit exceed")
            }
            return
        }

        val acquired = semaphore.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)
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
                if (attempt > 1) {
                    paymentRetryCounter.increment()
                }
                releasePermitOnce(permitReleased)
                val currentDelay = exponentialBackoffDelay(attempt)
                val remainingTime = deadline - now()
                val sleepTime = min(currentDelay, remainingTime - 10)
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime)
                    performRequestWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                }
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
            if (attempt > 1) {
                paymentRetryCounter.increment()
            }
            releasePermitOnce(permitReleased)
            val currentDelay = exponentialBackoffDelay(attempt)
            val remainingTime = deadline - now()
            val sleepTime = min(currentDelay, remainingTime - 10)
            if (sleepTime > 0) {
                Thread.sleep(sleepTime)
                performRequestWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
            }
            null
        }
    }

    private fun performHedgedRequest(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ) {
        if (now() + estimatedProcessingTimeMs > deadline) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before hedged request start")
            }
            return
        }

        if (!slidingWindowRateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit exceed")
            }
            return
        }

        val acquired = semaphore.tryAcquire(deadline - now(), TimeUnit.MILLISECONDS)
        if (!acquired) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout")
            }
            return
        }

        val completed = AtomicBoolean(false)
        val primaryReleased = AtomicBoolean(false)
        val request = buildPaymentRequest(paymentId, amount, transactionId)

        val remainingToDeadline = max(1L, deadline - now())
        hedgeScheduler.schedule(
            {
                if (completed.compareAndSet(false, true)) {
                    paymentErrorCounter.increment()
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                    }
                }
            },
            remainingToDeadline,
            TimeUnit.MILLISECONDS
        )

        sendHedgedAttempt(request, paymentId, transactionId, completed, primaryReleased)

        for (hedgeIndex in 1 until maxHedgeRequests) {
            val hedgeDelay = hedgeIndex * hedgeDelayMs
            if (hedgeDelay >= max(0L, deadline - now() - hedgeTimeoutReserveMs)) {
                break
            }

            hedgeScheduler.schedule(
                {
                    if (!completed.get() && slidingWindowRateLimiter.tick() && semaphore.tryAcquire()) {
                        paymentRetryCounter.increment()
                        val hedgeReleased = AtomicBoolean(false)
                        sendHedgedAttempt(request, paymentId, transactionId, completed, hedgeReleased)
                    }
                },
                hedgeDelay,
                TimeUnit.MILLISECONDS
            )
        }
    }

    private fun sendHedgedAttempt(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        completed: AtomicBoolean,
        released: AtomicBoolean
    ) {
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Hedged payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            if (body.result && completed.compareAndSet(false, true)) {
                paymentSuccessCounter.increment()
                paymentESService.update(paymentId) {
                    it.logProcessing(true, now(), transactionId, reason = body.message)
                }
            }

            releasePermitOnce(released)
        }.exceptionally { ex ->
            logger.error("[$accountName] Hedged payment failed for txId: $transactionId, payment: $paymentId", ex)
            releasePermitOnce(released)
            null
        }
    }

    private fun buildPaymentRequest(paymentId: UUID, amount: Int, transactionId: UUID): HttpRequest {
        return HttpRequest.newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()
    }

    private fun exponentialBackoffDelay(attempt: Int): Long {
        return minOf((delayBaseMs * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelayMs)
    }

    private fun releasePermitOnce(released: AtomicBoolean) {
        if (released.compareAndSet(false, true)) {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
