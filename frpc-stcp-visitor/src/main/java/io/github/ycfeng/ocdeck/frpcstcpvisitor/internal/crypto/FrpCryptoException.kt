package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpSafeIOException
import java.util.concurrent.CancellationException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

internal enum class FrpCryptoFailure(val description: String) {
    INVALID_KEY("invalid key"),
    INVALID_NONCE("invalid nonce"),
    TRUNCATED_NONCE("truncated nonce"),
    TRUNCATED_HEADER("truncated record header"),
    TRUNCATED_RECORD("truncated record"),
    INVALID_LENGTH("invalid record length"),
    INTEGRITY_FAILURE("record integrity failure"),
    NONCE_EXHAUSTED("record nonce exhausted"),
    RECORD_LIMIT("record limit exceeded"),
    UNSUPPORTED_ALGORITHM("unsupported algorithm"),
    IO_FAILURE("stream io failure"),
    WRITE_FAILED("stream write failure"),
}

internal class FrpCryptoException(
    val failure: FrpCryptoFailure,
) : FrpSafeIOException("frp crypto failure: ${failure.description}")

internal fun mapCryptoStreamFailure(
    failure: Throwable,
    fallback: FrpCryptoFailure,
): Throwable = when (failure) {
    is Error -> failure
    is CancellationException -> failure
    is FrpSafeIOException -> failure
    is Exception -> FrpCryptoException(fallback)
    else -> failure
}

internal fun combineCryptoFailures(
    first: Throwable?,
    second: Throwable?,
    fallback: FrpCryptoFailure,
): Throwable? {
    val firstMapped = first?.let { mapCryptoStreamFailure(it, fallback) }
    val secondMapped = second?.let { mapCryptoStreamFailure(it, fallback) }
    if (firstMapped == null) {
        return secondMapped
    }
    if (secondMapped == null || firstMapped === secondMapped) {
        return firstMapped
    }
    val primary: Throwable
    val secondary: Throwable
    if (failurePriority(secondMapped) > failurePriority(firstMapped)) {
        primary = secondMapped
        secondary = firstMapped
    } else {
        primary = firstMapped
        secondary = secondMapped
    }
    primary.addSuppressed(
        FrpCryptoSuppressedException(
            category = when (secondary) {
                is Error -> "jvm-error"
                is CancellationException -> "cancellation"
                else -> "expected-exception"
            },
            fallback = fallback,
        ),
    )
    return primary
}

internal enum class FrpStreamCloseRole {
    OWNER,
    WAITER,
    ALREADY_CLOSED,
}

internal class FrpCryptoStreamLifecycle(
    private val operationFailure: FrpCryptoFailure,
) {
    private val stateLock = ReentrantLock()
    private val stateChanged = stateLock.newCondition()
    private var phase = Phase.OPEN
    private var activeOperations = 0
    private var recordedFailure: Throwable? = null

    fun enterOperation() {
        stateLock.withLock {
            recordedFailure?.let { throw it }
            if (phase != Phase.OPEN) {
                throw FrpCryptoException(operationFailure)
            }
            activeOperations++
        }
    }

    fun failureBeforeIo(): Throwable? = stateLock.withLock {
        recordedFailure ?: if (phase == Phase.OPEN) null else FrpCryptoException(operationFailure)
    }

    fun recordOperationFailure(failure: Throwable): Throwable = stateLock.withLock {
        combineCryptoFailures(recordedFailure, failure, operationFailure)
            .also { recordedFailure = it }
            ?: throw AssertionError("missing stream failure")
    }

    fun exitOperation() {
        stateLock.withLock {
            activeOperations--
            if (activeOperations < 0) {
                throw AssertionError("negative active stream operation count")
            }
            if (activeOperations == 0) {
                stateChanged.signalAll()
            }
        }
    }

    fun beginClose(): FrpStreamCloseRole = stateLock.withLock {
        when (phase) {
            Phase.OPEN -> {
                phase = Phase.CLOSING
                FrpStreamCloseRole.OWNER
            }
            Phase.CLOSING -> FrpStreamCloseRole.WAITER
            Phase.CLOSED -> FrpStreamCloseRole.ALREADY_CLOSED
        }
    }

    fun awaitOperations(): CancellationException? {
        var interrupted = Thread.interrupted()
        stateLock.withLock {
            while (activeOperations != 0) {
                try {
                    stateChanged.await()
                } catch (_: InterruptedException) {
                    interrupted = true
                }
            }
        }
        return interruptedCloseFailure(interrupted)
    }

    fun awaitClosed(): CancellationException? {
        var interrupted = Thread.interrupted()
        stateLock.withLock {
            while (phase != Phase.CLOSED) {
                try {
                    stateChanged.await()
                } catch (_: InterruptedException) {
                    interrupted = true
                }
            }
        }
        return interruptedCloseFailure(interrupted)
    }

    fun recordedFailure(): Throwable? = stateLock.withLock { recordedFailure }

    fun isFailed(): Boolean = stateLock.withLock { recordedFailure != null }

    fun finishClose() {
        stateLock.withLock {
            recordedFailure = null
            phase = Phase.CLOSED
            stateChanged.signalAll()
        }
    }

    private fun interruptedCloseFailure(interrupted: Boolean): CancellationException? {
        if (!interrupted) {
            return null
        }
        Thread.currentThread().interrupt()
        return CancellationException("frp stream close interrupted")
    }

    private enum class Phase {
        OPEN,
        CLOSING,
        CLOSED,
    }
}

internal fun <T> runCryptoStreamOperation(
    lifecycle: FrpCryptoStreamLifecycle,
    ioLock: Any,
    operation: () -> T,
): T {
    lifecycle.enterOperation()
    return try {
        synchronized(ioLock) {
            lifecycle.failureBeforeIo()?.let { throw it }
            try {
                operation()
            } catch (failure: Throwable) {
                throw lifecycle.recordOperationFailure(failure)
            }
        }
    } finally {
        lifecycle.exitOperation()
    }
}

private fun failurePriority(failure: Throwable): Int = when (failure) {
    is Error -> 3
    is CancellationException -> 2
    else -> 1
}

private class FrpCryptoSuppressedException(
    category: String,
    fallback: FrpCryptoFailure,
) : FrpSafeIOException("frp crypto suppressed failure: $category during ${fallback.description}")
