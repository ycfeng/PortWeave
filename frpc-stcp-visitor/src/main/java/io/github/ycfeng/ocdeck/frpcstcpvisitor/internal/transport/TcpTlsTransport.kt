package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport

import android.annotation.SuppressLint
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxPhysicalTransport
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.security.MessageDigest
import java.security.SecureRandom
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket
import javax.net.ssl.X509TrustManager
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext

internal class TcpTlsTransportDialer(
    private val config: TcpTlsTransportConfig,
    private val socketFactory: TcpSocketFactory = TcpSocketFactory(::Socket),
    private val routeProvider: TcpDialRouteProvider? = null,
    private val tlsSocketFactory: TlsClientSocketFactory = tlsClientSocketFactory(config),
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val beforeResultHandoff: suspend () -> Unit = {},
) : AutoCloseable {
    private val started = AtomicBoolean(false)
    private val closeController = TransportCloseController()
    private val activeOperations = AtomicInteger(0)

    suspend fun connect(): TcpTlsYamuxTransport {
        if (!started.compareAndSet(false, true)) {
            throw FrpTransportException(FrpTransportFailure.CLOSED)
        }
        if (closeController.isClosed) {
            throw FrpTransportException(FrpTransportFailure.CLOSED)
        }

        return try {
            val established = withTransportBudgetTimeout(
                timeoutMillis = totalEstablishmentTimeoutMillis(),
                timeoutFailure = FrpTransportFailure.CONNECT_TIMEOUT,
            ) {
                connectFirstAvailable(prepareDialPlan())
            }

            if (closeController.isClosed) {
                throw FrpTransportException(FrpTransportFailure.CLOSED)
            }
            val transport = TcpTlsYamuxTransport(
                input = established.input,
                output = established.output,
                closeController = closeController,
                dispatcher = dispatcher,
            )
            beforeResultHandoff()
            handoffFrpOwnedResult(transport, TcpTlsYamuxTransport::close)
        } catch (failure: CancellationException) {
            closeAfterFailure(failure)
        } catch (failure: Error) {
            closeAfterFailure(failure)
        } catch (failure: Exception) {
            closeAfterFailure(failure)
        }
    }

    override fun close() {
        closeController.close()
    }

    fun diagnostics(): TcpTlsDialDiagnostics = TcpTlsDialDiagnostics(
        started = started.get(),
        closed = closeController.isClosed,
        activeOperations = activeOperations.get(),
    )

    override fun toString(): String {
        val diagnostics = diagnostics()
        return "TcpTlsTransportDialer(started=${diagnostics.started}, closed=${diagnostics.closed}, " +
            "activeOperations=${diagnostics.activeOperations})"
    }

    private suspend fun prepareDialPlan(): PreparedTcpDialPlan {
        val provider = routeProvider ?: return PreparedTcpDialPlan(
            route = null,
            addresses = listOf(InetSocketAddress.createUnresolved(config.endpoint.host, config.endpoint.port)),
        )
        return try {
            runBlockingOperation {
                val route = provider.current()
                val resolved = route.resolve(config.endpoint.host)
                if (resolved.isEmpty() || resolved.size > MAXIMUM_DIAL_ADDRESSES) {
                    throw FrpTransportException(FrpTransportFailure.CONNECT_FAILED)
                }
                PreparedTcpDialPlan(
                    route = route,
                    addresses = resolved.map { address -> InetSocketAddress(address, config.endpoint.port) },
                )
            }
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: FrpTransportException) {
            throw failure
        } catch (_: Exception) {
            throw FrpTransportException(FrpTransportFailure.CONNECT_FAILED)
        }
    }

    private suspend fun connectFirstAvailable(plan: PreparedTcpDialPlan): EstablishedTcpDial {
        var lastFailure: FrpTransportException? = null
        val attemptTimeoutMillis = (config.connectTimeoutMillis / plan.addresses.size).coerceAtLeast(1L)
        for (address in plan.addresses) {
            currentCoroutineContext().ensureActive()
            val socket = createSocket(plan.route)
            var tlsSocket: TlsClientSocket? = null
            try {
                configureSocket(socket)
                connectSocket(socket, address, attemptTimeoutMillis)
                if (config.tlsEnabled) {
                    tlsSocket = createTlsSocket(socket)
                    handshake(socket, tlsSocket)
                    return EstablishedTcpDial(
                        input = tlsSocket.getInputStreamSafely(),
                        output = tlsSocket.getOutputStreamSafely(),
                    )
                }
                return EstablishedTcpDial(
                    input = socket.getInputStreamSafely(),
                    output = socket.getOutputStreamSafely(),
                )
            } catch (failure: CancellationException) {
                throw failure
            } catch (failure: Error) {
                throw failure
            } catch (failure: FrpTransportException) {
                closeController.releaseCandidate(socket, tlsSocket)
                if (failure.failure == FrpTransportFailure.CLOSED) {
                    throw FrpTransportException(FrpTransportFailure.CLOSED)
                }
                if (closeController.isClosed) throw failure
                lastFailure = failure
            }
        }
        throw lastFailure ?: FrpTransportException(FrpTransportFailure.CONNECT_FAILED)
    }

    private fun totalEstablishmentTimeoutMillis(): Long =
        if (config.tlsEnabled) {
            if (config.tlsHandshakeTimeoutMillis > Long.MAX_VALUE - config.connectTimeoutMillis) {
                Long.MAX_VALUE
            } else {
                config.connectTimeoutMillis + config.tlsHandshakeTimeoutMillis
            }
        } else {
            config.connectTimeoutMillis
        }

    private suspend fun createSocket(route: TcpDialRoute?): Socket = try {
        runBlockingOperation {
            (route?.createSocket() ?: socketFactory.create()).also(closeController::attachSocket)
        }
    } catch (failure: CancellationException) {
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (failure: FrpTransportException) {
        throw failure
    } catch (_: Exception) {
        throw FrpTransportException(FrpTransportFailure.CONNECT_FAILED)
    }

    private fun configureSocket(socket: Socket) {
        try {
            socket.tcpNoDelay = true
            socket.keepAlive = true
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            throw FrpTransportException(FrpTransportFailure.CONNECT_FAILED)
        }
    }

    private suspend fun connectSocket(socket: Socket, address: InetSocketAddress, timeoutMillis: Long) {
        try {
            withTransportBudgetTimeout(
                timeoutMillis = timeoutMillis,
                timeoutFailure = FrpTransportFailure.CONNECT_TIMEOUT,
            ) {
                runBlockingOperation(
                    budgetTimeoutCleanup = { closeController.releaseCandidate(socket, null) },
                ) {
                    val connectAddress = if (address.isUnresolved) {
                        InetSocketAddress(address.hostString, address.port)
                    } else {
                        address
                    }
                    socket.connect(connectAddress, timeoutMillis.coerceAtMost(Int.MAX_VALUE.toLong()).toInt())
                }
            }
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: SocketTimeoutException) {
            throw FrpTransportException(FrpTransportFailure.CONNECT_TIMEOUT)
        } catch (failure: FrpTransportException) {
            throw failure
        } catch (_: Exception) {
            val externallyClosed = closeController.isClosed
            throw FrpTransportException(
                if (externallyClosed) FrpTransportFailure.CLOSED else FrpTransportFailure.CONNECT_FAILED,
            )
        }
    }

    private suspend fun createTlsSocket(socket: Socket): TlsClientSocket = try {
        runBlockingOperation {
            tlsSocketFactory.create(socket, config.endpoint).also(closeController::attachTlsSocket)
        }
    } catch (failure: CancellationException) {
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (failure: FrpTransportException) {
        throw failure
    } catch (_: Exception) {
        throw FrpTransportException(FrpTransportFailure.TLS_SETUP_FAILED)
    }

    private suspend fun handshake(socket: Socket, tlsSocket: TlsClientSocket) {
        try {
            withTransportBudgetTimeout(
                timeoutMillis = config.tlsHandshakeTimeoutMillis,
                timeoutFailure = FrpTransportFailure.TLS_HANDSHAKE_TIMEOUT,
            ) {
                runBlockingOperation(
                    budgetTimeoutCleanup = { closeController.releaseCandidate(socket, tlsSocket) },
                    block = tlsSocket::startHandshake,
                )
            }
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: FrpTransportException) {
            throw failure
        } catch (failure: Exception) {
            val externallyClosed = closeController.isClosed
            throw FrpTransportException(
                if (externallyClosed) FrpTransportFailure.CLOSED else FrpTransportFailure.TLS_HANDSHAKE_FAILED,
                failure,
            )
        }
    }

    private suspend fun <T> runBlockingOperation(
        budgetTimeoutCleanup: (() -> Unit)? = null,
        block: () -> T,
    ): T =
        suspendCancellableCoroutine { continuation ->
            activeOperations.incrementAndGet()
            continuation.invokeOnCancellation { cause ->
                if (budgetTimeoutCleanup != null && cause.hasTransportBudgetCancellation()) {
                    closeAfterBudgetTimeout(budgetTimeoutCleanup, cause)
                } else {
                    closeAfterCancellation(closeController, cause)
                }
            }
            dispatchBlockingOperation(
                dispatcher = dispatcher,
                continuation = continuation,
                onFinished = activeOperations::decrementAndGet,
                block = block,
            )
        }

    private fun closeAfterFailure(failure: Throwable): Nothing {
        var selected = failure
        try {
            closeController.close()
        } catch (closeFailure: CancellationException) {
            selected = preferFatalFailure(selected, closeFailure)
        } catch (closeFailure: Error) {
            selected = preferFatalFailure(selected, closeFailure)
        }
        throw selected
    }

    private companion object {
        const val MAXIMUM_DIAL_ADDRESSES = 16
    }
}

private data class PreparedTcpDialPlan(
    val route: TcpDialRoute?,
    val addresses: List<InetSocketAddress>,
)

private data class EstablishedTcpDial(
    val input: InputStream,
    val output: OutputStream,
)

internal data class TcpTlsDialDiagnostics(
    val started: Boolean,
    val closed: Boolean,
    val activeOperations: Int,
)

internal class TcpTlsYamuxTransport internal constructor(
    private val input: InputStream,
    private val output: OutputStream,
    private val closeController: TransportCloseController,
    private val dispatcher: CoroutineDispatcher,
) : YamuxPhysicalTransport {
    private val activeReads = AtomicInteger(0)
    private val activeWrites = AtomicInteger(0)
    private val readerToken = Any()

    @OptIn(InternalCoroutinesApi::class)
    override suspend fun runReaderLoop(block: suspend () -> Unit) {
        val readerJob = checkNotNull(currentCoroutineContext()[Job])
        val readerContext = DirectReaderContext(readerToken)
        val cancellationHandle = readerJob.invokeOnCompletion(
            onCancelling = true,
            invokeImmediately = true,
        ) { cause ->
            if (cause != null) closeAfterCancellation(closeController, cause)
        }
        try {
            withContext(dispatcher + readerContext) {
                val ownerJob = checkNotNull(currentCoroutineContext()[Job])
                readerContext.ownerJob = ownerJob
                ownerJob.ensureActive()
                block()
            }
        } finally {
            cancellationHandle.dispose()
        }
    }

    override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
        requireRange(destination.size, offset, length)
        if (length == 0) return 0
        val context = currentCoroutineContext()
        val directReader = context[DirectReaderContext]
        val ownerJob = context[Job]
        if (
            directReader?.token === readerToken &&
            directReader.ownerJob === ownerJob &&
            ownerJob != null
        ) {
            return readDirect(destination, offset, length, ownerJob)
        }
        if (closeController.isClosed) throw FrpTransportException(FrpTransportFailure.CLOSED)
        val count = runTransportIo(FrpTransportFailure.READ_FAILED, activeReads) {
            input.read(destination, offset, length)
        }
        if (count == 0 || count < -1 || count > length) {
            throw FrpTransportException(FrpTransportFailure.READ_FAILED)
        }
        return count
    }

    private fun readDirect(destination: ByteArray, offset: Int, length: Int, ownerJob: Job): Int {
        ownerJob.ensureActive()
        if (closeController.isClosed) {
            ownerJob.ensureActive()
            throw FrpTransportException(FrpTransportFailure.CLOSED)
        }
        activeReads.incrementAndGet()
        return try {
            val count = try {
                input.read(destination, offset, length)
            } catch (failure: CancellationException) {
                throw failure
            } catch (failure: Error) {
                throw failure
            } catch (failure: FrpTransportException) {
                ownerJob.ensureActive()
                throw failure
            } catch (_: Exception) {
                ownerJob.ensureActive()
                throw FrpTransportException(
                    if (closeController.isClosed) FrpTransportFailure.CLOSED else FrpTransportFailure.READ_FAILED,
                )
            }
            ownerJob.ensureActive()
            if (count == 0 || count < -1 || count > length) {
                throw FrpTransportException(FrpTransportFailure.READ_FAILED)
            }
            count
        } finally {
            activeReads.decrementAndGet()
        }
    }

    override suspend fun writeFully(source: ByteArray, offset: Int, length: Int) {
        requireRange(source.size, offset, length)
        if (length == 0) return
        if (closeController.isClosed) throw FrpTransportException(FrpTransportFailure.CLOSED)
        runTransportIo(FrpTransportFailure.WRITE_FAILED, activeWrites) {
            output.write(source, offset, length)
        }
    }

    override fun close() {
        closeController.close()
    }

    fun diagnostics(): TcpTlsTransportDiagnostics = TcpTlsTransportDiagnostics(
        closed = closeController.isClosed,
        activeReads = activeReads.get(),
        activeWrites = activeWrites.get(),
    )

    override fun toString(): String {
        val diagnostics = diagnostics()
        return "TcpTlsYamuxTransport(closed=${diagnostics.closed}, activeReads=${diagnostics.activeReads}, " +
            "activeWrites=${diagnostics.activeWrites})"
    }

    private suspend fun <T> runTransportIo(
        fallback: FrpTransportFailure,
        activeOperations: AtomicInteger,
        block: () -> T,
    ): T {
        activeOperations.incrementAndGet()
        return try {
            suspendCancellableCoroutine { continuation ->
                continuation.invokeOnCancellation { cause ->
                    closeAfterCancellation(closeController, cause)
                }
                dispatchBlockingOperation(
                    dispatcher = dispatcher,
                    continuation = continuation,
                    onFinished = activeOperations::decrementAndGet,
                    block = block,
                )
            }
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: FrpTransportException) {
            throw failure
        } catch (_: Exception) {
            throw FrpTransportException(
                if (closeController.isClosed) FrpTransportFailure.CLOSED else fallback,
            )
        }
    }

    private fun requireRange(size: Int, offset: Int, length: Int) {
        require(offset >= 0 && length >= 0 && offset <= size - length)
    }

    private class DirectReaderContext(
        val token: Any,
    ) : AbstractCoroutineContextElement(DirectReaderContext) {
        var ownerJob: Job? = null

        companion object Key : CoroutineContext.Key<DirectReaderContext>
    }
}

internal data class TcpTlsTransportDiagnostics(
    val closed: Boolean,
    val activeReads: Int,
    val activeWrites: Int,
)

internal class TransportCloseController {
    private val stateLock = Any()
    private val closeCompletion = CountDownLatch(1)
    @Volatile
    private var closeState = TransportCloseState.OPEN
    private var socket: Socket? = null
    private var tlsSocket: TlsClientSocket? = null
    private var closeOwner: Thread? = null
    private var closeFailure: Throwable? = null

    val isClosed: Boolean
        get() = closeState != TransportCloseState.OPEN

    fun attachSocket(value: Socket) {
        val accepted = synchronized(stateLock) {
            if (closeState != TransportCloseState.OPEN || socket != null) {
                false
            } else {
                socket = value
                true
            }
        }
        if (!accepted) {
            closeExpected(value)
            throw FrpTransportException(FrpTransportFailure.CLOSED)
        }
    }

    fun releaseSocket(value: Socket) {
        val released = synchronized(stateLock) {
            if (closeState == TransportCloseState.OPEN && socket === value) {
                socket = null
                true
            } else {
                false
            }
        }
        if (released) closeExpected(value)
    }

    fun releaseCandidate(socketValue: Socket, tlsSocketValue: TlsClientSocket?) {
        val resources = synchronized(stateLock) {
            if (closeState != TransportCloseState.OPEN || socket !== socketValue) {
                emptyList()
            } else if (tlsSocketValue != null && tlsSocket !== tlsSocketValue) {
                emptyList()
            } else {
                socket = null
                if (tlsSocketValue != null) tlsSocket = null
                listOfNotNull(socketValue, tlsSocketValue)
            }
        }
        resources.forEach(::closeExpected)
    }

    fun attachTlsSocket(value: TlsClientSocket) {
        val accepted = synchronized(stateLock) {
            if (closeState != TransportCloseState.OPEN || tlsSocket != null) {
                false
            } else {
                tlsSocket = value
                true
            }
        }
        if (!accepted) {
            closeExpected(value)
            throw FrpTransportException(FrpTransportFailure.CLOSED)
        }
    }

    fun close() {
        val currentThread = Thread.currentThread()
        var resources: List<AutoCloseable>? = null
        val waitForOwner = synchronized(stateLock) {
            when (closeState) {
                TransportCloseState.OPEN -> {
                    closeState = TransportCloseState.CLOSING
                    closeOwner = currentThread
                    resources = listOfNotNull(socket, tlsSocket)
                    tlsSocket = null
                    socket = null
                    false
                }

                TransportCloseState.CLOSING -> {
                    if (closeOwner === currentThread) return
                    true
                }

                TransportCloseState.CLOSED -> {
                    closeFailure?.let { throw it }
                    return
                }
            }
        }
        if (waitForOwner) {
            awaitCloseCompletion()
            synchronized(stateLock) { closeFailure?.let { throw it } }
            return
        }

        var fatalFailure: Throwable? = null
        try {
            checkNotNull(resources).forEach { resource ->
                try {
                    closeExpected(resource)
                } catch (failure: CancellationException) {
                    fatalFailure = preferFatalFailure(fatalFailure, failure)
                } catch (failure: Error) {
                    fatalFailure = preferFatalFailure(fatalFailure, failure)
                }
            }
        } finally {
            synchronized(stateLock) {
                closeFailure = fatalFailure
                closeOwner = null
                closeState = TransportCloseState.CLOSED
            }
            closeCompletion.countDown()
        }
        fatalFailure?.let { throw it }
    }

    override fun toString(): String = "TransportCloseController(closed=$isClosed)"

    private fun awaitCloseCompletion() {
        var interrupted = false
        while (true) {
            try {
                closeCompletion.await()
                break
            } catch (_: InterruptedException) {
                interrupted = true
            }
        }
        if (interrupted) Thread.currentThread().interrupt()
    }

    private fun closeExpected(closeable: AutoCloseable) {
        try {
            closeable.close()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            // Closing is best effort; active I/O reports the selected typed failure.
        }
    }

    private enum class TransportCloseState {
        OPEN,
        CLOSING,
        CLOSED,
    }
}

internal suspend fun <T> handoffFrpOwnedResult(value: T, close: (T) -> Unit): T =
    suspendCancellableCoroutine { continuation ->
        continuation.resume(value) { _, rejected, _ ->
            try {
                close(rejected)
            } catch (_: CancellationException) {
                // The enclosing connect cleanup replays the owner's persisted fatal result.
            } catch (_: Error) {
                // The enclosing connect cleanup replays the owner's persisted fatal result.
            } catch (_: Exception) {
                // Ordinary close I/O cannot replace cancellation at the handoff boundary.
            }
        }
    }

private suspend fun <T> withTransportBudgetTimeout(
    timeoutMillis: Long,
    timeoutFailure: FrpTransportFailure,
    block: suspend () -> T,
): T {
    val callerContext = currentCoroutineContext()
    val budgetCancellation = AtomicReference<TransportBudgetCancellation?>()
    return try {
        coroutineScope {
            val budgetOwner = checkNotNull(currentCoroutineContext()[Job])
            val timer = launch {
                delay(timeoutMillis)
                val cancellation = TransportBudgetCancellation()
                budgetCancellation.compareAndSet(null, cancellation)
                budgetOwner.cancel(cancellation)
            }
            try {
                block()
            } finally {
                timer.cancel()
            }
        }
    } catch (failure: CancellationException) {
        if (!callerContext.isActive) callerContext.ensureActive()
        if (budgetCancellation.get() != null) throw FrpTransportException(timeoutFailure)
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (failure: Exception) {
        if (!callerContext.isActive) callerContext.ensureActive()
        if (budgetCancellation.get() != null) throw FrpTransportException(timeoutFailure)
        throw failure
    }
}

private class TransportBudgetCancellation : CancellationException("frp transport budget expired")

private fun Throwable?.hasTransportBudgetCancellation(): Boolean =
    generateSequence(this) { failure -> failure.cause }
        .any { failure -> failure is TransportBudgetCancellation }

@OptIn(InternalCoroutinesApi::class)
private fun <T> dispatchBlockingOperation(
    dispatcher: CoroutineDispatcher,
    continuation: CancellableContinuation<T>,
    onFinished: () -> Unit,
    block: () -> T,
) {
    val completed = AtomicBoolean(false)
    fun complete(result: Result<T>): Boolean {
        if (!completed.compareAndSet(false, true)) return false
        onFinished()
        val token = result.fold(
            onSuccess = { value -> continuation.tryResume(value, null) },
            onFailure = continuation::tryResumeWithException,
        )
        if (token == null) return false
        continuation.completeResume(token)
        return true
    }

    val taskStarted = AtomicBoolean(false)
    val task = Runnable {
        taskStarted.set(true)
        try {
            complete(Result.success(block()))
        } catch (failure: CancellationException) {
            complete(Result.failure(failure))
        } catch (failure: Error) {
            if (!complete(Result.failure(failure))) throw failure
        } catch (failure: Exception) {
            complete(Result.failure(failure))
        }
    }
    if (!dispatcher.isDispatchNeeded(continuation.context)) {
        task.run()
        return
    }
    try {
        dispatcher.dispatch(continuation.context, task)
    } catch (failure: CancellationException) {
        complete(Result.failure(failure))
    } catch (failure: Error) {
        if (taskStarted.get() || !complete(Result.failure(failure))) throw failure
    } catch (failure: Exception) {
        complete(Result.failure(failure))
    }
}

private fun closeAfterCancellation(
    closeController: TransportCloseController,
    cancellation: Throwable?,
) {
    try {
        closeController.close()
    } catch (failure: CancellationException) {
        cancellation?.let { addSuppressedFailure(it, failure) }
    } catch (failure: Error) {
        cancellation?.let { addSuppressedFailure(it, failure) }
    }
}

private fun closeAfterBudgetTimeout(cleanup: () -> Unit, cancellation: Throwable?) {
    try {
        cleanup()
    } catch (failure: CancellationException) {
        cancellation?.let { addSuppressedFailure(it, failure) }
    } catch (failure: Error) {
        cancellation?.let { addSuppressedFailure(it, failure) }
    }
}

private fun preferFatalFailure(current: Throwable?, candidate: Throwable): Throwable {
    if (current == null || current === candidate) return candidate
    val selected = when {
        candidate is Error && current !is Error -> candidate
        current is Error -> current
        candidate is CancellationException && current !is CancellationException -> candidate
        else -> current
    }
    val secondary = if (selected === current) candidate else current
    addSuppressedFailure(selected, secondary)
    return selected
}

private fun addSuppressedFailure(primary: Throwable, secondary: Throwable) {
    if (primary !== secondary) primary.addSuppressed(secondary)
}

private fun Socket.getInputStreamSafely(): InputStream = try {
    getInputStream()
} catch (failure: CancellationException) {
    throw failure
} catch (failure: Error) {
    throw failure
} catch (_: Exception) {
    throw FrpTransportException(FrpTransportFailure.CONNECT_FAILED)
}

private fun Socket.getOutputStreamSafely(): OutputStream = try {
    getOutputStream()
} catch (failure: CancellationException) {
    throw failure
} catch (failure: Error) {
    throw failure
} catch (_: Exception) {
    throw FrpTransportException(FrpTransportFailure.CONNECT_FAILED)
}

private fun TlsClientSocket.getInputStreamSafely(): InputStream = try {
    inputStream
} catch (failure: CancellationException) {
    throw failure
} catch (failure: Error) {
    throw failure
} catch (_: Exception) {
    throw FrpTransportException(FrpTransportFailure.TLS_SETUP_FAILED)
}

private fun TlsClientSocket.getOutputStreamSafely(): OutputStream = try {
    outputStream
} catch (failure: CancellationException) {
    throw failure
} catch (failure: Error) {
    throw failure
} catch (_: Exception) {
    throw FrpTransportException(FrpTransportFailure.TLS_SETUP_FAILED)
}

@SuppressLint("CustomX509TrustManager", "TrustAllX509TrustManager")
internal object InsecureTlsClientSocketFactory : TlsClientSocketFactory {
    private val socketFactory by lazy {
        val context = SSLContext.getInstance("TLS")
        context.init(null, arrayOf(InsecureTrustManager), SecureRandom())
        context.socketFactory
    }

    override fun create(socket: Socket, endpoint: FrpTcpEndpoint): TlsClientSocket {
        val sslSocket = socketFactory.createSocket(socket, endpoint.tlsHost, endpoint.port, true) as SSLSocket
        sslSocket.useClientMode = true
        return JdkTlsClientSocket(sslSocket)
    }

    override fun toString(): String = "InsecureTlsClientSocketFactory(verificationEnabled=false)"
}

private object InsecureTrustManager : X509TrustManager {
    override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) = Unit

    override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) = Unit

    override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
}

internal fun tlsClientSocketFactory(config: TcpTlsTransportConfig): TlsClientSocketFactory =
    config.tlsServerCertificateSha256
        ?.let(::PinnedTlsClientSocketFactory)
        ?: InsecureTlsClientSocketFactory

internal class PinnedTlsClientSocketFactory(
    certificateSha256: String,
) : TlsClientSocketFactory {
    private val trustManager = PinnedCertificateTrustManager(certificateSha256)
    private val socketFactory by lazy {
        val context = SSLContext.getInstance("TLS")
        context.init(null, arrayOf(trustManager), SecureRandom())
        context.socketFactory
    }

    override fun create(socket: Socket, endpoint: FrpTcpEndpoint): TlsClientSocket {
        val sslSocket = socketFactory.createSocket(socket, endpoint.tlsHost, endpoint.port, true) as SSLSocket
        sslSocket.useClientMode = true
        sslSocket.sslParameters = sslSocket.sslParameters.apply {
            endpointIdentificationAlgorithm = "HTTPS"
        }
        return JdkTlsClientSocket(sslSocket)
    }

    override fun toString(): String = "PinnedTlsClientSocketFactory(certificateSha256=<redacted>)"
}

internal class PinnedCertificateTrustManager(
    certificateSha256: String,
) : X509TrustManager {
    private val expectedSha256 = certificateSha256.decodeSha256()

    override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) {
        throw CertificateException("client certificates are unsupported")
    }

    override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) {
        val leaf = chain?.firstOrNull() ?: throw CertificateException("server certificate was missing")
        leaf.checkValidity()
        if (leaf.basicConstraints >= 0) {
            throw CertificateException("server certificate leaf must not be a certificate authority")
        }
        val actualSha256 = MessageDigest.getInstance("SHA-256").digest(leaf.encoded)
        try {
            if (!MessageDigest.isEqual(expectedSha256, actualSha256)) {
                throw CertificateException("server certificate pin did not match")
            }
        } finally {
            actualSha256.fill(0)
        }
    }

    override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()

    override fun toString(): String = "PinnedCertificateTrustManager(certificateSha256=<redacted>)"
}

private fun String.decodeSha256(): ByteArray {
    if (!TcpTlsTransportConfig.TLS_SERVER_CERTIFICATE_SHA256.matches(this)) {
        throw FrpTransportException(FrpTransportFailure.INVALID_CONFIGURATION)
    }
    return ByteArray(length / 2) { index -> substring(index * 2, index * 2 + 2).toInt(16).toByte() }
}

private class JdkTlsClientSocket(
    private val socket: SSLSocket,
) : TlsClientSocket {
    override val inputStream: InputStream
        get() = socket.inputStream

    override val outputStream: OutputStream
        get() = socket.outputStream

    override fun startHandshake() {
        socket.startHandshake()
    }

    override fun close() {
        socket.close()
    }

    override fun toString(): String = "JdkTlsClientSocket()"
}
