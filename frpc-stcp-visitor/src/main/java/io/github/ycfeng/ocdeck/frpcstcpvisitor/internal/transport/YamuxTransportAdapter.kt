package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxClientSession
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxPhysicalTransport
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.createYamuxClientSession
import java.net.Socket
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

internal const val FRP_YAMUX_RECEIVE_WINDOW_SIZE = 6L * 1024L * 1024L
internal const val FRP_YAMUX_KEEPALIVE_INTERVAL_MILLIS = 30_000L
internal const val FRP_YAMUX_MAX_STREAMS = 33
internal const val FRP_YAMUX_MAX_PENDING_OPENS = 33

internal fun productionFrpYamuxConfig(): YamuxConfig = YamuxConfig(
    receiveWindowSize = FRP_YAMUX_RECEIVE_WINDOW_SIZE,
    maxDataFrameSize = FRP_YAMUX_RECEIVE_WINDOW_SIZE.toInt(),
    maxStreams = FRP_YAMUX_MAX_STREAMS,
    maxPendingOpens = FRP_YAMUX_MAX_PENDING_OPENS,
    keepAliveEnabled = true,
    keepAliveIntervalMillis = FRP_YAMUX_KEEPALIVE_INTERVAL_MILLIS,
)

internal class TcpTlsYamuxConnector(
    private val transportConfig: TcpTlsTransportConfig,
    private val parentScope: CoroutineScope,
    private val socketFactory: TcpSocketFactory = TcpSocketFactory(::Socket),
    private val routeProvider: TcpDialRouteProvider? = null,
    private val tlsSocketFactory: TlsClientSocketFactory = tlsClientSocketFactory(transportConfig),
    private val transportDispatcher: kotlinx.coroutines.CoroutineDispatcher = Dispatchers.IO,
    private val yamuxConfig: YamuxConfig = productionFrpYamuxConfig(),
    private val beforeResultHandoff: suspend () -> Unit = {},
) : FrpMuxConnector {
    override suspend fun connect(): FrpMuxConnection {
        val dialer = TcpTlsTransportDialer(
            config = transportConfig,
            socketFactory = socketFactory,
            routeProvider = routeProvider,
            tlsSocketFactory = tlsSocketFactory,
            dispatcher = transportDispatcher,
        )
        val transport = try {
            dialer.connect()
        } catch (failure: CancellationException) {
            closeConnectorResource(failure, dialer::close)
        } catch (failure: Error) {
            closeConnectorResource(failure, dialer::close)
        } catch (failure: Exception) {
            closeConnectorResource(failure, dialer::close)
        }
        var connection: YamuxMuxConnection? = null
        val sharedTransport = SharedCloseYamuxPhysicalTransport(transport)
        return try {
            val session = createYamuxClientSession(
                transport = sharedTransport,
                parentScope = parentScope,
                config = yamuxConfig,
            )
            val createdConnection = YamuxMuxConnection(session, sharedTransport, parentScope)
            connection = createdConnection
            beforeResultHandoff()
            handoffFrpOwnedResult(createdConnection, FrpMuxConnection::close)
        } catch (failure: CancellationException) {
            closeConnectorConnection(failure, connection, sharedTransport)
        } catch (failure: Error) {
            closeConnectorConnection(failure, connection, sharedTransport)
        } catch (failure: Exception) {
            closeConnectorConnection(failure, connection, sharedTransport)
        }
    }

    override fun toString(): String =
        "TcpTlsYamuxConnector(tlsEnabled=${transportConfig.tlsEnabled}, " +
            "receiveWindowSize=${yamuxConfig.receiveWindowSize}, " +
            "keepAliveIntervalMillis=${yamuxConfig.keepAliveIntervalMillis})"
}

private fun closeConnectorResource(failure: Throwable, close: () -> Unit): Nothing {
    var selected = failure
    try {
        close()
    } catch (closeFailure: CancellationException) {
        selected = checkNotNull(preferMuxCloseFailure(selected, closeFailure))
    } catch (closeFailure: Error) {
        selected = checkNotNull(preferMuxCloseFailure(selected, closeFailure))
    } catch (_: Exception) {
        // The construction failure remains authoritative for ordinary close I/O.
    }
    throw selected
}

private suspend fun closeConnectorConnection(
    failure: Throwable,
    connection: FrpMuxConnection?,
    transport: YamuxPhysicalTransport,
): Nothing {
    if (connection == null) closeConnectorResource(failure, transport::close)
    var selected = failure
    try {
        connection.close()
    } catch (closeFailure: CancellationException) {
        selected = checkNotNull(preferMuxCloseFailure(selected, closeFailure))
    } catch (closeFailure: Error) {
        selected = checkNotNull(preferMuxCloseFailure(selected, closeFailure))
    } catch (_: Exception) {
        // The construction failure remains authoritative for ordinary close I/O.
    }
    try {
        withContext(NonCancellable) { connection.awaitTermination() }
    } catch (closeFailure: CancellationException) {
        selected = checkNotNull(preferMuxCloseFailure(selected, closeFailure))
    } catch (closeFailure: Error) {
        selected = checkNotNull(preferMuxCloseFailure(selected, closeFailure))
    } catch (_: Exception) {
        // The safe construction failure remains authoritative for ordinary close I/O.
    }
    throw selected
}

internal class YamuxMuxConnection(
    private val session: YamuxClientSession,
    private val transport: SharedCloseYamuxPhysicalTransport,
    parentScope: CoroutineScope,
) : FrpMuxConnection {
    private val closeStarted = AtomicBoolean(false)
    private val closeFailure = AtomicReference<Throwable?>()
    private val closeCompletion = CompletableDeferred<Unit>()
    private val cleanupJob = SupervisorJob()
    private val cleanupScope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) +
            cleanupJob +
            CoroutineName("FrpYamuxConnectionCleanup"),
    )

    override suspend fun openStream(): FrpMuxStream {
        if (closeStarted.get()) throw FrpTransportException(FrpTransportFailure.CLOSED)
        return YamuxMuxStream(session.openStream())
    }

    override suspend fun awaitTermination() {
        var sessionFailure: Throwable? = null
        try {
            session.awaitTermination()
        } catch (failure: CancellationException) {
            sessionFailure = failure
        } catch (failure: Error) {
            sessionFailure = failure
        } catch (failure: Exception) {
            sessionFailure = failure
        }
        if (closeStarted.get()) closeCompletion.await()
        preferMuxCloseFailure(sessionFailure, closeFailure.get())?.let { throw it }
    }

    override fun forceTerminationAfterClose() {
        if (!closeStarted.get()) return
        session.forceTerminationAfterPhysicalClose()
    }

    override fun close() {
        startCloseSequence()
        closeFailure.get()?.let { throw it }
    }

    override fun toString(): String = "YamuxMuxConnection(closed=${closeStarted.get()})"

    private fun startCloseSequence() {
        if (!closeStarted.compareAndSet(false, true)) return
        val physicalCloseOwner = transport.reserveCloseOwner()
        val sessionCloseJob = cleanupScope.launch(start = CoroutineStart.UNDISPATCHED) {
            try {
                session.close()
            } catch (failure: CancellationException) {
                recordCloseFailure(failure)
            } catch (failure: Error) {
                recordCloseFailure(failure)
            } catch (failure: Exception) {
                recordCloseFailure(failure)
            }
        }
        try {
            if (physicalCloseOwner) transport.closeFromReservedOwner() else transport.close()
        } catch (failure: CancellationException) {
            recordCloseFailure(failure)
        } catch (failure: Error) {
            recordCloseFailure(failure)
        } catch (_: Exception) {
            // Local Closed remains authoritative for ordinary physical close I/O.
        }
        sessionCloseJob.invokeOnCompletion {
            closeCompletion.complete(Unit)
            cleanupJob.cancel()
        }
    }

    private fun recordCloseFailure(failure: Throwable) {
        while (true) {
            val current = closeFailure.get()
            val selected = preferMuxCloseFailure(current, failure)
            if (selected === current || closeFailure.compareAndSet(current, selected)) return
        }
    }
}

internal class SharedCloseYamuxPhysicalTransport(
    private val delegate: YamuxPhysicalTransport,
) : YamuxPhysicalTransport {
    private val closeLock = Any()
    private var closeState = SharedPhysicalCloseState.OPEN
    private var closeFailure: Throwable? = null

    override suspend fun runReaderLoop(block: suspend () -> Unit) {
        delegate.runReaderLoop(block)
    }

    override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int =
        delegate.read(destination, offset, length)

    override suspend fun writeFully(source: ByteArray, offset: Int, length: Int) {
        delegate.writeFully(source, offset, length)
    }

    internal fun reserveCloseOwner(): Boolean = synchronized(closeLock) {
        if (closeState != SharedPhysicalCloseState.OPEN) {
            false
        } else {
            closeState = SharedPhysicalCloseState.RESERVED
            true
        }
    }

    internal fun closeFromReservedOwner() {
        val shouldClose = synchronized(closeLock) {
            check(closeState == SharedPhysicalCloseState.RESERVED)
            closeState = SharedPhysicalCloseState.CLOSING
            true
        }
        if (shouldClose) closeDelegate()
    }

    override fun close() {
        val shouldClose = synchronized(closeLock) {
            when (closeState) {
                SharedPhysicalCloseState.OPEN -> {
                    closeState = SharedPhysicalCloseState.CLOSING
                    true
                }
                SharedPhysicalCloseState.RESERVED,
                SharedPhysicalCloseState.CLOSING -> false
                SharedPhysicalCloseState.CLOSED -> {
                    closeFailure?.let { throw it }
                    false
                }
            }
        }
        if (shouldClose) closeDelegate()
    }

    override fun toString(): String = synchronized(closeLock) {
        "SharedCloseYamuxPhysicalTransport(closed=${closeState == SharedPhysicalCloseState.CLOSED})"
    }

    private fun closeDelegate() {
        var fatalFailure: Throwable? = null
        try {
            delegate.close()
        } catch (failure: CancellationException) {
            fatalFailure = failure
            throw failure
        } catch (failure: Error) {
            fatalFailure = failure
            throw failure
        } finally {
            synchronized(closeLock) {
                closeFailure = fatalFailure
                closeState = SharedPhysicalCloseState.CLOSED
            }
        }
    }

    private enum class SharedPhysicalCloseState {
        OPEN,
        RESERVED,
        CLOSING,
        CLOSED,
    }
}

private fun preferMuxCloseFailure(current: Throwable?, candidate: Throwable?): Throwable? {
    if (candidate == null || current === candidate) return current
    if (current == null) return candidate
    val selected = when {
        candidate is Error && current !is Error -> candidate
        current is Error -> current
        candidate is CancellationException && current !is CancellationException -> candidate
        else -> current
    }
    val secondary = if (selected === current) candidate else current
    if (selected !== secondary) selected.addSuppressed(secondary)
    return selected
}

private class YamuxMuxStream(
    private val stream: YamuxStream,
) : FrpMuxStream {
    override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int =
        stream.read(destination, offset, length)

    override suspend fun write(source: ByteArray, offset: Int, length: Int) {
        stream.write(source, offset, length)
    }

    override suspend fun closeWrite() {
        stream.closeWrite()
    }

    override suspend fun reset() {
        stream.reset()
    }

    override fun toString(): String = "YamuxMuxStream()"
}
