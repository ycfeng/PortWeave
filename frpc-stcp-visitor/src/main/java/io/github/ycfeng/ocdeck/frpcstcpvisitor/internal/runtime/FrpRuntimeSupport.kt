package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime

import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcSessionConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.compression.FrpSnappyFramedInputStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.compression.FrpSnappyFramedOutputStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpClientStreamLease
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlClient
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlIdentity
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlReconnectDiagnostics
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlState
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpUnixTimeSource
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpWireProtocol
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpTimestampAuth
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpV1Cfb
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV1
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV2
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.NewVisitorConn
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.NewVisitorConnResp
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.writeSafely
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.BlockingIoThreadGuard
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpTcpEndpoint
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.TcpDialRouteProvider
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.TcpTlsTransportConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.TcpTlsYamuxConnector
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.YamuxStreamBlockingIo
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext

internal interface FrpSessionControl : AutoCloseable {
    val state: StateFlow<FrpControlState>

    fun start()

    fun invalidateTransport(): FrpControlIdentity? = null

    fun reconnectCleanupFailure(): FrpControlFailure? = null

    fun reconnectEstablishmentFailure(): FrpControlFailure? = null

    fun reconnectDiagnostics(): FrpControlReconnectDiagnostics = FrpControlReconnectDiagnostics()

    suspend fun openClientStream(expectedIdentity: FrpControlIdentity): FrpClientStreamLease

    suspend fun awaitStopped()
}

internal fun interface FrpSessionControlFactory {
    fun create(
        config: FrpcSessionConfig,
        parentScope: CoroutineScope,
        generation: Long,
    ): FrpSessionControl
}

internal class ProductionFrpSessionControlFactory(
    private val routeProvider: TcpDialRouteProvider? = null,
) : FrpSessionControlFactory {
    override fun create(
        config: FrpcSessionConfig,
        parentScope: CoroutineScope,
        generation: Long,
    ): FrpSessionControl {
        val wireProtocol = when (config.wireProtocol) {
            "v1" -> FrpWireProtocol.V1
            "v2" -> FrpWireProtocol.V2
            else -> throw KotlinFrpcStcpVisitorException(
                KotlinFrpcStcpVisitorFailure.UNSUPPORTED_CONFIGURATION,
            )
        }
        val connector = TcpTlsYamuxConnector(
            transportConfig = TcpTlsTransportConfig(
                endpoint = FrpTcpEndpoint(config.serverAddr, config.serverPort, config.tlsServerName),
                tlsEnabled = true,
                tlsServerCertificateSha256 = config.tlsServerCertificateSha256,
            ),
            parentScope = parentScope,
            routeProvider = routeProvider,
        )
        return FrpControlAdapter(
            FrpControlClient(
                config = productionFrpControlConfig(config, wireProtocol),
                connector = connector,
                parentScope = parentScope,
                generation = generation,
            ),
        )
    }

}

internal fun productionFrpControlConfig(
    config: FrpcSessionConfig,
    wireProtocol: FrpWireProtocol,
): FrpControlConfig = FrpControlConfig(
    token = config.authToken,
    user = config.user.orEmpty(),
    wireProtocol = wireProtocol,
)

private class FrpControlAdapter(
    private val delegate: FrpControlClient,
) : FrpSessionControl {
    override val state: StateFlow<FrpControlState>
        get() = delegate.state

    override fun start() {
        delegate.start()
    }

    override fun invalidateTransport(): FrpControlIdentity? = delegate.invalidateTransport()

    override fun reconnectCleanupFailure(): FrpControlFailure? = delegate.reconnectCleanupFailure()

    override fun reconnectEstablishmentFailure(): FrpControlFailure? = delegate.reconnectEstablishmentFailure()

    override fun reconnectDiagnostics(): FrpControlReconnectDiagnostics = delegate.reconnectDiagnostics()

    override suspend fun openClientStream(expectedIdentity: FrpControlIdentity): FrpClientStreamLease =
        delegate.openClientStream(expectedIdentity)

    override suspend fun awaitStopped() {
        delegate.awaitStopped()
    }

    override fun close() {
        delegate.close()
    }

    override fun toString(): String = "FrpControlAdapter(delegate=$delegate)"
}

internal interface FrpLocalConnection : AutoCloseable {
    val input: InputStream

    val output: OutputStream

    fun shutdownOutput()
}

internal interface FrpLocalListener : AutoCloseable {
    val bindPort: Int

    suspend fun accept(): FrpLocalConnection
}

internal fun interface FrpLocalListenerFactory {
    suspend fun bind(address: InetAddress, port: Int): FrpLocalListener
}

internal class SocketFrpLocalListenerFactory(
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : FrpLocalListenerFactory {
    override suspend fun bind(address: InetAddress, port: Int): FrpLocalListener {
        val holder = AtomicReference<SocketFrpLocalListener?>()
        return try {
            val listener = runInterruptible(dispatcher) {
                check(address.address.contentEquals(IPV4_LOOPBACK_BYTES))
                val serverSocket = ServerSocket()
                try {
                    serverSocket.reuseAddress = true
                    serverSocket.bind(InetSocketAddress(address, port), LOCAL_LISTENER_BACKLOG)
                    SocketFrpLocalListener(serverSocket, dispatcher).also(holder::set)
                } catch (failure: CancellationException) {
                    closeServerSocket(serverSocket)
                    throw failure
                } catch (failure: Error) {
                    closeServerSocket(serverSocket)
                    throw failure
                } catch (failure: Exception) {
                    closeServerSocket(serverSocket)
                    throw failure
                }
            }
            currentCoroutineContext().ensureActive()
            holder.set(null)
            listener
        } catch (failure: CancellationException) {
            holder.getAndSet(null)?.close()
            throw failure
        } catch (failure: Error) {
            holder.getAndSet(null)?.close()
            throw failure
        } catch (failure: Exception) {
            holder.getAndSet(null)?.close()
            throw failure
        }
    }

    override fun toString(): String = "SocketFrpLocalListenerFactory()"

    private fun closeServerSocket(socket: ServerSocket) {
        try {
            socket.close()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            // Construction failure remains authoritative.
        }
    }

    private companion object {
        val IPV4_LOOPBACK_BYTES = byteArrayOf(127, 0, 0, 1)
        const val LOCAL_LISTENER_BACKLOG = 128
    }
}

private class SocketFrpLocalListener(
    private val socket: ServerSocket,
    private val dispatcher: CoroutineDispatcher,
) : FrpLocalListener {
    private val closed = AtomicBoolean(false)

    override val bindPort: Int
        get() = socket.localPort

    override suspend fun accept(): FrpLocalConnection {
        if (closed.get()) throw KotlinFrpcStcpVisitorException(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
        val holder = AtomicReference<FrpLocalConnection?>()
        return try {
            val connection = runInterruptible(dispatcher) {
                val accepted = socket.accept()
                try {
                    accepted.tcpNoDelay = true
                    accepted.keepAlive = true
                    SocketFrpLocalConnection(accepted).also(holder::set)
                } catch (failure: CancellationException) {
                    closeSocket(accepted)
                    throw failure
                } catch (failure: Error) {
                    closeSocket(accepted)
                    throw failure
                } catch (failure: Exception) {
                    closeSocket(accepted)
                    throw failure
                }
            }
            currentCoroutineContext().ensureActive()
            holder.set(null)
            connection
        } catch (failure: CancellationException) {
            holder.getAndSet(null)?.close()
            throw failure
        } catch (failure: Error) {
            holder.getAndSet(null)?.close()
            throw failure
        } catch (failure: Exception) {
            holder.getAndSet(null)?.close()
            throw failure
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        socket.close()
    }

    override fun toString(): String = "SocketFrpLocalListener(closed=${closed.get()})"
}

private class SocketFrpLocalConnection(
    private val socket: Socket,
) : FrpLocalConnection {
    private val closed = AtomicBoolean(false)

    override val input: InputStream = socket.getInputStream()
    override val output: OutputStream = socket.getOutputStream()

    override fun shutdownOutput() {
        if (!closed.get() && !socket.isOutputShutdown) socket.shutdownOutput()
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        socket.close()
    }

    override fun toString(): String = "SocketFrpLocalConnection(closed=${closed.get()})"
}

private fun closeSocket(socket: Socket) {
    try {
        socket.close()
    } catch (failure: CancellationException) {
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (_: Exception) {
        // Construction failure remains authoritative.
    }
}

internal data class FrpRelayVisitorConfig(
    val serverName: String,
    val serverUser: String,
    val sessionUser: String,
    val secretKey: String,
    val useEncryption: Boolean = false,
    val useCompression: Boolean = false,
) {
    override fun toString(): String =
        "FrpRelayVisitorConfig(serverUserPresent=${serverUser.isNotEmpty()}, " +
            "sessionUserPresent=${sessionUser.isNotEmpty()}, secretKey=<redacted>, " +
            "useEncryption=$useEncryption, useCompression=$useCompression)"
}

internal enum class FrpVisitorRelayCleanupPhase {
    OPENING,
    RELAYING,
    RESETTING_LEASE,
    RESETTING_ADAPTER,
    COMPLETE,
}

private enum class FrpVisitorHandshakeState {
    ACTIVE,
    COMPLETED,
    TIMED_OUT,
}

internal class FrpVisitorRelay(
    val visitorRevision: Long,
    val listenerAttempt: Long,
    private val control: FrpSessionControl,
    private val identity: FrpControlIdentity,
    private val visitor: FrpRelayVisitorConfig,
    private val local: FrpLocalConnection,
    parentScope: CoroutineScope,
    private val unixTimeSource: FrpUnixTimeSource,
    private val ioDispatcher: CoroutineDispatcher,
    private val afterPumpOutcomeReceived: suspend () -> Unit = {},
    private val afterOrdinaryPumpFailureClassified: suspend () -> Unit = {},
    private val beforeHandshakeCommit: suspend () -> Unit = {},
    private val handshakeTimeoutMillis: Long = VISITOR_HANDSHAKE_TIMEOUT_MILLIS,
) {
    init {
        require(handshakeTimeoutMillis > 0L)
    }

    private val closeStarted = AtomicBoolean(false)
    private val activeLease = AtomicReference<FrpClientStreamLease?>()
    private val fatalFailure = AtomicReference<Throwable?>()
    private val ordinaryFailure = AtomicReference<KotlinFrpcStcpVisitorException?>()
    private val localStopFailure = AtomicReference<Throwable?>()
    private val localStopCompletion = CompletableDeferred<Unit>()
    private val localStopCompletionFinalized = AtomicBoolean(false)
    private val completion = CompletableDeferred<Unit>()
    private val completionFinalized = AtomicBoolean(false)
    private val ownerJob = SupervisorJob(parentScope.coroutineContext[Job])
    private val scope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) + ownerJob + CoroutineName("FrpVisitorRelay"),
    )
    private val blockingIoMarker = ThreadLocal<Boolean>()
    private val blockingIoContext: CoroutineContext =
        ioDispatcher + blockingIoMarker.asContextElement(true)
    private val blockingIoGuard = BlockingIoThreadGuard {
        check(blockingIoMarker.get() == true) { "visitor handshake requires its blocking dispatcher" }
    }
    private val runner = AtomicReference<Job?>()
    private val runnerState = AtomicReference(RelayRunnerState.NEW)
    private val relayCleanupPhase = AtomicReference(FrpVisitorRelayCleanupPhase.OPENING)

    val isCompleted: Boolean
        get() = completion.isCompleted

    val cleanupPhase: FrpVisitorRelayCleanupPhase
        get() = relayCleanupPhase.get()

    fun start() {
        val job = scope.launch(start = CoroutineStart.LAZY) { runRelay() }
        check(runner.compareAndSet(null, job))
        job.invokeOnCompletion { failure ->
            runnerState.set(RelayRunnerState.TERMINATED)
            relayCleanupPhase.set(FrpVisitorRelayCleanupPhase.COMPLETE)
            val completionError = findRelayCompletionError(failure)
            completeLocalStopOnce(completionError)
            completeRelayOnce(completionError, null)
        }
        job.start()
    }

    fun close() {
        if (!closeStarted.compareAndSet(false, true)) return
        var closeFailure = closeLocalExpected()
        try {
            activeLease.get()?.beginReset()
        } catch (failure: CancellationException) {
            closeFailure = preferRelayFatal(closeFailure, failure)
        } catch (failure: Error) {
            closeFailure = preferRelayFatal(closeFailure, failure)
        } catch (_: Exception) {
            // Logical stream invalidation is best effort after the local relay is closed.
        }
        completeLocalStopOnce(closeFailure)
        if (runnerState.compareAndSet(RelayRunnerState.NEW, RelayRunnerState.TERMINATED)) {
            relayCleanupPhase.set(FrpVisitorRelayCleanupPhase.COMPLETE)
            completeRelayOnce(closeFailure, null)
        }
        ownerJob.cancel(FrpRelayClosedCancellation())
        closeFailure?.let { throw it }
    }

    suspend fun awaitLocallyStopped() {
        localStopCompletion.await()
        localStopFailure.get()?.let { throw it }
    }

    suspend fun awaitStopped() {
        completion.await()
        fatalFailure.get()?.let { throw it }
        ordinaryFailure.get()?.let { throw it }
    }

    override fun toString(): String =
        "FrpVisitorRelay(identity=$identity, visitorRevision=$visitorRevision, " +
            "listenerAttempt=$listenerAttempt, closed=${closeStarted.get()}, completed=${completion.isCompleted})"

    private suspend fun runRelay() {
        if (!runnerState.compareAndSet(RelayRunnerState.NEW, RelayRunnerState.RUNNING)) return
        var lease: FrpClientStreamLease? = null
        var handshakeIo: YamuxStreamBlockingIo? = null
        var selectedFatal: Throwable? = null
        var selectedOrdinary: KotlinFrpcStcpVisitorException? = null
        val relayingFatalFailure = AtomicReference<Throwable?>()
        fun observeFailure(failure: Throwable?) {
            when (failure) {
                null -> Unit
                is CancellationException -> selectUnexpectedRelayCloseFailure(failure)?.let { unexpected ->
                    selectedFatal = preferRelayFatal(selectedFatal, unexpected)
                }
                is Error -> selectedFatal = preferRelayFatal(selectedFatal, failure)
                is KotlinFrpcStcpVisitorException -> if (selectedOrdinary == null) selectedOrdinary = failure
                is Exception -> if (selectedOrdinary == null) {
                    selectedOrdinary = KotlinFrpcStcpVisitorException(KotlinFrpcStcpVisitorFailure.RELAY_FAILED)
                }
            }
        }
        try {
            val opened = control.openClientStream(identity)
            lease = opened
            activeLease.set(opened)
            if (closeStarted.get()) throw FrpRelayClosedCancellation()
            val io = if (visitor.useEncryption || visitor.useCompression) {
                YamuxStreamBlockingIo(opened.stream, scope, blockingIoGuard)
            } else {
                null
            }
            handshakeIo = io
            performHandshake(opened, io)
            relayCleanupPhase.set(FrpVisitorRelayCleanupPhase.RELAYING)
            relayBidirectionally(opened, io, relayingFatalFailure)
        } catch (failure: CancellationException) {
            observeFailure(failure)
            observeFailure(relayingFatalFailure.get())
        } catch (failure: Error) {
            observeFailure(failure)
            observeFailure(relayingFatalFailure.get())
        } catch (failure: Exception) {
            observeFailure(failure)
            observeFailure(relayingFatalFailure.get())
        } finally {
            closeStarted.set(true)
            closeLocalExpected()?.let { failure ->
                selectedFatal = preferRelayFatal(selectedFatal, failure)
            }
            try {
                lease?.beginReset()
            } catch (failure: CancellationException) {
                selectedFatal = preferRelayFatal(selectedFatal, failure)
            } catch (failure: Error) {
                selectedFatal = preferRelayFatal(selectedFatal, failure)
            } catch (_: Exception) {
                // Logical stream invalidation is best effort after the local relay is closed.
            }
            try {
                handshakeIo?.close()
            } catch (failure: CancellationException) {
                selectedFatal = preferRelayFatal(selectedFatal, failure)
            } catch (failure: Error) {
                selectedFatal = preferRelayFatal(selectedFatal, failure)
            } catch (_: Exception) {
                // Adapter cleanup remains owned by the full relay completion path.
            }
            completeLocalStopOnce(selectedFatal)
            if (lease != null) {
                relayCleanupPhase.set(FrpVisitorRelayCleanupPhase.RESETTING_LEASE)
                try {
                    withContext(NonCancellable) { lease.reset() }
                } catch (failure: CancellationException) {
                    selectedFatal = preferRelayFatal(selectedFatal, failure)
                } catch (failure: Error) {
                    selectedFatal = preferRelayFatal(selectedFatal, failure)
                } catch (_: Exception) {
                    // Ordinary reset I/O is secondary to relay termination.
                }
                relayCleanupPhase.set(FrpVisitorRelayCleanupPhase.RESETTING_ADAPTER)
                try {
                    withContext(NonCancellable) { handshakeIo?.awaitReset() }
                } catch (failure: CancellationException) {
                    selectedFatal = preferRelayFatal(selectedFatal, failure)
                } catch (failure: Error) {
                    selectedFatal = preferRelayFatal(selectedFatal, failure)
                } catch (_: Exception) {
                    // Ordinary adapter reset I/O is secondary to relay termination.
                }
            }
            activeLease.set(null)
            runnerState.set(RelayRunnerState.TERMINATED)
            relayCleanupPhase.set(FrpVisitorRelayCleanupPhase.COMPLETE)
            completeRelayOnce(selectedFatal, selectedOrdinary)
            ownerJob.cancel()
        }
        selectedFatal?.let { throw it }
    }

    private suspend fun performHandshake(
        lease: FrpClientStreamLease,
        io: YamuxStreamBlockingIo?,
    ) = coroutineScope {
        val handshakeState = AtomicReference(FrpVisitorHandshakeState.ACTIVE)
        val timer = launch {
            delay(handshakeTimeoutMillis)
            if (handshakeState.compareAndSet(
                    FrpVisitorHandshakeState.ACTIVE,
                    FrpVisitorHandshakeState.TIMED_OUT,
                )
            ) {
                lease.beginReset()
            }
        }
        try {
            val timestamp = unixTimeSource.nowSeconds()
            val prefix = visitor.serverUser.ifEmpty { visitor.sessionUser }
            val proxyName = if (prefix.isEmpty()) visitor.serverName else "$prefix.${visitor.serverName}"
            val request = NewVisitorConn(
                runId = lease.runId,
                proxyName = proxyName,
                signKey = FrpTimestampAuth.key(visitor.secretKey, timestamp),
                timestamp = timestamp,
                useEncryption = visitor.useEncryption,
                useCompression = visitor.useCompression,
            )
            val response = if (io == null) {
                val encoded = when (lease.wireProtocol) {
                    FrpWireProtocol.V1 -> FrpWireV1.encodeMessage(request)
                    FrpWireProtocol.V2 -> FrpWireV2.encodeMessageWithMagic(request)
                }
                try {
                    writeSafely(lease.stream, encoded)
                } finally {
                    encoded.fill(0)
                }
                when (lease.wireProtocol) {
                    FrpWireProtocol.V1 -> FrpWireV1.readMessage(lease.stream)
                    FrpWireProtocol.V2 -> FrpWireV2.readMessage(lease.stream)
                }
            } else {
                runInterruptible(blockingIoContext) {
                    if (lease.wireProtocol == FrpWireProtocol.V2) FrpWireV2.writeMagic(io.output)
                    val encoded = when (lease.wireProtocol) {
                        FrpWireProtocol.V1 -> FrpWireV1.encodeMessage(request)
                        FrpWireProtocol.V2 -> FrpWireV2.encodeMessage(request)
                    }
                    try {
                        io.output.write(encoded)
                        io.output.flush()
                    } finally {
                        encoded.fill(0)
                    }
                    when (lease.wireProtocol) {
                        FrpWireProtocol.V1 -> FrpWireV1.readMessage(io.input)
                        FrpWireProtocol.V2 -> FrpWireV2.readMessage(io.input)
                    }
                }
            }
            val visitorResponse = response as? NewVisitorConnResp
                ?: throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.VISITOR_HANDSHAKE_FAILED,
                )
            if (visitorResponse.error.isNotEmpty()) {
                throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.VISITOR_HANDSHAKE_FAILED,
                )
            }
            beforeHandshakeCommit()
            if (!handshakeState.compareAndSet(
                    FrpVisitorHandshakeState.ACTIVE,
                    FrpVisitorHandshakeState.COMPLETED,
                )
            ) {
                throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.VISITOR_HANDSHAKE_TIMEOUT,
                )
            }
        } catch (failure: CancellationException) {
            if (handshakeState.get() == FrpVisitorHandshakeState.TIMED_OUT) {
                throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.VISITOR_HANDSHAKE_TIMEOUT,
                )
            }
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: Exception) {
            if (handshakeState.get() == FrpVisitorHandshakeState.TIMED_OUT) {
                throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.VISITOR_HANDSHAKE_TIMEOUT,
                )
            }
            if (failure is KotlinFrpcStcpVisitorException) throw failure
            throw KotlinFrpcStcpVisitorException(
                KotlinFrpcStcpVisitorFailure.VISITOR_HANDSHAKE_FAILED,
            )
        } finally {
            timer.cancel()
        }
    }

    private suspend fun relayBidirectionally(
        lease: FrpClientStreamLease,
        io: YamuxStreamBlockingIo?,
        relayingFatalFailure: AtomicReference<Throwable?>,
    ) = supervisorScope {
        val completedPumps = Channel<Int>(capacity = Channel.UNLIMITED)
        val outcomes = List(2) { AtomicReference<RelayPumpOutcome?>() }
        val mergedOutcomes = BooleanArray(2)
        val pumps = if (!visitor.useEncryption && !visitor.useCompression) {
            listOf(
                launchRelayPump(0, outcomes[0], completedPumps) { copyLocalToRemoteDirect(lease) },
                launchRelayPump(1, outcomes[1], completedPumps) { copyRemoteToLocalDirect(lease) },
            )
        } else {
            val transformedIo = checkNotNull(io)
            listOf(
                launchRelayPump(0, outcomes[0], completedPumps) { copyLocalToRemote(lease, transformedIo.output) },
                launchRelayPump(1, outcomes[1], completedPumps) { copyRemoteToLocal(transformedIo.input) },
            )
        }
        var selectedFailure: Throwable? = null
        var abortStarted = false
        var observed = 0
        try {
            while (observed < pumps.size) {
                val completedIndex = completedPumps.receive()
                afterPumpOutcomeReceived()
                observed += 1
                mergedOutcomes[completedIndex] = true
                val failure = checkNotNull(outcomes[completedIndex].get()).failure
                if (failure != null) {
                    selectedFailure = preferRelayFatal(selectedFailure, failure)
                    if (!abortStarted) {
                        abortStarted = true
                        abortRelay(lease)?.let { failure ->
                            selectedFailure = preferRelayFatal(selectedFailure, failure)
                        }
                    }
                }
            }
        } catch (failure: CancellationException) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Error) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Exception) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } finally {
            withContext(NonCancellable) {
                if (selectedFailure != null && !abortStarted) {
                    abortStarted = true
                    abortRelay(lease)?.let { failure ->
                        selectedFailure = preferRelayFatal(selectedFailure, failure)
                    }
                }
                pumps.joinAll()
                outcomes.forEachIndexed { index, outcome ->
                    if (!mergedOutcomes[index]) checkNotNull(outcome.get()).failure?.let { failure ->
                        selectedFailure = preferRelayFatal(selectedFailure, failure)
                    }
                }
                relayingFatalFailure.set(
                    when (val failure = selectedFailure) {
                        is Error -> failure
                        is CancellationException -> selectUnexpectedRelayCloseFailure(failure)
                        else -> null
                    },
                )
            }
        }
        selectedFailure?.let { throw it }
    }

    private fun CoroutineScope.launchRelayPump(
        index: Int,
        outcome: AtomicReference<RelayPumpOutcome?>,
        completedPumps: Channel<Int>,
        block: suspend () -> Unit,
    ): Job {
        val job = launch(start = CoroutineStart.UNDISPATCHED) {
            var failure: Throwable? = null
            try {
                block()
            } catch (caught: CancellationException) {
                failure = caught
            } catch (caught: Error) {
                failure = caught
            } catch (caught: Exception) {
                failure = caught
            } finally {
                recordRelayPumpOutcome(index, outcome, completedPumps, failure)
            }
        }
        job.invokeOnCompletion { failure ->
            recordRelayPumpOutcome(index, outcome, completedPumps, failure)
        }
        return job
    }

    private fun recordRelayPumpOutcome(
        index: Int,
        outcome: AtomicReference<RelayPumpOutcome?>,
        completedPumps: Channel<Int>,
        failure: Throwable?,
    ) {
        if (!outcome.compareAndSet(null, RelayPumpOutcome(failure))) return
        check(completedPumps.trySend(index).isSuccess)
    }

    private fun abortRelay(lease: FrpClientStreamLease): Throwable? {
        var selectedFailure = closeLocalExpected()
        try {
            lease.beginReset()
        } catch (failure: CancellationException) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Error) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (_: Exception) {
            if (selectedFailure == null) {
                selectedFailure = KotlinFrpcStcpVisitorException(KotlinFrpcStcpVisitorFailure.RELAY_FAILED)
            }
        }
        return selectedFailure
    }

    private suspend fun copyLocalToRemote(
        lease: FrpClientStreamLease,
        baseOutput: OutputStream,
    ) {
        val buffer = ByteArray(RELAY_BUFFER_SIZE)
        var remoteOutput: OutputStream? = null
        var selectedFailure: Throwable? = null
        try {
            remoteOutput = createRemoteOutput(baseOutput)
            while (true) {
                val count = runInterruptible(ioDispatcher) { local.input.read(buffer) }
                if (count < 0) break
                if (count == 0) throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.RELAY_FAILED,
                )
                runInterruptible(blockingIoContext) { remoteOutput.write(buffer, 0, count) }
            }
        } catch (failure: CancellationException) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Error) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Exception) {
            classifyOrdinaryRelayIoFailure(failure)?.let { classified ->
                selectedFailure = preferRelayFatal(selectedFailure, classified)
            }
        } finally {
            buffer.fill(0)
            remoteOutput?.let { output ->
                try {
                    runInterruptible(blockingIoContext) { output.close() }
                } catch (failure: CancellationException) {
                    selectedFailure = preferRelayFatal(selectedFailure, failure)
                } catch (failure: Error) {
                    selectedFailure = preferRelayFatal(selectedFailure, failure)
                } catch (failure: Exception) {
                    classifyOrdinaryRelayIoFailure(failure)?.let { classified ->
                        selectedFailure = preferRelayFatal(selectedFailure, classified)
                    }
                }
            }
        }
        selectedFailure?.let { throw it }
        try {
            lease.stream.closeWrite()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: Exception) {
            classifyOrdinaryRelayIoFailure(failure)?.let { throw it }
        }
    }

    private suspend fun copyLocalToRemoteDirect(lease: FrpClientStreamLease) = withContext(ioDispatcher) {
        val buffer = ByteArray(RELAY_BUFFER_SIZE)
        var selectedFailure: Throwable? = null
        try {
            while (true) {
                val count = local.input.read(buffer)
                if (count < 0) break
                if (count == 0) throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.RELAY_FAILED,
                )
                lease.stream.write(buffer, 0, count)
            }
        } catch (failure: CancellationException) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Error) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Exception) {
            classifyOrdinaryRelayIoFailure(failure)?.let { classified ->
                selectedFailure = preferRelayFatal(selectedFailure, classified)
            }
        } finally {
            buffer.fill(0)
        }
        selectedFailure?.let { throw it }
        try {
            lease.stream.closeWrite()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: Exception) {
            classifyOrdinaryRelayIoFailure(failure)?.let { throw it }
        }
    }

    private suspend fun copyRemoteToLocal(baseInput: InputStream) {
        val buffer = ByteArray(RELAY_BUFFER_SIZE)
        var remoteInput: InputStream? = null
        var selectedFailure: Throwable? = null
        try {
            remoteInput = createRemoteInput(baseInput)
            while (true) {
                val count = runInterruptible(blockingIoContext) { remoteInput.read(buffer) }
                if (count < 0) break
                if (count == 0) throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.RELAY_FAILED,
                )
                runInterruptible(ioDispatcher) { local.output.write(buffer, 0, count) }
            }
        } catch (failure: CancellationException) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Error) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Exception) {
            classifyOrdinaryRelayIoFailure(failure)?.let { classified ->
                selectedFailure = preferRelayFatal(selectedFailure, classified)
            }
        } finally {
            buffer.fill(0)
            remoteInput?.let { input ->
                try {
                    runInterruptible(blockingIoContext) { input.close() }
                } catch (failure: CancellationException) {
                    selectedFailure = preferRelayFatal(selectedFailure, failure)
                } catch (failure: Error) {
                    selectedFailure = preferRelayFatal(selectedFailure, failure)
                } catch (failure: Exception) {
                    classifyOrdinaryRelayIoFailure(failure)?.let { classified ->
                        selectedFailure = preferRelayFatal(selectedFailure, classified)
                    }
                }
            }
        }
        selectedFailure?.let { throw it }
        try {
            runInterruptible(ioDispatcher) { local.shutdownOutput() }
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: Exception) {
            classifyOrdinaryRelayIoFailure(failure)?.let { throw it }
        }
    }

    private suspend fun copyRemoteToLocalDirect(lease: FrpClientStreamLease) = withContext(ioDispatcher) {
        val buffer = ByteArray(RELAY_BUFFER_SIZE)
        var selectedFailure: Throwable? = null
        try {
            while (true) {
                val count = lease.stream.read(buffer, 0, buffer.size)
                if (count < 0) break
                if (count == 0) throw KotlinFrpcStcpVisitorException(
                    KotlinFrpcStcpVisitorFailure.RELAY_FAILED,
                )
                local.output.write(buffer, 0, count)
            }
        } catch (failure: CancellationException) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Error) {
            selectedFailure = preferRelayFatal(selectedFailure, failure)
        } catch (failure: Exception) {
            classifyOrdinaryRelayIoFailure(failure)?.let { classified ->
                selectedFailure = preferRelayFatal(selectedFailure, classified)
            }
        } finally {
            buffer.fill(0)
        }
        selectedFailure?.let { throw it }
        try {
            local.shutdownOutput()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: Exception) {
            classifyOrdinaryRelayIoFailure(failure)?.let { throw it }
        }
    }

    private suspend fun classifyOrdinaryRelayIoFailure(
        failure: Exception,
    ): KotlinFrpcStcpVisitorException? {
        // Closing the local endpoint and lease can wake blocked I/O with an ordinary failure.
        if (closeStarted.get()) return null
        val classified = failure as? KotlinFrpcStcpVisitorException
            ?: KotlinFrpcStcpVisitorException(KotlinFrpcStcpVisitorFailure.RELAY_FAILED)
        withContext(NonCancellable) { afterOrdinaryPumpFailureClassified() }
        return classified
    }

    private fun createRemoteOutput(baseOutput: OutputStream): OutputStream {
        var output: OutputStream = NonClosingOutputStream(baseOutput)
        if (visitor.useEncryption) output = FrpV1Cfb.encrypting(output, visitor.secretKey)
        if (visitor.useCompression) output = FrpSnappyFramedOutputStream(output)
        return output
    }

    private fun createRemoteInput(baseInput: InputStream): InputStream {
        var input: InputStream = NonClosingInputStream(baseInput)
        if (visitor.useEncryption) input = FrpV1Cfb.decrypting(input, visitor.secretKey)
        if (visitor.useCompression) input = FrpSnappyFramedInputStream(input)
        return input
    }

    private fun closeLocalExpected(): Throwable? =
        try {
            local.close()
            null
        } catch (failure: CancellationException) {
            failure
        } catch (failure: Error) {
            failure
        } catch (_: Exception) {
            // Socket close is best effort after relay invalidation.
            null
        }

    private fun completeRelayOnce(
        fatal: Throwable?,
        ordinary: KotlinFrpcStcpVisitorException?,
    ) {
        if (!completionFinalized.compareAndSet(false, true)) return
        fatalFailure.set(fatal)
        ordinaryFailure.set(ordinary)
        completion.complete(Unit)
    }

    private fun completeLocalStopOnce(fatal: Throwable?) {
        if (!localStopCompletionFinalized.compareAndSet(false, true)) return
        localStopFailure.set(fatal)
        localStopCompletion.complete(Unit)
    }

    private fun findRelayCompletionError(failure: Throwable?): Error? = when {
        failure is Error -> failure
        failure?.cause is Error -> failure.cause as Error
        else -> null
    }

    private companion object {
        const val VISITOR_HANDSHAKE_TIMEOUT_MILLIS = 10_000L
        const val RELAY_BUFFER_SIZE = 16 * 1024
    }

    private enum class RelayRunnerState {
        NEW,
        RUNNING,
        TERMINATED,
    }

    private data class RelayPumpOutcome(val failure: Throwable?)
}

private fun preferRelayFatal(current: Throwable?, candidate: Throwable): Throwable {
    if (current == null || current === candidate) return candidate
    val selected = if (relayFailurePriority(candidate) > relayFailurePriority(current)) candidate else current
    val secondary = if (selected === current) candidate else current
    if (selected !== secondary &&
        !selected.referencesThrowable(secondary) &&
        !secondary.referencesThrowable(selected)
    ) {
        selected.addSuppressed(secondary)
    }
    return selected
}

private fun Throwable.referencesThrowable(target: Throwable): Boolean {
    val seen = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
    val pending = ArrayDeque<Throwable>()
    pending.addLast(this)
    var visited = 0
    while (pending.isNotEmpty()) {
        val current = pending.removeLast()
        if (current === target) return true
        if (!seen.add(current)) continue
        visited += 1
        if (visited >= MAXIMUM_RELAY_FAILURE_GRAPH_NODES) return true
        current.cause?.takeUnless { it === current }?.let(pending::addLast)
        current.suppressed.forEach { suppressed ->
            if (suppressed !== current) pending.addLast(suppressed)
        }
    }
    return false
}

private fun relayFailurePriority(failure: Throwable): Int = when {
    failure is Error -> 3
    failure is CancellationException && !failure.isRelayCloseCancellation() -> 2
    failure is CancellationException -> 0
    else -> 1
}

private fun selectUnexpectedRelayCloseFailure(failure: CancellationException): Throwable? {
    var selected: Throwable? = null
    val seen = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
    val pending = ArrayDeque<Throwable>()
    pending.addLast(failure)
    var visited = 0
    while (pending.isNotEmpty()) {
        val candidate = pending.removeLast()
        if (!seen.add(candidate)) continue
        visited += 1
        if (!candidate.isRelayCloseCancellation()) {
            selected = preferRelayFatal(selected, candidate)
        }
        if (visited >= MAXIMUM_RELAY_FAILURE_GRAPH_NODES) break
        candidate.cause?.takeUnless { it === candidate }?.let(pending::addLast)
        candidate.suppressed.forEach { suppressed ->
            if (suppressed !== candidate) pending.addLast(suppressed)
        }
    }
    return selected
}

private fun Throwable.isRelayCloseCancellation(): Boolean {
    var current: Throwable? = this
    repeat(MAXIMUM_RELAY_FAILURE_CAUSE_DEPTH) {
        if (current is FrpRelayClosedCancellation) return true
        val next = current?.cause
        if (next == null || next === current) return false
        current = next
    }
    return false
}

private const val MAXIMUM_RELAY_FAILURE_CAUSE_DEPTH = 16
private const val MAXIMUM_RELAY_FAILURE_GRAPH_NODES = 64

private class NonClosingInputStream(
    private val delegate: InputStream,
) : InputStream() {
    override fun read(): Int = delegate.read()

    override fun read(destination: ByteArray, offset: Int, length: Int): Int =
        delegate.read(destination, offset, length)

    override fun close() = Unit

    override fun toString(): String = "NonClosingFrpPayloadInputStream"
}

private class NonClosingOutputStream(
    private val delegate: OutputStream,
) : OutputStream() {
    override fun write(value: Int) = delegate.write(value)

    override fun write(source: ByteArray, offset: Int, length: Int) = delegate.write(source, offset, length)

    override fun flush() = delegate.flush()

    override fun close() = Unit

    override fun toString(): String = "NonClosingFrpPayloadOutputStream"
}

private class FrpRelayClosedCancellation : CancellationException("frp visitor relay closed")
