package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpCryptoException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpCryptoFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.crypto.FrpTimestampAuth
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.ClientSpec
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpMessage
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpProtocolException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV1
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpWireV2
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Login
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.NewWorkConn
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Ping
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.Pong
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.ReqWorkConn
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.StartWorkConn
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.BlockingIoThreadGuard
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpMuxConnection
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpMuxConnector
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpMuxStream
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpTransportException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.FrpTransportFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.YamuxStreamBlockingIo
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.handoffFrpOwnedResult
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxProtocolFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxTimeoutFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxTimeoutKind
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxTransportFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.yamux.YamuxTruncatedFrameFailure
import java.io.InputStream
import java.io.OutputStream
import java.security.SecureRandom
import java.util.LinkedHashSet
import java.util.concurrent.CancellationException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
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
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import kotlinx.coroutines.sync.Semaphore

internal fun interface FrpWorkConnectionHandler {
    suspend fun handle(connection: FrpWorkConnection)
}

internal data class FrpControlLifecycleHooks(
    val beforeOpenPublish: suspend (FrpControlIdentity) -> Unit = {},
    val beforeWorkDelivery: suspend (FrpControlIdentity) -> Unit = {},
    val afterWorkRegistration: suspend (FrpControlIdentity) -> Unit = {},
    val beforeClientStreamPermitWait: suspend (FrpControlIdentity) -> Unit = {},
    val afterClientStreamPermitAcquired: suspend (FrpControlIdentity) -> Unit = {},
    val beforeClientStreamOpen: suspend (FrpControlIdentity) -> Unit = {},
    val afterClientStreamOpen: suspend (FrpControlIdentity) -> Unit = {},
    val afterClientStreamRegistration: suspend (FrpControlIdentity) -> Unit = {},
    val afterWorkPermitAcquired: suspend (FrpControlIdentity) -> Unit = {},
    val beforeStatePublish: (FrpControlState) -> Unit = {},
    val afterTerminationRecorded: (FrpControlIdentity) -> Unit = {},
)

internal class FrpWorkConnection internal constructor(
    val identity: FrpControlIdentity,
    val start: StartWorkConn,
    internal val stream: FrpMuxStream,
    private val blockingIo: YamuxStreamBlockingIo,
) : AutoCloseable {
    val input: InputStream
        get() = blockingIo.input

    val output: OutputStream
        get() = blockingIo.output

    val isClosed: Boolean
        get() = blockingIo.isClosed

    suspend fun closeWrite() {
        if (blockingIo.isClosed) throw FrpControlException(FrpControlFailure.CLOSED)
        stream.closeWrite()
    }

    override fun close() {
        blockingIo.close()
    }

    suspend fun awaitClosed() {
        blockingIo.awaitReset()
    }

    override fun toString(): String =
        "FrpWorkConnection(identity=$identity, proxyNamePresent=${start.proxyName.isNotEmpty()}, " +
            "sourcePresent=${start.srcAddr.isNotEmpty() || start.srcPort != 0}, " +
            "destinationPresent=${start.dstAddr.isNotEmpty() || start.dstPort != 0}, " +
            "errorPresent=${start.error.isNotEmpty()}, closed=$isClosed)"
}

internal class FrpClientStreamLease internal constructor(
    val identity: FrpControlIdentity,
    val runId: String,
    val wireProtocol: FrpWireProtocol,
    delegate: FrpMuxStream,
    cleanupScope: CoroutineScope,
    private val onReleased: (FrpClientStreamLease) -> Unit,
) {
    private val closed = AtomicBoolean(false)
    private val resetFailure = AtomicReference<Throwable?>()
    private val resetDone = CompletableDeferred<Unit>()
    private val delegateStream = delegate

    val stream: FrpMuxStream = object : FrpMuxStream {
        override suspend fun read(destination: ByteArray, offset: Int, length: Int): Int {
            checkOpen()
            return delegateStream.read(destination, offset, length)
        }

        override suspend fun write(source: ByteArray, offset: Int, length: Int) {
            checkOpen()
            delegateStream.write(source, offset, length)
        }

        override suspend fun closeWrite() {
            checkOpen()
            delegateStream.closeWrite()
        }

        override suspend fun reset() {
            this@FrpClientStreamLease.reset()
        }

        override fun toString(): String = "FrpClientLeaseStream(closed=${closed.get()})"
    }

    private val resetJob = AtomicReference<Job?>()
    private val cleanupScope = cleanupScope

    val isClosed: Boolean
        get() = closed.get()

    suspend fun reset() {
        beginReset()
        resetDone.await()
        resetFailure.get()?.let { throw it }
    }

    internal fun beginReset() {
        if (!closed.compareAndSet(false, true)) return
        val job = cleanupScope.launch(
            context = CoroutineName("FrpClientStreamLeaseCleanup"),
            start = CoroutineStart.UNDISPATCHED,
        ) {
            var failure: Throwable? = null
            try {
                withContext(NonCancellable) { delegateStream.reset() }
            } catch (resetCancellation: CancellationException) {
                failure = resetCancellation
            } catch (resetError: Error) {
                failure = resetError
            } catch (_: Exception) {
                // Reset is best effort after the logical lease is synchronously invalidated.
            } finally {
                resetFailure.set(failure)
                try {
                    onReleased(this@FrpClientStreamLease)
                } catch (releaseCancellation: CancellationException) {
                    resetFailure.set(preferControlFatalFailure(resetFailure.get(), releaseCancellation))
                } catch (releaseError: Error) {
                    resetFailure.set(preferControlFatalFailure(resetFailure.get(), releaseError))
                } finally {
                    resetDone.complete(Unit)
                }
            }
        }
        check(resetJob.compareAndSet(null, job))
    }

    internal suspend fun awaitReset() {
        if (!closed.get()) return
        resetDone.await()
        resetFailure.get()?.let { throw it }
    }

    override fun toString(): String =
        "FrpClientStreamLease(identity=$identity, runIdPresent=${runId.isNotEmpty()}, " +
            "wireProtocol=$wireProtocol, closed=${closed.get()})"

    private fun checkOpen() {
        if (closed.get()) throw FrpControlException(FrpControlFailure.CLOSED)
    }
}

internal data class FrpControlDiagnostics(
    val started: Boolean,
    val closed: Boolean,
    val runnerActive: Boolean,
    val identity: FrpControlIdentity?,
    val activeWorkConnections: Int,
    val activeClientStreams: Int,
    val availableDataPermits: Int,
    val workFailureCount: Long,
)

internal class FrpControlClient(
    private val config: FrpControlConfig,
    private val connector: FrpMuxConnector,
    parentScope: CoroutineScope,
    private val generation: Long,
    private val workHandler: FrpWorkConnectionHandler = FrpWorkConnectionHandler { it.close() },
    private val unixTimeSource: FrpUnixTimeSource = SystemFrpUnixTimeSource,
    private val monotonicTimeSource: FrpMonotonicTimeSource = SystemFrpMonotonicTimeSource,
    private val delayer: FrpControlDelayer = CoroutineFrpControlDelayer,
    private val secureRandom: SecureRandom = SecureRandom(),
    private val lifecycleHooks: FrpControlLifecycleHooks = FrpControlLifecycleHooks(),
) : AutoCloseable {
    private val started = AtomicBoolean(false)
    private val closed = AtomicBoolean(false)
    private val transportSequence = AtomicLong(0L)
    private val epochSequence = AtomicLong(0L)
    private val activeSession = AtomicReference<ControlSession?>()
    private val reconnectLoginTimedOut = AtomicBoolean(false)
    private val reconnectEstablishmentFailure = AtomicReference<FrpControlFailure?>()
    private val reconnectCleanupFailure = AtomicReference<FrpControlFailure?>()
    private val reconnectEstablishAttemptCount = AtomicLong(0L)
    private val reconnectLoginTimerScheduledCount = AtomicLong(0L)
    private val reconnectLoginTimerFiredCount = AtomicLong(0L)
    private val reconnectSuccessfulOpenCount = AtomicLong(0L)
    private val reconnectCurrentAttempt = AtomicLong(0L)
    private val reconnectLoginTimerScheduledAttempt = AtomicLong(0L)
    private val reconnectLoginTimerFiredAttempt = AtomicLong(0L)
    private val budgetScheduler: ScheduledExecutorService = ScheduledThreadPoolExecutor(
        1,
        ThreadFactory { runnable ->
            Thread(runnable, "FrpControlBudgetTimer").apply { isDaemon = true }
        },
    ).apply {
        removeOnCancelPolicy = true
        executeExistingDelayedTasksAfterShutdownPolicy = false
        continueExistingPeriodicTasksAfterShutdownPolicy = false
    }
    private val runner = AtomicReference<Job?>()
    private val stateLock = Any()
    private val stopped = CompletableDeferred<Unit>()
    private val parentJob = parentScope.coroutineContext[Job]
    private val ownerJob = SupervisorJob(parentJob)
    private val blockingIoDispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(
        config.workWorkerCount + CONTROL_CODEC_PARALLELISM,
        "FrpControlBlockingIo",
    )
    private val blockingIoMarker = ThreadLocal<Boolean>()
    private val blockingIoContext: CoroutineContext =
        blockingIoDispatcher + blockingIoMarker.asContextElement(true)
    private val blockingIoThreadGuard = BlockingIoThreadGuard {
        check(blockingIoMarker.get() == true) {
            "FRP control codecs require the dedicated blocking dispatcher"
        }
    }
    private val workHandlerDispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(
        config.maxActiveWorkConnections,
        "FrpWorkHandlers",
    )
    private val workHandlerContext: CoroutineContext =
        workHandlerDispatcher + blockingIoMarker.asContextElement(true)
    private val scope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) +
            ownerJob +
            CoroutineName("FrpControlClient"),
    )
    private val mutableState = MutableStateFlow<FrpControlState>(
        FrpControlState.Connecting(generation, attempt = 0),
    )

    val state: StateFlow<FrpControlState> = mutableState.asStateFlow()

    fun start(): Job {
        if (closed.get() || !started.compareAndSet(false, true)) {
            throw FrpControlException(FrpControlFailure.CLOSED)
        }
        val job = scope.launch {
            runClient()
        }
        check(runner.compareAndSet(null, job))
        job.invokeOnCompletion { failure ->
            val session = activeSession.getAndSet(null)
            if (failure is CancellationException || closed.get() || !hasActiveParentLease()) {
                transitionToClosed()
            }
            session?.close()
            budgetScheduler.shutdownNow()
            ownerJob.complete()
            stopped.complete(Unit)
        }
        return job
    }

    suspend fun awaitStopped() {
        stopped.await()
    }

    suspend fun openClientStream(expectedIdentity: FrpControlIdentity): FrpClientStreamLease {
        currentCoroutineContext().ensureActive()
        if (!hasActiveOwnerLease() || expectedIdentity.generation != generation) {
            throw FrpControlException(FrpControlFailure.STALE_IDENTITY)
        }
        val session = activeSession.get()
            ?: throw FrpControlException(FrpControlFailure.STALE_IDENTITY)
        if (session.identity != expectedIdentity) {
            throw FrpControlException(FrpControlFailure.STALE_IDENTITY)
        }
        return session.openClientStream(expectedIdentity)
    }

    fun invalidateTransport(): FrpControlIdentity? {
        if (!hasActiveOwnerLease()) return null
        val identity = (state.value as? FrpControlState.Open)?.identity ?: return null
        val session = activeSession.get()
        if (session?.identity == identity) session.invalidateTransport()
        return identity
    }

    fun reconnectCleanupFailure(): FrpControlFailure? = reconnectCleanupFailure.get()

    fun reconnectEstablishmentFailure(): FrpControlFailure? = reconnectEstablishmentFailure.get()

    fun reconnectDiagnostics(): FrpControlReconnectDiagnostics = FrpControlReconnectDiagnostics(
        establishAttemptCount = reconnectEstablishAttemptCount.get(),
        loginTimerScheduledCount = reconnectLoginTimerScheduledCount.get(),
        loginTimerFiredCount = reconnectLoginTimerFiredCount.get(),
        successfulOpenCount = reconnectSuccessfulOpenCount.get(),
        currentAttempt = reconnectCurrentAttempt.get(),
        loginTimerScheduledAttempt = reconnectLoginTimerScheduledAttempt.get(),
        loginTimerFiredAttempt = reconnectLoginTimerFiredAttempt.get(),
    )

    override fun close() {
        if (!transitionToClosed()) return
        budgetScheduler.shutdownNow()
        val session = activeSession.getAndSet(null)
        if (session == null) {
            ownerJob.cancel(ControlClientClosedCancellation())
        } else {
            session.close()
        }
        if (!started.get()) stopped.complete(Unit)
    }

    fun diagnostics(): FrpControlDiagnostics {
        val session = activeSession.get()
        return FrpControlDiagnostics(
            started = started.get(),
            closed = closed.get(),
            runnerActive = runner.get()?.isActive == true,
            identity = session?.identity,
            activeWorkConnections = session?.activeWorkConnectionCount ?: 0,
            activeClientStreams = session?.activeClientStreamCount ?: 0,
            availableDataPermits = session?.availableDataPermits ?: config.maxActiveWorkConnections,
            workFailureCount = session?.workFailureCount ?: 0L,
        )
    }

    override fun toString(): String {
        val diagnostics = diagnostics()
        return "FrpControlClient(generation=$generation, started=${diagnostics.started}, " +
            "closed=${diagnostics.closed}, runnerActive=${diagnostics.runnerActive}, " +
            "identityPresent=${diagnostics.identity != null}, " +
            "activeWorkConnections=${diagnostics.activeWorkConnections}, " +
            "activeClientStreams=${diagnostics.activeClientStreams}, " +
            "availableDataPermits=${diagnostics.availableDataPermits}, " +
            "workFailureCount=${diagnostics.workFailureCount})"
    }

    private suspend fun runClient() {
        var previousRunId = ""
        var hasOpened = false
        var retryAttempt = 0
        var escapingFailure: Throwable? = null
        try {
            while (hasActiveOwnerLease()) {
                if (!hasOpened) publish(FrpControlState.Connecting(generation, attempt = 1))
                val transportIdentity = transportSequence.incrementAndGet()
                val epoch = epochSequence.get() + 1L
                val reconnecting = hasOpened
                val established = try {
                    establish(
                        previousRunId = previousRunId,
                        identity = FrpControlIdentity(generation, epoch, transportIdentity),
                        forceCleanupAfterTimeout = reconnecting,
                    )
                } catch (failure: CancellationException) {
                    throw failure
                } catch (failure: Error) {
                    throw failure
                } catch (failure: Exception) {
                    val mapped = mapControlFailure(
                        failure,
                        FrpControlFailure.TRANSPORT_FAILED,
                        login = true,
                    ) as FrpControlException
                    if (!hasOpened) {
                        publish(FrpControlState.Failed(generation, mapped.failure))
                        return
                    }
                    retryAttempt = incrementAttempt(retryAttempt)
                    val delayMillis = reconnectDelay(retryAttempt)
                    publish(FrpControlState.Retrying(generation, retryAttempt, delayMillis, mapped.failure))
                    delayer.delayMillis(delayMillis)
                    continue
                }

                previousRunId = established.runId
                epochSequence.set(epoch)
                val session = ControlSession(
                    identity = established.identity,
                    runId = established.runId,
                    mux = established.mux,
                    channel = established.channel,
                    config = config,
                    parentScope = scope,
                    workHandler = workHandler,
                    unixTimeSource = unixTimeSource,
                    monotonicTimeSource = monotonicTimeSource,
                    delayer = delayer,
                    blockingIoContext = blockingIoContext,
                    workHandlerContext = workHandlerContext,
                    budgetScheduler = budgetScheduler,
                    blockingIoThreadGuard = blockingIoThreadGuard,
                    lifecycleHooks = lifecycleHooks,
                    isAuthoritative = ::isAuthoritative,
                    onReconnectCleanupFailure = reconnectCleanupFailure::set,
                )
                if (!hasActiveOwnerLease() || !activeSession.compareAndSet(null, session)) {
                    session.close()
                    val completedFailure = shutdownSession(session, null)
                    if (completedFailure is CancellationException || completedFailure is Error) {
                        throw completedFailure
                    }
                    return
                }
                session.start()
                lifecycleHooks.beforeOpenPublish(session.identity)
                if (session.publishOpenIfActive { publish(FrpControlState.Open(session.identity)) }) {
                    if (reconnecting) incrementSaturating(reconnectSuccessfulOpenCount)
                    hasOpened = true
                    retryAttempt = 0
                }

                var awaitFailure: Throwable? = null
                try {
                    session.awaitTermination()
                } catch (failure: CancellationException) {
                    awaitFailure = failure
                } catch (failure: Error) {
                    awaitFailure = failure
                } catch (failure: Exception) {
                    awaitFailure = failure
                }
                val failure = awaitFailure?.let { preferControlFailure(session.finalFailure(), it) }
                    ?: session.finalFailure()
                var retryDelayMillis: Long? = null
                var terminateAfterCleanup = false
                when {
                    failure is Error -> {
                        if (hasActiveOwnerLease()) {
                            publish(FrpControlState.Failed(generation, FrpControlFailure.TRANSPORT_FAILED))
                        } else {
                            transitionToClosed()
                        }
                    }
                    failure is CancellationException || !hasActiveOwnerLease() -> transitionToClosed()
                    !hasOpened -> {
                        val mapped = mapControlFailure(
                            failure,
                            FrpControlFailure.TRANSPORT_FAILED,
                        ) as FrpControlException
                        publish(FrpControlState.Failed(generation, mapped.failure))
                        terminateAfterCleanup = true
                    }
                    else -> {
                        val mapped = mapControlFailure(
                            failure,
                            FrpControlFailure.TRANSPORT_FAILED,
                        ) as FrpControlException
                        retryAttempt = incrementAttempt(retryAttempt)
                        val delayMillis = reconnectDelay(retryAttempt)
                        retryDelayMillis = delayMillis
                        publish(FrpControlState.Retrying(generation, retryAttempt, delayMillis, mapped.failure))
                    }
                }
                val completedFailure = shutdownSession(
                    session = session,
                    current = failure,
                    forceMuxTerminationAfterTimeout = retryDelayMillis != null,
                )
                activeSession.compareAndSet(session, null)
                when (completedFailure) {
                    is CancellationException -> throw completedFailure
                    is Error -> {
                        if (hasActiveOwnerLease()) {
                            publish(FrpControlState.Failed(generation, FrpControlFailure.TRANSPORT_FAILED))
                        }
                        throw completedFailure
                    }
                }
                if (terminateAfterCleanup) return
                if (!hasActiveOwnerLease()) return
                delayer.delayMillis(checkNotNull(retryDelayMillis))
            }
        } catch (failure: CancellationException) {
            escapingFailure = failure
            if (!closed.get()) {
                transitionToClosed()
            }
            throw failure
        } catch (failure: Error) {
            escapingFailure = failure
            if (!closed.get()) {
                publish(FrpControlState.Failed(generation, FrpControlFailure.TRANSPORT_FAILED))
            }
            throw failure
        } catch (failure: Exception) {
            escapingFailure = failure
            if (!closed.get()) {
                publish(FrpControlState.Failed(generation, FrpControlFailure.TRANSPORT_FAILED))
            }
            throw failure
        } finally {
            activeSession.getAndSet(null)?.let { session ->
                session.close()
                val completedFailure = shutdownSession(session, escapingFailure)
                if (completedFailure !== escapingFailure &&
                    (completedFailure is CancellationException || completedFailure is Error)
                ) {
                    throw completedFailure
                }
            }
        }
    }

    private suspend fun shutdownSession(
        session: ControlSession,
        current: Throwable?,
        forceMuxTerminationAfterTimeout: Boolean = false,
    ): Throwable {
        var shutdownFailure: Throwable? = null
        try {
            withContext(NonCancellable) {
                session.shutdown(forceMuxTerminationAfterTimeout)
            }
        } catch (failure: CancellationException) {
            shutdownFailure = preferControlFailure(shutdownFailure, failure)
        } catch (failure: Error) {
            shutdownFailure = preferControlFailure(shutdownFailure, failure)
        }
        val completedFailure = shutdownFailure?.let { failure ->
            preferControlFailure(session.finalFailure(), failure)
        } ?: session.finalFailure()
        return current?.let { failure -> preferControlFailure(failure, completedFailure) } ?: completedFailure
    }

    private suspend fun establish(
        previousRunId: String,
        identity: FrpControlIdentity,
        forceCleanupAfterTimeout: Boolean,
    ): EstablishedControl {
        var mux: FrpMuxConnection? = null
        var blockingIo: YamuxStreamBlockingIo? = null
        val reconnectAttempt = if (forceCleanupAfterTimeout) {
            incrementSaturating(reconnectEstablishAttemptCount).also(reconnectCurrentAttempt::set)
        } else {
            0L
        }
        try {
            if (forceCleanupAfterTimeout) {
                reconnectEstablishmentFailure.set(FrpControlFailure.RECONNECT_CONNECTING_TRANSPORT)
            }
            mux = connector.connect()
            if (forceCleanupAfterTimeout) {
                reconnectEstablishmentFailure.set(FrpControlFailure.RECONNECT_OPENING_CONTROL_STREAM)
            }
            val controlStream = mux.openStream()
            blockingIo = YamuxStreamBlockingIo(controlStream, scope, blockingIoThreadGuard)
            if (forceCleanupAfterTimeout) {
                reconnectEstablishmentFailure.set(
                    if (reconnectLoginTimedOut.get()) {
                        FrpControlFailure.RECONNECT_WAITING_LOGIN_AFTER_TIMEOUT
                    } else {
                        FrpControlFailure.RECONNECT_WAITING_LOGIN
                    },
                )
            }
            val login = buildLogin(previousRunId)
            val loginTimeoutMillis = if (forceCleanupAfterTimeout) {
                config.reconnectLoginTimeoutMillis
            } else {
                config.loginTimeoutMillis
            }
            val loggedIn = withControlBudgetTimeout(
                scheduler = budgetScheduler,
                timeoutMillis = loginTimeoutMillis,
                timeoutFailure = FrpControlFailure.LOGIN_TIMEOUT,
                onTimerScheduled = if (forceCleanupAfterTimeout) {
                    {
                        reconnectLoginTimerScheduledAttempt.set(reconnectAttempt)
                        incrementSaturating(reconnectLoginTimerScheduledCount)
                    }
                } else {
                    null
                },
                onTimerFired = if (forceCleanupAfterTimeout) {
                    {
                        reconnectLoginTimerFiredAttempt.set(reconnectAttempt)
                        incrementSaturating(reconnectLoginTimerFiredCount)
                    }
                } else {
                    null
                },
                onTimeout = {
                    if (forceCleanupAfterTimeout) {
                        reconnectLoginTimedOut.set(true)
                        reconnectEstablishmentFailure.set(FrpControlFailure.RECONNECT_ABORTING_LOGIN_TIMEOUT)
                    }
                    blockingIo.close()
                },
            ) {
                runControlCodec {
                    FrpControlBootstrap(config, secureRandom).exchange(
                        input = blockingIo.input,
                        output = blockingIo.output,
                        login = login,
                        closeOwner = blockingIo,
                    )
                }
            }
            reconnectLoginTimedOut.set(false)
            reconnectEstablishmentFailure.set(null)
            return EstablishedControl(identity, mux, loggedIn.runId, loggedIn.channel)
        } catch (failure: CancellationException) {
            closeEstablishedResources(blockingIo, mux, failure, forceCleanupAfterTimeout)
        } catch (failure: Error) {
            closeEstablishedResources(blockingIo, mux, failure, forceCleanupAfterTimeout)
        } catch (failure: Exception) {
            val selected = try {
                currentCoroutineContext().ensureActive()
                mapControlFailure(failure, FrpControlFailure.TRANSPORT_FAILED, login = true)
            } catch (cancellation: CancellationException) {
                cancellation
            }
            closeEstablishedResources(
                blockingIo,
                mux,
                selected,
                forceCleanupAfterTimeout,
            )
        }
    }

    private suspend fun <T> runControlCodec(block: () -> T): T =
        runInterruptible(blockingIoContext, block)

    private suspend fun closeEstablishedResources(
        blockingIo: YamuxStreamBlockingIo?,
        mux: FrpMuxConnection?,
        failure: Throwable,
        forceCleanupAfterTimeout: Boolean,
    ): Nothing {
        var selected = failure
        fun selectFatal(candidate: Throwable) {
            selected = preferControlFatalFailure(selected, candidate)
        }
        if (blockingIo != null) {
            try {
                blockingIo.close()
            } catch (closeFailure: CancellationException) {
                selectFatal(closeFailure)
            } catch (closeFailure: Error) {
                selectFatal(closeFailure)
            } catch (_: Exception) {
                // The safe establishment failure remains authoritative.
            }
        }
        if (mux != null) {
            try {
                mux.close()
            } catch (closeFailure: CancellationException) {
                selectFatal(closeFailure)
            } catch (closeFailure: Error) {
                selectFatal(closeFailure)
            } catch (_: Exception) {
                // The safe establishment failure remains authoritative.
            }
        }
        if (forceCleanupAfterTimeout) {
            reconnectCleanupFailure.set(FrpControlFailure.RECONNECT_CLOSING_FAILED_ESTABLISHMENT)
        }
        val watchdogFailure = AtomicReference<Throwable?>()
        val cleanupMux = mux
        val cleanupWatchdogOwner = if (forceCleanupAfterTimeout && cleanupMux != null) SupervisorJob() else null
        val cleanupWatchdog = cleanupWatchdogOwner?.let { owner ->
            CoroutineScope(
                Dispatchers.Default + owner + CoroutineName("FrpControlFailedEstablishmentCleanupWatchdog"),
            ).launch {
                delay(config.reconnectCleanupTimeoutMillis)
                try {
                    cleanupMux?.forceTerminationAfterClose()
                } catch (closeFailure: CancellationException) {
                    watchdogFailure.set(closeFailure)
                } catch (closeFailure: Error) {
                    watchdogFailure.set(closeFailure)
                } catch (_: Exception) {
                    // The safe establishment failure remains authoritative.
                }
            }
        }
        try {
            withContext(NonCancellable) {
                if (blockingIo != null) {
                    try {
                        blockingIo.awaitReset()
                    } catch (closeFailure: CancellationException) {
                        selectFatal(closeFailure)
                    } catch (closeFailure: Error) {
                        selectFatal(closeFailure)
                    } catch (_: Exception) {
                        // The safe establishment failure remains authoritative.
                    }
                }
                if (mux != null) {
                    try {
                        mux.awaitTermination()
                    } catch (closeFailure: CancellationException) {
                        selectFatal(closeFailure)
                    } catch (closeFailure: Error) {
                        selectFatal(closeFailure)
                    } catch (_: Exception) {
                        // The safe establishment failure remains authoritative.
                    }
                }
            }
        } finally {
            withContext(NonCancellable) {
                cleanupWatchdogOwner?.cancel()
                cleanupWatchdog?.join()
            }
            watchdogFailure.get()?.let(::selectFatal)
            if (forceCleanupAfterTimeout) reconnectCleanupFailure.set(null)
            if (forceCleanupAfterTimeout) reconnectEstablishmentFailure.set(null)
        }
        throw selected
    }

    private fun buildLogin(previousRunId: String): Login {
        val timestamp = unixTimeSource.nowSeconds()
        return Login(
            version = config.version,
            hostname = config.hostname,
            os = config.os,
            arch = config.arch,
            user = config.user,
            privilegeKey = FrpTimestampAuth.key(config.token, timestamp),
            timestamp = timestamp,
            runId = previousRunId,
            clientId = config.clientId,
            metas = config.metas,
            clientSpec = ClientSpec(),
            poolCount = 1,
        )
    }

    private suspend fun delayBeforeRetry(attempt: Int, failure: FrpControlFailure) {
        val delayMillis = reconnectDelay(attempt)
        publish(FrpControlState.Retrying(generation, attempt, delayMillis, failure))
        delayer.delayMillis(delayMillis)
    }

    private fun reconnectDelay(attempt: Int): Long {
        var value = config.reconnectInitialDelayMillis
        repeat((attempt - 1).coerceAtLeast(0)) {
            if (value >= config.reconnectMaximumDelayMillis) return config.reconnectMaximumDelayMillis
            value = (value * 2L).coerceAtMost(config.reconnectMaximumDelayMillis)
        }
        return value
    }

    private fun incrementAttempt(current: Int): Int =
        if (current == Int.MAX_VALUE) current else current + 1

    private fun isAuthoritative(identity: FrpControlIdentity): Boolean {
        if (!hasActiveOwnerLease() || identity.generation != generation) return false
        return activeSession.get()?.identity == identity
    }

    private fun hasActiveParentLease(): Boolean = parentJob?.isActive != false && ownerJob.isActive

    private fun hasActiveOwnerLease(): Boolean = hasActiveParentLease() && !closed.get()

    private fun transitionToClosed(): Boolean = synchronized(stateLock) {
        val changed = closed.compareAndSet(false, true)
        if (mutableState.value !is FrpControlState.Closed) {
            mutableState.value = FrpControlState.Closed(generation)
        }
        changed
    }

    private fun publish(value: FrpControlState) {
        if (value !is FrpControlState.Closed) {
            if (!hasActiveOwnerLease()) return
            lifecycleHooks.beforeStatePublish(value)
        }
        synchronized(stateLock) {
            if (mutableState.value is FrpControlState.Closed) return
            if (value is FrpControlState.Closed || hasActiveOwnerLease()) mutableState.value = value
        }
    }

    private data class EstablishedControl(
        val identity: FrpControlIdentity,
        val mux: FrpMuxConnection,
        val runId: String,
        val channel: FrpMessageChannel,
    ) {
        override fun toString(): String =
            "EstablishedControl(identity=$identity, runIdPresent=${runId.isNotEmpty()}, channel=$channel)"
    }

    private companion object {
        const val CONTROL_CODEC_PARALLELISM = 2
    }
}

private class ControlSession(
    val identity: FrpControlIdentity,
    private val runId: String,
    private val mux: FrpMuxConnection,
    private val channel: FrpMessageChannel,
    private val config: FrpControlConfig,
    parentScope: CoroutineScope,
    private val workHandler: FrpWorkConnectionHandler,
    private val unixTimeSource: FrpUnixTimeSource,
    private val monotonicTimeSource: FrpMonotonicTimeSource,
    private val delayer: FrpControlDelayer,
    private val blockingIoContext: CoroutineContext,
    private val workHandlerContext: CoroutineContext,
    private val budgetScheduler: ScheduledExecutorService,
    private val blockingIoThreadGuard: BlockingIoThreadGuard,
    private val lifecycleHooks: FrpControlLifecycleHooks,
    private val isAuthoritative: (FrpControlIdentity) -> Boolean,
    private val onReconnectCleanupFailure: (FrpControlFailure?) -> Unit,
) : AutoCloseable {
    private val started = AtomicBoolean(false)
    private val closing = AtomicBoolean(false)
    private val terminationSignal = CompletableDeferred<Unit>()
    private val openDecision = CompletableDeferred<Boolean>()
    private val physicalCloseStarted = AtomicBoolean(false)
    private val physicalCloseCompletion = CompletableDeferred<Unit>()
    private val physicalCloseFailure = AtomicReference<Throwable?>()
    private val lifecycleLock = Any()
    private var lifecycle = SessionLifecycle.STARTING
    private var selectedFailure: Throwable? = null
    private val writerQueue = Channel<OutgoingMessage>(config.writerQueueCapacity)
    private val workQueue = Channel<Unit>(config.workQueueCapacity)
    private val sessionJob = SupervisorJob(parentScope.coroutineContext[Job])
    private val scope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) +
            sessionJob +
            CoroutineName("FrpControlSession"),
    )
    private val jobsLock = Any()
    private val jobs = mutableListOf<Job>()
    private val activeWork = LinkedHashSet<ActiveWorkLease>()
    private val handlerJobs = LinkedHashSet<Job>()
    private val workCleanupFlights = LinkedHashSet<ActiveWorkLease>()
    private val activeClientStreams = LinkedHashSet<FrpClientStreamLease>()
    private val activeClientOpenCalls = LinkedHashSet<CompletableDeferred<Unit>>()
    private val activeDataPermits = Semaphore(config.maxActiveWorkConnections)
    private val clientStreamCleanupJob = SupervisorJob()
    private val clientStreamCleanupScope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) +
            clientStreamCleanupJob +
            CoroutineName("FrpClientStreamCleanup"),
    )
    private val workCleanupJob = SupervisorJob()
    private val workCleanupScope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) +
            workCleanupJob +
            CoroutineName("FrpWorkConnectionCleanup"),
    )
    private val workFailures = AtomicLong(0L)
    private val lastPongMillis = AtomicLong(monotonicTimeSource.nowMillis())
    private val firstPingMillis = CompletableDeferred<Long>()

    val activeWorkConnectionCount: Int
        get() = synchronized(lifecycleLock) { activeWork.size }

    val activeClientStreamCount: Int
        get() = synchronized(lifecycleLock) { activeClientStreams.size }

    val availableDataPermits: Int
        get() = activeDataPermits.availablePermits

    val workFailureCount: Long
        get() = workFailures.get()

    fun start() {
        if (!started.compareAndSet(false, true)) throw FrpControlException(FrpControlFailure.CLOSED)
        launchSession { runWriter() }
        repeat(config.workWorkerCount) { workerIndex ->
            launchSession(CoroutineName("FrpControlWorkWorker-$workerIndex")) { runWorkWorker() }
        }
        launchSession { runReader() }
        launchSession { runMuxMonitor() }
        if (config.heartbeatIntervalMillis > 0L) {
            launchSession { runHeartbeatSender() }
            if (config.heartbeatTimeoutMillis > 0L) launchSession { runHeartbeatMonitor() }
        }
    }

    suspend fun awaitTermination() {
        terminationSignal.await()
    }

    suspend fun openClientStream(expectedIdentity: FrpControlIdentity): FrpClientStreamLease {
        val openCall = registerClientOpenCall(expectedIdentity)
        var permitOwned = false
        var stream: FrpMuxStream? = null
        var lease: FrpClientStreamLease? = null
        try {
            lifecycleHooks.beforeClientStreamPermitWait(identity)
            activeDataPermits.acquire()
            permitOwned = true
            lifecycleHooks.afterClientStreamPermitAcquired(identity)
            currentCoroutineContext().ensureActive()
            lifecycleHooks.beforeClientStreamOpen(identity)
            requireOpenClientStreamOwner(expectedIdentity)
            val openedStream = mux.openStream()
            stream = openedStream
            lifecycleHooks.afterClientStreamOpen(identity)
            requireOpenClientStreamOwner(expectedIdentity)

            val created = FrpClientStreamLease(
                identity = identity,
                runId = runId,
                wireProtocol = config.wireProtocol,
                delegate = openedStream,
                cleanupScope = clientStreamCleanupScope,
                onReleased = ::releaseClientStream,
            )
            lease = created
            stream = null
            if (!registerClientStream(created)) {
                created.reset()
                throw FrpControlException(FrpControlFailure.STALE_IDENTITY)
            }
            permitOwned = false
            lifecycleHooks.afterClientStreamRegistration(identity)
            if (!isRegisteredClientStream(created)) {
                created.reset()
                throw FrpControlException(FrpControlFailure.STALE_IDENTITY)
            }
            val handedOff = handoffFrpOwnedResult(created, FrpClientStreamLease::beginReset)
            lease = null
            return handedOff
        } catch (failure: CancellationException) {
            cleanupRejectedClientStream(lease, stream, failure)
        } catch (failure: Error) {
            cleanupRejectedClientStream(lease, stream, failure)
        } catch (failure: Exception) {
            val mapped = if (failure is FrpControlException) {
                failure
            } else {
                FrpControlException(FrpControlFailure.CLIENT_STREAM_FAILED)
            }
            cleanupRejectedClientStream(lease, stream, mapped)
        } finally {
            if (permitOwned) releaseDataPermit()
            finishClientOpenCall(openCall)
        }
    }

    fun publishOpenIfActive(publish: () -> Unit): Boolean = synchronized(lifecycleLock) {
        if (lifecycle != SessionLifecycle.STARTING || closing.get() || !isAuthoritative(identity)) {
            openDecision.complete(false)
            return@synchronized false
        }
        lifecycle = SessionLifecycle.OPEN
        publish()
        openDecision.complete(true)
        true
    }

    fun finalFailure(): Throwable = synchronized(lifecycleLock) {
        selectedFailure ?: FrpControlException(FrpControlFailure.CLOSED)
    }

    override fun close() {
        recordFailure(FrpControlException(FrpControlFailure.CLOSED))
    }

    fun invalidateTransport(): Boolean =
        recordFailure(FrpControlException(FrpControlFailure.TRANSPORT_FAILED))

    suspend fun shutdown(forceMuxTerminationAfterTimeout: Boolean = false) {
        var shutdownFailure: Throwable? = null
        if (forceMuxTerminationAfterTimeout) {
            onReconnectCleanupFailure(FrpControlFailure.RECONNECT_CLOSING_TRANSPORT)
        }
        try {
            closePhysicalTransportAndAwait()
        } catch (failure: CancellationException) {
            shutdownFailure = preferControlFailure(shutdownFailure, failure)
            recordFailure(failure)
        } catch (failure: Error) {
            shutdownFailure = preferControlFailure(shutdownFailure, failure)
            recordFailure(failure)
        }
        val cleanupWatchdogOwner = if (forceMuxTerminationAfterTimeout) SupervisorJob() else null
        val cleanupWatchdog = cleanupWatchdogOwner?.let { owner ->
            CoroutineScope(
                Dispatchers.Default +
                    owner +
                    CoroutineName("FrpControlReconnectCleanupWatchdog"),
            ).launch {
                delay(config.reconnectCleanupTimeoutMillis)
                try {
                    mux.forceTerminationAfterClose()
                } catch (failure: CancellationException) {
                    recordFailure(failure)
                } catch (failure: Error) {
                    recordFailure(failure)
                } catch (failure: Exception) {
                    recordFailure(mapControlFailure(failure, FrpControlFailure.TRANSPORT_FAILED))
                }
            }
        }
        try {
            if (forceMuxTerminationAfterTimeout) {
                onReconnectCleanupFailure(FrpControlFailure.RECONNECT_JOINING_JOBS)
            }
            val activeHandlers = synchronized(lifecycleLock) { handlerJobs.toList() }
            sessionJob.cancel(ControlSessionClosedCancellation())
            val childJobs = synchronized(jobsLock) { jobs.toList() }
            (childJobs + activeHandlers).distinct().joinAll()
            if (forceMuxTerminationAfterTimeout) {
                onReconnectCleanupFailure(FrpControlFailure.RECONNECT_CLOSING_WORK)
            }
            closeActiveWorkConnections()
            awaitWorkCleanup()?.let { failure ->
                shutdownFailure = preferControlFailure(shutdownFailure, failure)
                recordFailure(failure)
            }
            workCleanupJob.cancel()
            if (forceMuxTerminationAfterTimeout) {
                onReconnectCleanupFailure(FrpControlFailure.RECONNECT_CLOSING_CLIENT_STREAMS)
            }
            awaitClientOpenCalls()
            awaitClientStreamShutdown()
            clientStreamCleanupJob.cancel()
            if (forceMuxTerminationAfterTimeout) {
                onReconnectCleanupFailure(FrpControlFailure.RECONNECT_CLOSING_YAMUX)
            }
            try {
                mux.awaitTermination()
            } catch (failure: CancellationException) {
                shutdownFailure = preferControlFailure(shutdownFailure, failure)
                recordFailure(failure)
            } catch (failure: Error) {
                shutdownFailure = preferControlFailure(shutdownFailure, failure)
                recordFailure(failure)
            } catch (_: Exception) {
                // The selected control failure remains authoritative for ordinary mux close I/O.
            }
            try {
                runControlCodec { channel.close() }
            } catch (failure: CancellationException) {
                shutdownFailure = preferControlFailure(shutdownFailure, failure)
                recordFailure(failure)
            } catch (failure: Error) {
                shutdownFailure = preferControlFailure(shutdownFailure, failure)
                recordFailure(failure)
            } catch (_: Exception) {
                // The selected control failure remains authoritative.
            }
        } finally {
            cleanupWatchdogOwner?.cancel()
            cleanupWatchdog?.join()
            if (forceMuxTerminationAfterTimeout) onReconnectCleanupFailure(null)
        }
        val completedFailure = preferControlFailure(shutdownFailure, finalFailure())
        if (completedFailure is CancellationException || completedFailure is Error) throw completedFailure
    }

    override fun toString(): String =
        "ControlSession(identity=$identity, runIdPresent=${runId.isNotEmpty()}, " +
            "started=${started.get()}, closing=${closing.get()}, " +
            "activeWorkConnections=$activeWorkConnectionCount, workFailureCount=${workFailures.get()})"

    private suspend fun <T> runControlCodec(block: () -> T): T =
        runInterruptible(blockingIoContext, block)

    private fun launchSession(
        context: CoroutineName = CoroutineName("FrpControlSessionWorker"),
        block: suspend () -> Unit,
    ) {
        val job = scope.launch(context) {
            try {
                block()
            } catch (failure: CancellationException) {
                if (!closing.get()) recordFailure(failure)
            } catch (failure: Error) {
                recordFailure(failure)
            } catch (failure: Exception) {
                recordFailure(mapControlFailure(failure, FrpControlFailure.TRANSPORT_FAILED))
            }
        }
        synchronized(jobsLock) { jobs += job }
    }

    private suspend fun runReader() {
        while (!closing.get()) {
            val message = try {
                runControlCodec { channel.readMessage() }
            } catch (failure: CancellationException) {
                throw failure
            } catch (failure: Error) {
                throw failure
            } catch (failure: Exception) {
                throw mapControlFailure(failure, FrpControlFailure.READ_FAILED)
            } ?: throw FrpControlException(FrpControlFailure.READ_FAILED)
            when (message) {
                is Pong -> {
                    if (message.error.isNotEmpty()) {
                        throw FrpControlException(FrpControlFailure.PROTOCOL_FAILED)
                    }
                    lastPongMillis.set(monotonicTimeSource.nowMillis())
                }
                is ReqWorkConn -> {
                    if (!workQueue.trySend(Unit).isSuccess) {
                        throw FrpControlException(FrpControlFailure.QUEUE_OVERFLOW)
                    }
                }
                else -> throw FrpControlException(FrpControlFailure.PROTOCOL_FAILED)
            }
        }
    }

    private suspend fun runWriter() {
        try {
            for (outgoing in writerQueue) {
                try {
                    runControlCodec { channel.writeMessage(outgoing.message) }
                    outgoing.result.complete(Unit)
                } catch (failure: CancellationException) {
                    outgoing.result.completeExceptionally(failure)
                    throw failure
                } catch (failure: Error) {
                    outgoing.result.completeExceptionally(failure)
                    throw failure
                } catch (failure: Exception) {
                    val mapped = mapControlFailure(failure, FrpControlFailure.WRITE_FAILED)
                    outgoing.result.completeExceptionally(mapped)
                    throw mapped
                }
            }
        } finally {
            val failure = finalFailure()
            while (true) {
                val pending = writerQueue.tryReceive().getOrNull() ?: break
                pending.result.completeExceptionally(failure)
            }
        }
    }

    private suspend fun send(message: FrpMessage) {
        if (closing.get()) throw finalFailure()
        val outgoing = OutgoingMessage(message)
        writerQueue.send(outgoing)
        outgoing.result.await()
    }

    private suspend fun runHeartbeatSender() {
        while (!closing.get()) {
            val timestamp = unixTimeSource.nowSeconds()
            send(
                Ping(
                    privilegeKey = if (config.authenticateHeartbeats) {
                        FrpTimestampAuth.key(config.token, timestamp)
                    } else {
                        ""
                    },
                    timestamp = if (config.authenticateHeartbeats) timestamp else 0L,
                ),
            )
            firstPingMillis.complete(monotonicTimeSource.nowMillis())
            delayer.delayMillis(config.heartbeatIntervalMillis)
        }
    }

    private suspend fun runHeartbeatMonitor() {
        val checkInterval = minOf(1_000L, config.heartbeatIntervalMillis).coerceAtLeast(1L)
        val firstPing = firstPingMillis.await()
        delayer.delayMillis(config.heartbeatTimeoutMillis)
        while (!closing.get()) {
            val lastResponse = maxOf(firstPing, lastPongMillis.get())
            if (monotonicTimeSource.nowMillis() - lastResponse > config.heartbeatTimeoutMillis) {
                throw FrpControlException(FrpControlFailure.HEARTBEAT_TIMEOUT)
            }
            delayer.delayMillis(checkInterval)
        }
    }

    private suspend fun runMuxMonitor() {
        try {
            mux.awaitTermination()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (failure: Exception) {
            throw mapControlFailure(failure, FrpControlFailure.TRANSPORT_FAILED)
        }
        if (!closing.get()) throw FrpControlException(FrpControlFailure.TRANSPORT_FAILED)
    }

    private suspend fun runWorkWorker() {
        for (ignored in workQueue) {
            if (closing.get()) return
            processWorkRequest()
        }
    }

    private suspend fun processWorkRequest() {
        var permitOwned = false
        var blockingIo: YamuxStreamBlockingIo? = null
        var workConnection: FrpWorkConnection? = null
        var activeLease: ActiveWorkLease? = null
        try {
            activeDataPermits.acquire()
            permitOwned = true
            lifecycleHooks.afterWorkPermitAcquired(identity)
            currentCoroutineContext().ensureActive()
            if (closing.get()) return
            if (!isAuthoritative(identity)) return
            val stream = mux.openStream()
            if (!isAuthoritative(identity)) {
                stream.reset()
                return
            }
            val workIo = YamuxStreamBlockingIo(stream, scope, blockingIoThreadGuard)
            blockingIo = workIo
            val startMessage = withControlBudgetTimeout(
                scheduler = budgetScheduler,
                timeoutMillis = config.workHandshakeTimeoutMillis,
                timeoutFailure = FrpControlFailure.WORK_FAILED,
            ) {
                runControlCodec {
                    if (config.wireProtocol == FrpWireProtocol.V2) {
                        FrpWireV2.writeMagic(workIo.output)
                    }
                    val timestamp = unixTimeSource.nowSeconds()
                    writeWorkMessage(
                        workIo.output,
                        NewWorkConn(
                            runId = runId,
                            privilegeKey = if (config.authenticateNewWorkConnections) {
                                FrpTimestampAuth.key(config.token, timestamp)
                            } else {
                                ""
                            },
                            timestamp = if (config.authenticateNewWorkConnections) timestamp else 0L,
                        ),
                    )
                    val response = readWorkMessage(workIo.input) as? StartWorkConn
                        ?: throw FrpControlException(FrpControlFailure.WORK_FAILED)
                    if (response.error.isNotEmpty()) {
                        throw FrpControlException(FrpControlFailure.WORK_FAILED)
                    }
                    response
                }
            }
            workConnection = FrpWorkConnection(
                identity = identity,
                start = startMessage.withFrpUserPrefixRemoved(config.user),
                stream = stream,
                blockingIo = workIo,
            )
            if (!openDecision.await() || !isAuthoritative(identity)) return
            lifecycleHooks.beforeWorkDelivery(identity)
            val lease = ActiveWorkLease(workConnection)
            activeLease = lease
            workConnection = null
            blockingIo = null
            permitOwned = false
            val handlerJob = scope.launch(
                context = CoroutineName("FrpControlWorkHandler"),
                start = CoroutineStart.LAZY,
            ) {
                runWorkHandler(lease)
            }
            lease.attach(handlerJob)
            handlerJob.invokeOnCompletion { lease.beginCleanup() }
            if (!registerWorkHandler(lease)) {
                handlerJob.cancel()
                return
            }
            lifecycleHooks.afterWorkRegistration(identity)
            if (!handlerJob.start()) return
            activeLease = null
        } catch (failure: CancellationException) {
            if (hasLiveSessionOwner()) throw failure
        } catch (failure: Error) {
            recordFailure(failure)
        } catch (_: Exception) {
            workFailures.incrementAndGet()
        } finally {
            activeLease?.beginCleanup()
            workConnection?.close() ?: blockingIo?.close()
            if (permitOwned) releaseDataPermit()
        }
    }

    private fun registerWorkHandler(lease: ActiveWorkLease): Boolean = synchronized(lifecycleLock) {
        if (lifecycle != SessionLifecycle.OPEN || !isAuthoritative(identity) || lease.isReleased) {
            false
        } else {
            activeWork += lease
            handlerJobs += lease.job
            true
        }
    }

    private fun hasLiveSessionOwner(): Boolean = synchronized(lifecycleLock) {
        lifecycle != SessionLifecycle.TERMINATED && !closing.get() && isAuthoritative(identity)
    }

    private fun hasOpenHandlerOwner(): Boolean = synchronized(lifecycleLock) {
        lifecycle == SessionLifecycle.OPEN && !closing.get() && isAuthoritative(identity)
    }

    private suspend fun runWorkHandler(lease: ActiveWorkLease) {
        val deliver = synchronized(lifecycleLock) {
            lifecycle == SessionLifecycle.OPEN &&
                activeWork.contains(lease) &&
                handlerJobs.contains(lease.job) &&
                isAuthoritative(identity)
        }
        if (!deliver) return
        try {
            withContext(workHandlerContext) { workHandler.handle(lease.connection) }
        } catch (failure: CancellationException) {
            if (hasOpenHandlerOwner()) workFailures.incrementAndGet()
        } catch (failure: Error) {
            recordFailure(failure)
        } catch (_: Exception) {
            workFailures.incrementAndGet()
        }
    }

    private fun releaseDataPermit() {
        activeDataPermits.release()
    }

    private fun hasOpenClientStreamOwner(): Boolean = synchronized(lifecycleLock) {
        lifecycle == SessionLifecycle.OPEN && !closing.get() && isAuthoritative(identity)
    }

    private fun registerClientOpenCall(expectedIdentity: FrpControlIdentity): CompletableDeferred<Unit> =
        synchronized(lifecycleLock) {
            if (expectedIdentity != identity || lifecycle != SessionLifecycle.OPEN || closing.get() ||
                !isAuthoritative(identity)
            ) {
                throw FrpControlException(FrpControlFailure.STALE_IDENTITY)
            }
            CompletableDeferred<Unit>().also(activeClientOpenCalls::add)
        }

    private fun finishClientOpenCall(openCall: CompletableDeferred<Unit>) {
        synchronized(lifecycleLock) { activeClientOpenCalls.remove(openCall) }
        openCall.complete(Unit)
    }

    private suspend fun awaitClientOpenCalls() {
        val calls = synchronized(lifecycleLock) { activeClientOpenCalls.toList() }
        calls.forEach { withContext(NonCancellable) { it.await() } }
    }

    private fun requireOpenClientStreamOwner(expectedIdentity: FrpControlIdentity) {
        if (expectedIdentity != identity || !hasOpenClientStreamOwner()) {
            throw FrpControlException(FrpControlFailure.STALE_IDENTITY)
        }
    }

    private fun registerClientStream(lease: FrpClientStreamLease): Boolean = synchronized(lifecycleLock) {
        if (lifecycle != SessionLifecycle.OPEN || closing.get() || !isAuthoritative(identity) || lease.isClosed) {
            false
        } else {
            activeClientStreams += lease
            true
        }
    }

    private fun isRegisteredClientStream(lease: FrpClientStreamLease): Boolean = synchronized(lifecycleLock) {
        lifecycle == SessionLifecycle.OPEN &&
            !closing.get() &&
            isAuthoritative(identity) &&
            activeClientStreams.contains(lease) &&
            !lease.isClosed
    }

    private fun releaseClientStream(lease: FrpClientStreamLease) {
        val removed = synchronized(lifecycleLock) { activeClientStreams.remove(lease) }
        if (removed) releaseDataPermit()
    }

    private suspend fun cleanupRejectedClientStream(
        lease: FrpClientStreamLease?,
        stream: FrpMuxStream?,
        failure: Throwable,
    ): Nothing {
        var selected = failure
        if (lease != null) {
            try {
                lease.reset()
            } catch (cleanupFailure: CancellationException) {
                selected = preferControlFatalFailure(selected, cleanupFailure)
            } catch (cleanupFailure: Error) {
                selected = preferControlFatalFailure(selected, cleanupFailure)
            } catch (_: Exception) {
                // The typed open failure remains authoritative.
            }
        } else if (stream != null) {
            try {
                withContext(NonCancellable) { stream.reset() }
            } catch (cleanupFailure: CancellationException) {
                selected = preferControlFatalFailure(selected, cleanupFailure)
            } catch (cleanupFailure: Error) {
                selected = preferControlFatalFailure(selected, cleanupFailure)
            } catch (_: Exception) {
                // The typed open failure remains authoritative.
            }
        }
        throw selected
    }

    private fun closeActiveClientStreams() {
        val leases = synchronized(lifecycleLock) { activeClientStreams.toList() }
        leases.forEach(FrpClientStreamLease::beginReset)
    }

    private suspend fun awaitClientStreamShutdown() {
        val leases = synchronized(lifecycleLock) { activeClientStreams.toList() }
        var fatalFailure: Throwable? = null
        leases.forEach { lease ->
            lease.beginReset()
            try {
                withContext(NonCancellable) { lease.awaitReset() }
            } catch (failure: CancellationException) {
                fatalFailure = preferControlFatalFailure(fatalFailure, failure)
            } catch (failure: Error) {
                fatalFailure = preferControlFatalFailure(fatalFailure, failure)
            } catch (_: Exception) {
                // Ordinary reset I/O is secondary to control shutdown.
            }
        }
        fatalFailure?.let(::recordFailure)
    }

    private fun readWorkMessage(input: InputStream): FrpMessage? = when (config.wireProtocol) {
        FrpWireProtocol.V1 -> FrpWireV1.readMessage(input)
        FrpWireProtocol.V2 -> FrpWireV2.readMessage(input)
    }

    private fun writeWorkMessage(output: OutputStream, message: FrpMessage) {
        when (config.wireProtocol) {
            FrpWireProtocol.V1 -> FrpWireV1.writeMessage(output, message)
            FrpWireProtocol.V2 -> FrpWireV2.writeMessage(output, message)
        }
        output.flush()
    }

    private fun recordFailure(failure: Throwable): Boolean {
        val firstTermination = synchronized(lifecycleLock) {
            selectedFailure = preferControlFailure(selectedFailure, failure)
            if (lifecycle == SessionLifecycle.TERMINATED) {
                false
            } else {
                lifecycle = SessionLifecycle.TERMINATED
                openDecision.complete(false)
                terminationSignal.complete(Unit)
                true
            }
        }
        if (firstTermination) lifecycleHooks.afterTerminationRecorded(identity)
        return firstTermination
    }

    private suspend fun closePhysicalTransportAndAwait() {
        if (physicalCloseStarted.compareAndSet(false, true)) {
            var closeFailure: Throwable? = null
            try {
                closing.set(true)
                writerQueue.close()
                workQueue.close()
                closeActiveWorkConnections()
                closeActiveClientStreams()
                try {
                    mux.close()
                } catch (failure: CancellationException) {
                    closeFailure = preferControlFailure(closeFailure, failure)
                    updateSelectedFailure(failure)
                } catch (failure: Error) {
                    closeFailure = preferControlFailure(closeFailure, failure)
                    updateSelectedFailure(failure)
                } catch (_: Exception) {
                    // Expected close I/O is secondary to the selected control failure.
                }
            } finally {
                physicalCloseFailure.set(closeFailure)
                physicalCloseCompletion.complete(Unit)
            }
        }
        withContext(NonCancellable) { physicalCloseCompletion.await() }
        physicalCloseFailure.get()?.let { throw it }
    }

    private fun closeActiveWorkConnections() {
        val leases = synchronized(lifecycleLock) { activeWork.toList() }
        leases.forEach(ActiveWorkLease::beginCleanup)
    }

    private suspend fun awaitWorkCleanup(): Throwable? {
        var selectedFailure: Throwable? = null
        while (true) {
            val flights = synchronized(lifecycleLock) { workCleanupFlights.toList() }
            if (flights.isEmpty()) return selectedFailure
            flights.forEach { lease ->
                try {
                    withContext(NonCancellable) { lease.awaitCleanup() }
                } catch (failure: CancellationException) {
                    selectedFailure = preferControlFailure(selectedFailure, failure)
                } catch (failure: Error) {
                    selectedFailure = preferControlFailure(selectedFailure, failure)
                } catch (_: Exception) {
                    // Ordinary reset I/O is local to the released work stream.
                }
            }
        }
    }

    private fun updateSelectedFailure(failure: Throwable) {
        synchronized(lifecycleLock) {
            selectedFailure = preferControlFailure(selectedFailure, failure)
        }
    }

    private inner class ActiveWorkLease(
        val connection: FrpWorkConnection,
    ) {
        private val cleanupStarted = AtomicBoolean(false)
        private val cleanupCompletion = CompletableDeferred<Unit>()
        private val cleanupFailure = AtomicReference<Throwable?>()
        private var handlerJob: Job? = null

        val job: Job
            get() = checkNotNull(handlerJob)

        val isReleased: Boolean
            get() = cleanupStarted.get()

        fun attach(job: Job) {
            check(handlerJob == null)
            handlerJob = job
        }

        fun beginCleanup() {
            if (!cleanupStarted.compareAndSet(false, true)) return
            val job = handlerJob
            job?.cancel()
            synchronized(lifecycleLock) {
                workCleanupFlights += this
            }
            workCleanupScope.launch(start = CoroutineStart.UNDISPATCHED) {
                var selectedFailure: Throwable? = null
                try {
                    try {
                        connection.close()
                    } catch (failure: CancellationException) {
                        selectedFailure = preferControlFailure(selectedFailure, failure)
                    } catch (failure: Error) {
                        selectedFailure = preferControlFailure(selectedFailure, failure)
                    } catch (_: Exception) {
                        // Continue to the adapter's reset completion barrier.
                    }
                    try {
                        connection.awaitClosed()
                    } catch (failure: CancellationException) {
                        selectedFailure = preferControlFailure(selectedFailure, failure)
                    } catch (failure: Error) {
                        selectedFailure = preferControlFailure(selectedFailure, failure)
                    } catch (_: Exception) {
                        // Ordinary reset I/O is isolated to this work connection.
                    }
                    if (job != null) {
                        try {
                            withContext(NonCancellable) { job.join() }
                        } catch (failure: CancellationException) {
                            selectedFailure = preferControlFailure(selectedFailure, failure)
                        } catch (failure: Error) {
                            selectedFailure = preferControlFailure(selectedFailure, failure)
                        } catch (_: Exception) {
                            // Handler-local ordinary failures were already classified by runWorkHandler.
                        }
                    }
                    selectedFailure?.let { failure ->
                        try {
                            recordFailure(failure)
                        } catch (candidate: CancellationException) {
                            selectedFailure = preferControlFailure(selectedFailure, candidate)
                        } catch (candidate: Error) {
                            selectedFailure = preferControlFailure(selectedFailure, candidate)
                        }
                    }
                } finally {
                    synchronized(lifecycleLock) {
                        activeWork.remove(this@ActiveWorkLease)
                        if (job != null) handlerJobs.remove(job)
                    }
                    releaseDataPermit()
                    cleanupFailure.set(selectedFailure)
                    cleanupCompletion.complete(Unit)
                    synchronized(lifecycleLock) {
                        workCleanupFlights.remove(this@ActiveWorkLease)
                    }
                }
            }
        }

        suspend fun awaitCleanup() {
            cleanupCompletion.await()
            cleanupFailure.get()?.let { throw it }
        }

        override fun toString(): String =
            "ActiveWorkLease(cleanupStarted=${cleanupStarted.get()}, jobAttached=${handlerJob != null})"
    }

    private class OutgoingMessage(
        val message: FrpMessage,
        val result: CompletableDeferred<Unit> = CompletableDeferred(),
    ) {
        override fun toString(): String =
            "OutgoingMessage(type=${message::class.java.simpleName}, completed=${result.isCompleted})"
    }

    private enum class SessionLifecycle {
        STARTING,
        OPEN,
        TERMINATED,
    }
}

internal fun StartWorkConn.withFrpUserPrefixRemoved(user: String): StartWorkConn {
    if (user.isEmpty()) return this
    val prefix = "$user."
    return if (proxyName.startsWith(prefix)) copy(proxyName = proxyName.removePrefix(prefix)) else this
}

private suspend fun <T> withControlBudgetTimeout(
    scheduler: ScheduledExecutorService,
    timeoutMillis: Long,
    timeoutFailure: FrpControlFailure,
    onTimerScheduled: (() -> Unit)? = null,
    onTimerFired: (() -> Unit)? = null,
    onTimeout: (() -> Unit)? = null,
    block: suspend () -> T,
): T {
    val callerContext = currentCoroutineContext()
    val budgetCancellation = AtomicReference<ControlBudgetCancellation?>()
    val timeoutCleanupFailure = AtomicReference<Throwable?>()
    return try {
        coroutineScope {
            val budgetOwner = checkNotNull(currentCoroutineContext()[Job])
            val timer = scheduler.schedule({
                val cancellation = ControlBudgetCancellation()
                budgetCancellation.compareAndSet(null, cancellation)
                try {
                    onTimerFired?.invoke()
                    onTimeout?.invoke()
                } catch (failure: CancellationException) {
                    timeoutCleanupFailure.compareAndSet(null, failure)
                } catch (failure: Error) {
                    timeoutCleanupFailure.compareAndSet(null, failure)
                } catch (_: Exception) {
                    // The timeout remains authoritative when best-effort stream invalidation fails.
                }
                budgetOwner.cancel(cancellation)
            }, timeoutMillis, TimeUnit.MILLISECONDS)
            try {
                onTimerScheduled?.invoke()
                block()
            } finally {
                timer.cancel(false)
            }
        }
    } catch (failure: CancellationException) {
        if (!callerContext.isActive) callerContext.ensureActive()
        if (budgetCancellation.get() != null) {
            timeoutCleanupFailure.get()?.let { throw it }
            throw FrpControlException(timeoutFailure)
        }
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (failure: Exception) {
        if (!callerContext.isActive) callerContext.ensureActive()
        if (budgetCancellation.get() != null) {
            timeoutCleanupFailure.get()?.let { throw it }
            throw FrpControlException(timeoutFailure)
        }
        throw failure
    }
}

private fun incrementSaturating(counter: AtomicLong): Long {
    while (true) {
        val current = counter.get()
        if (current == Long.MAX_VALUE) return current
        val next = current + 1L
        if (counter.compareAndSet(current, next)) return next
    }
}

private fun mapControlFailure(
    failure: Throwable,
    fallback: FrpControlFailure,
    login: Boolean = false,
): Throwable = when (failure) {
    is Error -> failure
    is CancellationException -> failure
    is FrpControlException -> failure
    is FrpTransportException -> FrpControlException(
        when (failure.failure) {
            FrpTransportFailure.CONNECT_FAILED -> FrpControlFailure.TRANSPORT_CONNECT_FAILED
            FrpTransportFailure.CONNECT_TIMEOUT -> FrpControlFailure.TRANSPORT_CONNECT_TIMEOUT
            FrpTransportFailure.TLS_SETUP_FAILED -> FrpControlFailure.TRANSPORT_TLS_SETUP_FAILED
            FrpTransportFailure.TLS_HANDSHAKE_FAILED -> FrpControlFailure.TRANSPORT_TLS_HANDSHAKE_FAILED
            FrpTransportFailure.TLS_HANDSHAKE_TIMEOUT -> FrpControlFailure.TRANSPORT_TLS_HANDSHAKE_TIMEOUT
            FrpTransportFailure.READ_FAILED -> FrpControlFailure.TRANSPORT_READ_FAILED
            FrpTransportFailure.WRITE_FAILED -> FrpControlFailure.TRANSPORT_WRITE_FAILED
            FrpTransportFailure.CLOSED -> FrpControlFailure.TRANSPORT_CLOSED
            FrpTransportFailure.INVALID_CONFIGURATION -> FrpControlFailure.TRANSPORT_INVALID_CONFIGURATION
        },
    )
    is YamuxProtocolFailure, is YamuxTruncatedFrameFailure ->
        FrpControlException(FrpControlFailure.YAMUX_PROTOCOL_FAILED)
    is YamuxTransportFailure -> FrpControlException(FrpControlFailure.YAMUX_TRANSPORT_FAILED)
    is YamuxTimeoutFailure -> FrpControlException(
        if (failure.kind == YamuxTimeoutKind.STREAM_OPEN) {
            FrpControlFailure.YAMUX_STREAM_OPEN_TIMEOUT
        } else {
            FrpControlFailure.YAMUX_TIMEOUT
        },
    )
    is YamuxFailure -> FrpControlException(FrpControlFailure.YAMUX_FAILED)
    is FrpProtocolException -> FrpControlException(
        if (login) FrpControlFailure.LOGIN_PROTOCOL_FAILED else FrpControlFailure.PROTOCOL_FAILED,
    )
    is FrpCryptoException -> FrpControlException(
        when (failure.failure) {
            FrpCryptoFailure.INVALID_KEY -> FrpControlFailure.CRYPTO_INVALID_KEY
            FrpCryptoFailure.INVALID_NONCE -> FrpControlFailure.CRYPTO_INVALID_NONCE
            FrpCryptoFailure.TRUNCATED_NONCE -> FrpControlFailure.CRYPTO_TRUNCATED_NONCE
            FrpCryptoFailure.TRUNCATED_HEADER -> FrpControlFailure.CRYPTO_TRUNCATED_HEADER
            FrpCryptoFailure.TRUNCATED_RECORD -> FrpControlFailure.CRYPTO_TRUNCATED_RECORD
            FrpCryptoFailure.INVALID_LENGTH -> FrpControlFailure.CRYPTO_INVALID_LENGTH
            FrpCryptoFailure.INTEGRITY_FAILURE -> FrpControlFailure.CRYPTO_INTEGRITY_FAILED
            FrpCryptoFailure.NONCE_EXHAUSTED -> FrpControlFailure.CRYPTO_NONCE_EXHAUSTED
            FrpCryptoFailure.RECORD_LIMIT -> FrpControlFailure.CRYPTO_RECORD_LIMIT
            FrpCryptoFailure.UNSUPPORTED_ALGORITHM -> FrpControlFailure.CRYPTO_UNSUPPORTED_ALGORITHM
            FrpCryptoFailure.IO_FAILURE -> FrpControlFailure.CRYPTO_IO_FAILED
            FrpCryptoFailure.WRITE_FAILED -> FrpControlFailure.CRYPTO_WRITE_FAILED
        },
    )
    else -> FrpControlException(fallback)
}

private fun preferControlFailure(current: Throwable?, candidate: Throwable): Throwable {
    if (current == null || current === candidate) return candidate
    val selected = when {
        candidate is Error && current !is Error -> candidate
        candidate is CancellationException && current !is Error && current !is CancellationException -> candidate
        else -> current
    }
    val secondary = if (selected === current) candidate else current
    if (secondary is Error || secondary is CancellationException) {
        if (selected !== secondary) selected.addSuppressed(secondary)
    } else {
        selected.addSuppressed(FrpControlException(FrpControlFailure.CLOSED))
    }
    return selected
}

private fun preferControlFatalFailure(current: Throwable?, candidate: Throwable): Throwable {
    if (current == null || current === candidate) return candidate
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

private class ControlClientClosedCancellation : CancellationException("frp control client closed")

private class ControlSessionClosedCancellation : CancellationException("frp control session closed")

private class ControlBudgetCancellation : CancellationException("frp control budget expired")
