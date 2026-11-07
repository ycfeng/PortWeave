package io.github.ycfeng.ocdeck.frpcstcpvisitor

import android.content.Context
import android.net.ConnectivityManager
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlFailure
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlIdentity
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlReconnectDiagnostics
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlState
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpUnixTimeSource
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.SystemFrpUnixTimeSource
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalConnection
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalListener
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalListenerFactory
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpRelayVisitorConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpSessionControl
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpSessionControlFactory
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpVisitorRelay
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpVisitorRelayCleanupPhase
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.ProductionFrpSessionControlFactory
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.SocketFrpLocalListenerFactory
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport.AndroidDefaultNetworkTcpDialRouteProvider
import java.net.BindException
import java.net.InetAddress
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.UUID
import java.util.concurrent.CancellationException
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
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock

class KotlinFrpcStcpVisitorClient private constructor(
    parentContext: CoroutineContext,
    private val controlFactory: FrpSessionControlFactory,
    private val listenerFactory: FrpLocalListenerFactory,
    private val unixTimeSource: FrpUnixTimeSource,
    private val sessionIdSource: FrpSessionIdSource,
    private val ioDispatcher: CoroutineDispatcher,
    private val lifecycleHooks: KotlinFrpcRuntimeLifecycleHooks,
    private val runtimeDelayer: FrpRuntimeDelayer,
) : FrpcStcpVisitorClient, FrpcStcpVisitorNetworkChangeAware, FrpcStcpVisitorStateObservable, AutoCloseable {
    constructor() : this(
        parentContext = Dispatchers.IO + CoroutineName("KotlinFrpcClientRoot"),
        controlFactory = ProductionFrpSessionControlFactory(),
        listenerFactory = SocketFrpLocalListenerFactory(),
        unixTimeSource = SystemFrpUnixTimeSource,
        sessionIdSource = FrpSessionIdSource { UUID.randomUUID().toString() },
        ioDispatcher = Dispatchers.IO.limitedParallelism(MAXIMUM_RELAY_IO_PARALLELISM, "FrpVisitorRelayIo"),
        lifecycleHooks = KotlinFrpcRuntimeLifecycleHooks(),
        runtimeDelayer = CoroutineFrpRuntimeDelayer,
    )

    /**
     * [reconnectGate] lets the host app stretch or pause control reconnects and [keepAliveScheduler] schedules
     * transport keepalives; null keeps frpc timing for either.
     */
    constructor(
        context: Context,
        reconnectGate: FrpcReconnectGate? = null,
        keepAliveScheduler: FrpcKeepAliveScheduler? = null,
    ) : this(
        parentContext = Dispatchers.IO + CoroutineName("KotlinFrpcClientRoot"),
        controlFactory = ProductionFrpSessionControlFactory(
            AndroidDefaultNetworkTcpDialRouteProvider(
                checkNotNull(context.applicationContext.getSystemService(ConnectivityManager::class.java)),
            ),
            reconnectGate,
            keepAliveScheduler,
        ),
        listenerFactory = SocketFrpLocalListenerFactory(),
        unixTimeSource = SystemFrpUnixTimeSource,
        sessionIdSource = FrpSessionIdSource { UUID.randomUUID().toString() },
        ioDispatcher = Dispatchers.IO.limitedParallelism(MAXIMUM_RELAY_IO_PARALLELISM, "FrpVisitorRelayIo"),
        lifecycleHooks = KotlinFrpcRuntimeLifecycleHooks(),
        runtimeDelayer = CoroutineFrpRuntimeDelayer,
    )

    internal constructor(
        parentScope: CoroutineScope,
        controlFactory: FrpSessionControlFactory,
        listenerFactory: FrpLocalListenerFactory,
        unixTimeSource: FrpUnixTimeSource = SystemFrpUnixTimeSource,
        sessionIdSource: FrpSessionIdSource = FrpSessionIdSource { UUID.randomUUID().toString() },
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
        lifecycleHooks: KotlinFrpcRuntimeLifecycleHooks = KotlinFrpcRuntimeLifecycleHooks(),
        runtimeDelayer: FrpRuntimeDelayer = CoroutineFrpRuntimeDelayer,
        @Suppress("UNUSED_PARAMETER") testOnly: Unit,
    ) : this(
        parentScope.coroutineContext,
        controlFactory,
        listenerFactory,
        unixTimeSource,
        sessionIdSource,
        ioDispatcher,
        lifecycleHooks,
        runtimeDelayer,
    )

    private val closed = AtomicBoolean(false)
    private val generationSequence = AtomicLong(0L)
    private val parentJob = parentContext[Job]
    private val ownerJob = SupervisorJob(parentJob)
    private val scope = CoroutineScope(
        parentContext.minusKey(Job) + ownerJob + CoroutineName("KotlinFrpcClient"),
    )
    private val clientCleanupJob = SupervisorJob()
    private val clientCleanupScope = CoroutineScope(
        parentContext.minusKey(Job) + clientCleanupJob + CoroutineName("KotlinFrpcClientCleanup"),
    )
    private val sessionsMutex = Mutex()
    private val sessions = LinkedHashMap<String, KotlinFrpcRuntimeSession>()
    private val relayBudget = RuntimeRelayBudget(MAXIMUM_GLOBAL_RELAYS)

    override suspend fun startSession(config: FrpcSessionConfig): String {
        currentCoroutineContext().ensureActive()
        val normalized = normalizeSessionConfig(config)
        return sessionsMutex.withLock {
            ensureClientOpen()
            if (sessions.size >= MAXIMUM_SESSIONS) {
                throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_LIMIT)
            }
            val sessionId = nextUniqueSessionIdLocked()
            val session = KotlinFrpcRuntimeSession(
                sessionId = sessionId,
                sessionConfig = normalized,
                generation = generationSequence.incrementAndGet(),
                parentScope = scope,
                controlFactory = controlFactory,
                listenerFactory = listenerFactory,
                unixTimeSource = unixTimeSource,
                ioDispatcher = ioDispatcher,
                lifecycleHooks = lifecycleHooks,
                runtimeDelayer = runtimeDelayer,
                relayBudget = relayBudget,
                onCleanupComplete = { completed ->
                    sessionsMutex.withLock {
                        if (sessions[sessionId] === completed) sessions.remove(sessionId)
                    }
                },
            )
            sessions[sessionId] = session
            try {
                session.start()
            } catch (failure: CancellationException) {
                sessions.remove(sessionId)
                session.close()
                throw failure
            } catch (failure: Error) {
                sessions.remove(sessionId)
                session.close()
                throw failure
            } catch (failure: Exception) {
                sessions.remove(sessionId)
                session.close()
                throw mapRuntimeFailure(failure, KotlinFrpcStcpVisitorFailure.CONTROL_FAILED)
            }
            sessionId
        }
    }

    init {
        ownerJob.invokeOnCompletion {
            if (closed.compareAndSet(false, true)) launchClientCleanup()
        }
    }

    override suspend fun ensureVisitor(
        sessionId: String,
        visitor: FrpcStcpVisitorConfig,
    ): FrpcEnsureVisitorResult {
        currentCoroutineContext().ensureActive()
        val normalized = normalizeVisitorConfig(visitor)
        return requireSession(sessionId).ensureVisitor(normalized)
    }

    override suspend fun waitVisitorReady(
        sessionId: String,
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ): FrpcVisitorReadyResult {
        currentCoroutineContext().ensureActive()
        val normalizedName = normalizeVisitorName(visitorName)
        if (desiredRevision <= 0L) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.INVALID_CONFIGURATION)
        validateTimeout(timeoutMillis)
        return requireSession(sessionId).waitVisitorReady(
            visitorName = normalizedName,
            desiredRevision = desiredRevision,
            timeoutMillis = timeoutMillis,
        )
    }

    override suspend fun stopVisitor(sessionId: String, visitorName: String) {
        currentCoroutineContext().ensureActive()
        val normalizedName = normalizeVisitorName(visitorName)
        findSession(sessionId)?.stopVisitor(normalizedName)
    }

    override suspend fun stopSession(
        sessionId: String,
        timeoutMillis: Long,
    ): FrpcStopSessionResult {
        currentCoroutineContext().ensureActive()
        val effectiveTimeoutMillis = normalizeStopTimeout(timeoutMillis)
        val session = findSession(sessionId)
            ?: return FrpcStopSessionResult(sessionId = sessionId, phase = PHASE_CLOSED)
        val cleanup = session.requestStop()
        val completed = withTimeoutOrNull(effectiveTimeoutMillis) { cleanup.await() }
        if (completed == null) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.STOP_TIMEOUT)
        return FrpcStopSessionResult(sessionId = sessionId, phase = PHASE_CLOSED)
    }

    override suspend fun getState(sessionId: String): FrpcStcpVisitorState =
        findSession(sessionId)?.state() ?: FrpcStcpVisitorState(
            sessionId = sessionId,
            phase = PHASE_CLOSED,
        )

    override suspend fun observeState(sessionId: String): Flow<FrpcStcpVisitorState> =
        findSession(sessionId)?.observeState()
            ?: flowOf(FrpcStcpVisitorState(sessionId = sessionId, phase = PHASE_CLOSED))

    override suspend fun reconnectAfterDefaultNetworkChange() {
        currentCoroutineContext().ensureActive()
        val snapshot = sessionsMutex.withLock {
            ensureClientOpen()
            sessions.values.toList()
        }
        val invalidated = snapshot.mapNotNull { session ->
            session.invalidateTransport()?.let { identity -> session to identity }
        }
        invalidated.forEach { (session, identity) ->
            session.awaitTransportInvalidated(identity, NETWORK_CHANGE_INVALIDATION_TIMEOUT_MILLIS)
        }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        launchClientCleanup()
    }

    private fun launchClientCleanup() {
        clientCleanupScope.launch {
            var selectedFatal: Throwable? = null
            try {
                val snapshot = sessionsMutex.withLock { sessions.values.toList() }
                snapshot.forEach(KotlinFrpcRuntimeSession::requestStop)
                snapshot.forEach { session ->
                    try {
                        session.requestStop().await()
                    } catch (failure: CancellationException) {
                        selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                    } catch (failure: Error) {
                        selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                    } catch (_: Exception) {
                        // Each session has already invalidated its listeners and control owner.
                    }
                }
            } finally {
                ownerJob.cancel()
                clientCleanupJob.cancel()
            }
            selectedFatal?.let { throw it }
        }
    }

    override fun toString(): String = "KotlinFrpcStcpVisitorClient(closed=${closed.get()})"

    internal suspend fun diagnostics(): KotlinFrpcRuntimeDiagnostics = KotlinFrpcRuntimeDiagnostics(
        activeSessions = sessionsMutex.withLock { sessions.size },
        activeRelays = relayBudget.activeCount,
        rejectedRelays = relayBudget.rejectedCount,
    )

    internal suspend fun sessionCleanupDiagnostics(
        sessionId: String,
    ): KotlinFrpcSessionCleanupDiagnostics? = sessionsMutex.withLock {
        sessions[sessionId]?.cleanupDiagnostics()
    }

    private suspend fun requireSession(sessionId: String): KotlinFrpcRuntimeSession =
        findSession(sessionId) ?: throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_NOT_FOUND)

    private suspend fun findSession(sessionId: String): KotlinFrpcRuntimeSession? =
        sessionsMutex.withLock { sessions[sessionId] }

    private fun ensureClientOpen() {
        if (closed.get() || !ownerJob.isActive) {
            throw runtimeFailure(KotlinFrpcStcpVisitorFailure.CLIENT_CLOSED)
        }
    }

    private fun nextUniqueSessionIdLocked(): String {
        repeat(MAXIMUM_SESSION_ID_ATTEMPTS) {
            val candidate = sessionIdSource.nextId()
            if (candidate.isNotBlank() && candidate.length <= MAXIMUM_SESSION_ID_LENGTH && candidate !in sessions) {
                return candidate
            }
        }
        throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_LIMIT)
    }

    private companion object {
        const val MAXIMUM_SESSIONS = 4
        const val MAXIMUM_SESSION_ID_ATTEMPTS = 8
        const val MAXIMUM_SESSION_ID_LENGTH = 128
        const val MAXIMUM_RELAY_IO_PARALLELISM = 256
        const val MAXIMUM_GLOBAL_RELAYS = 100
        const val NETWORK_CHANGE_INVALIDATION_TIMEOUT_MILLIS = 5_000L
    }
}

internal fun interface FrpSessionIdSource {
    fun nextId(): String
}

internal fun interface FrpRuntimeDelayer {
    suspend fun delayMillis(milliseconds: Long)
}

internal object CoroutineFrpRuntimeDelayer : FrpRuntimeDelayer {
    override suspend fun delayMillis(milliseconds: Long) {
        delay(milliseconds)
    }
}

internal data class KotlinFrpcRuntimeDiagnostics(
    val activeSessions: Int,
    val activeRelays: Int,
    val rejectedRelays: Int,
)

internal data class KotlinFrpcSessionCleanupDiagnostics(
    val phase: KotlinFrpcSessionCleanupPhase,
    val activeOwnedJobs: Int,
    val activeListeners: Int,
    val activeRelays: Int,
    val openingRelays: Int,
    val relayingRelays: Int,
    val resettingLeaseRelays: Int,
    val resettingAdapterRelays: Int,
    val actorCompleted: Boolean,
    val controlCollectorCompleted: Boolean,
)

internal enum class KotlinFrpcSessionCleanupPhase {
    ACTIVE,
    STOP_REQUESTED,
    WAITING_FOR_ACTOR,
    ACTOR_INVALIDATING,
    ACTOR_WAITING_FOR_CONTROL,
    ACTOR_COMPLETE,
    FINALIZING_JOBS,
    FINALIZING_CONTROL,
    FINALIZING_MAILBOX,
    EMERGENCY,
    REGISTRY_REMOVAL,
    COMPLETE,
}

internal data class KotlinFrpcRuntimeLifecycleHooks(
    val beforeVisitorBind: suspend (Long, FrpControlIdentity) -> Unit = { _, _ -> },
    val afterVisitorBind: suspend (Long, FrpControlIdentity) -> Unit = { _, _ -> },
    val afterControlState: suspend (FrpControlState) -> Unit = {},
    val beforeStopCleanup: suspend () -> Unit = {},
    val beforeSessionRegistryRemoval: suspend (KotlinFrpcSessionCleanupDiagnostics) -> Unit = {},
    val beforeRuntimeCommand: suspend (String) -> Unit = {},
    val observeRuntimeCommandSummary: (String) -> Unit = {},
    val afterRelayCompletion: suspend () -> Unit = {},
    val afterRelayWatcherScheduled: suspend () -> Unit = {},
    val beforeVisitorHandshakeCommit: suspend () -> Unit = {},
    val visitorHandshakeTimeoutMillis: Long = 10_000L,
)

private class KotlinFrpcRuntimeSession(
    private val sessionId: String,
    private val sessionConfig: FrpcSessionConfig,
    private val generation: Long,
    parentScope: CoroutineScope,
    controlFactory: FrpSessionControlFactory,
    private val listenerFactory: FrpLocalListenerFactory,
    private val unixTimeSource: FrpUnixTimeSource,
    private val ioDispatcher: CoroutineDispatcher,
    private val lifecycleHooks: KotlinFrpcRuntimeLifecycleHooks,
    private val runtimeDelayer: FrpRuntimeDelayer,
    private val relayBudget: RuntimeRelayBudget,
    private val onCleanupComplete: suspend (KotlinFrpcRuntimeSession) -> Unit,
) : AutoCloseable {
    private val started = AtomicBoolean(false)
    private val stopRequested = AtomicBoolean(false)
    private val stopCleanupCompleted = AtomicBoolean(false)
    private val cleanupStarted = AtomicBoolean(false)
    private val parentJob = parentScope.coroutineContext[Job]
    private val ownerJob = SupervisorJob(parentJob)
    private val scope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) + ownerJob + CoroutineName("KotlinFrpcSession"),
    )
    private val cleanupJob = SupervisorJob()
    private val cleanupScope = CoroutineScope(
        parentScope.coroutineContext.minusKey(Job) + cleanupJob + CoroutineName("KotlinFrpcSessionCleanup"),
    )
    private val mailbox = Channel<RuntimeCommand>(SESSION_MAILBOX_CAPACITY)
    private val cleanupCompletion = CompletableDeferred<Unit>()
    private val actorCompletion = CompletableDeferred<Throwable?>()
    private val actorBodyEntered = AtomicBoolean(false)
    private val cleanupPhase = AtomicReference(KotlinFrpcSessionCleanupPhase.ACTIVE)
    private val actorCompletionFinalized = AtomicBoolean(false)
    private val control: FrpSessionControl = controlFactory.create(sessionConfig, scope, generation)
    private val mutableSnapshot = MutableStateFlow(RuntimeSnapshot.initial(sessionId))
    private val mutableStopping = MutableStateFlow(false)
    private val relayVersion = MutableStateFlow(0L)
    private val actorJob = AtomicReference<Job?>()
    private val controlCollector = AtomicReference<Job?>()
    private val resourceLock = Any()
    private val ownedJobs = LinkedHashSet<Job>()
    private val ownedListeners = LinkedHashSet<RuntimeListenerOwner>()
    private val ownedRelays = LinkedHashSet<RuntimeRelayRecord>()
    private var observedRelayFatal: Throwable? = null

    private var phase = PHASE_CONNECTING
    private var configRevision = 0L
    private var controlEpoch = 0L
    private var currentIdentity: FrpControlIdentity? = null
    private var highestTransportIdentity = 0L
    private var lastFailure: KotlinFrpcStcpVisitorFailure? = null
    private var lastControlFailure: FrpControlFailure? = null
    private var listenerAttemptSequence = 0L
    private val visitors = LinkedHashMap<String, RuntimeVisitor>()
    private val relays = LinkedHashSet<RuntimeRelayRecord>()

    init {
        ownerJob.invokeOnCompletion { requestStop() }
    }

    fun start() {
        if (!started.compareAndSet(false, true) || stopRequested.get()) {
            throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
        }
        val actor = scope.launch { runActor() }
        check(actorJob.compareAndSet(null, actor))
        actor.invokeOnCompletion { failure ->
            if (!actorBodyEntered.get()) {
                completeActorOnce(failure)
                requestStop()
            }
        }
        val collector = scope.launch(CoroutineName("KotlinFrpcControlCollector")) {
            control.state.collect { state -> mailbox.send(ControlChangedCommand(state)) }
        }
        check(controlCollector.compareAndSet(null, collector))
        control.start()
    }

    suspend fun ensureVisitor(config: NormalizedVisitorConfig): FrpcEnsureVisitorResult {
        val reply = CompletableDeferred<FrpcEnsureVisitorResult>()
        enqueue(EnsureVisitorCommand(config, reply))
        return reply.await()
    }

    suspend fun stopVisitor(visitorName: String) {
        val reply = CompletableDeferred<Unit>()
        enqueue(StopVisitorCommand(visitorName, reply))
        reply.await()
    }

    suspend fun waitVisitorReady(
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ): FrpcVisitorReadyResult {
        val result = withTimeoutOrNull(timeoutMillis) {
            combine(mutableSnapshot, control.state, mutableStopping) { snapshot, controlState, isStopping ->
                snapshot.readyResult(visitorName, desiredRevision, controlState, isStopping)
            }.first { it != null }
        }
        return result ?: throw runtimeFailure(KotlinFrpcStcpVisitorFailure.WAIT_TIMEOUT)
    }

    fun observeState(): Flow<FrpcStcpVisitorState> =
        combine(mutableSnapshot, control.state, mutableStopping, relayVersion) { _, _, _, _ -> state() }
            .distinctUntilChanged()

    fun state(): FrpcStcpVisitorState {
        val base = mutableSnapshot.value.publicStateFor(
            controlState = control.state.value,
            stopping = stopRequested.get(),
            reconnectEstablishmentFailure = control.reconnectEstablishmentFailure(),
            reconnectCleanupFailure = control.reconnectCleanupFailure(),
            reconnectDiagnostics = control.reconnectDiagnostics(),
        )
        val counts = synchronized(resourceLock) { ownedRelays.groupingBy(RuntimeRelayRecord::visitorName).eachCount() }
        return base.copy(
            visitors = base.visitors.mapValues { (name, visitor) ->
                visitor.copy(activeConnections = counts[name] ?: 0)
            },
        )
    }

    fun invalidateTransport(): FrpControlIdentity? = control.invalidateTransport()

    suspend fun awaitTransportInvalidated(identity: FrpControlIdentity, timeoutMillis: Long) {
        withTimeoutOrNull(timeoutMillis) {
            control.state.first { state -> state !is FrpControlState.Open || state.identity != identity }
        }
    }

    fun requestStop(): CompletableDeferred<Unit> {
        stopRequested.set(true)
        mutableStopping.value = true
        if (cleanupStarted.compareAndSet(false, true)) {
            cleanupPhase.set(KotlinFrpcSessionCleanupPhase.STOP_REQUESTED)
            cleanupScope.launch {
                var selectedFatal: Throwable? = null
                var emergencyCleanupRequired = actorJob.get() == null || !actorBodyEntered.get()
                try {
                    lifecycleHooks.beforeStopCleanup()
                    if (actorJob.get()?.isActive != true || !actorBodyEntered.get()) {
                        emergencyCleanupRequired = true
                    } else {
                        cleanupPhase.set(KotlinFrpcSessionCleanupPhase.WAITING_FOR_ACTOR)
                        mailbox.send(StopSessionCommand)
                        selectedFatal = actorCompletion.await()
                        emergencyCleanupRequired = selectedFatal != null || !stopCleanupCompleted.get()
                    }
                } catch (failure: CancellationException) {
                    selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                    emergencyCleanupRequired = true
                } catch (failure: Error) {
                    selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                    emergencyCleanupRequired = true
                } catch (_: Exception) {
                    emergencyCleanupRequired = true
                } finally {
                    if (emergencyCleanupRequired || !actorCompletion.isCompleted || !stopCleanupCompleted.get()) {
                        try {
                            emergencyCleanup()
                        } catch (failure: CancellationException) {
                            selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                        } catch (failure: Error) {
                            selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                        } catch (_: Exception) {
                            // Logical ownership was already invalidated before best-effort I/O cleanup.
                        }
                    } else {
                        try {
                            finalizeNormalCleanup()
                        } catch (failure: CancellationException) {
                            selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                        } catch (failure: Error) {
                            selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                        } catch (_: Exception) {
                            // All logical owners were already invalidated by the actor cleanup.
                        }
                    }
                    try {
                        cleanupPhase.set(KotlinFrpcSessionCleanupPhase.REGISTRY_REMOVAL)
                        withContext(NonCancellable) {
                            lifecycleHooks.beforeSessionRegistryRemoval(cleanupDiagnostics())
                        }
                    } catch (failure: CancellationException) {
                        selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                    } catch (failure: Error) {
                        selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                    } catch (_: Exception) {
                        // Test-only observation cannot skip registry removal.
                    }
                    try {
                        withContext(NonCancellable) {
                            onCleanupComplete(this@KotlinFrpcRuntimeSession)
                        }
                    } catch (failure: CancellationException) {
                        selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                    } catch (failure: Error) {
                        selectedFatal = preferRuntimeFatal(selectedFatal, failure)
                    } catch (_: Exception) {
                        // Registry cleanup is best effort after all runtime ownership is invalidated.
                    }
                    cleanupPhase.set(KotlinFrpcSessionCleanupPhase.COMPLETE)
                    if (selectedFatal == null) {
                        cleanupCompletion.complete(Unit)
                    } else {
                        cleanupCompletion.completeExceptionally(selectedFatal)
                    }
                    cleanupJob.cancel()
                }
            }
        }
        return cleanupCompletion
    }

    override fun close() {
        requestStop()
    }

    override fun toString(): String {
        val snapshot = mutableSnapshot.value
        return "KotlinFrpcRuntimeSession(generation=$generation, phase=${snapshot.publicState.phase}, " +
            "configRevision=${snapshot.publicState.configRevision}, " +
            "controlEpoch=${snapshot.publicState.controlEpoch}, " +
            "visitorCount=${snapshot.publicState.visitors.size}, stopping=${stopRequested.get()})"
    }

    private suspend fun enqueue(command: RuntimeCommand) {
        currentCoroutineContext().ensureActive()
        if (stopRequested.get()) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
        val result = mailbox.trySend(command)
        if (result.isSuccess) return
        if (result.isClosed) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
        throw runtimeFailure(KotlinFrpcStcpVisitorFailure.MAILBOX_FULL)
    }

    private suspend fun runActor() {
        actorBodyEntered.set(true)
        var terminalFailure: Throwable? = null
        try {
            for (command in mailbox) {
                val keepRunning = try {
                    lifecycleHooks.observeRuntimeCommandSummary(command.toString())
                    lifecycleHooks.beforeRuntimeCommand(command.kind)
                    processCommand(command)
                } catch (failure: CancellationException) {
                    command.fail(failure)
                    throw failure
                } catch (failure: Error) {
                    command.fail(failure)
                    phase = PHASE_FAILED
                    currentIdentity = null
                    lastFailure = KotlinFrpcStcpVisitorFailure.CONTROL_FAILED
                    publishSnapshot()
                    terminalFailure = failure
                    throw failure
                } catch (failure: Exception) {
                    command.fail(mapRuntimeFailure(failure, KotlinFrpcStcpVisitorFailure.VISITOR_FAILED))
                    true
                }
                if (!keepRunning) break
            }
        } catch (failure: CancellationException) {
            if (!stopCleanupCompleted.get()) terminalFailure = failure
        } catch (failure: Error) {
            terminalFailure = preferRuntimeFatal(terminalFailure, failure)
            throw failure
        } finally {
            val pendingFailure = terminalFailure
                ?: runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
            terminalFailure = preferRuntimeFatalOrNull(
                terminalFailure,
                closeMailboxAndDrain(pendingFailure),
            )
            completeActorOnce(terminalFailure)
            if (terminalFailure != null) requestStop()
        }
    }

    private fun completeActorOnce(failure: Throwable?) {
        if (actorCompletionFinalized.compareAndSet(false, true)) actorCompletion.complete(failure)
    }

    private suspend fun processCommand(command: RuntimeCommand): Boolean = when (command) {
        is EnsureVisitorCommand -> {
            if (stopRequested.get()) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
            command.reply.complete(handleEnsureVisitor(command.config))
            true
        }
        is StopVisitorCommand -> {
            if (stopRequested.get()) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
            handleStopVisitor(command.visitorName)
            command.reply.complete(Unit)
            true
        }
        is ControlChangedCommand -> {
            handleControlState(command.state)
            true
        }
        is ListenerAcceptedCommand -> {
            handleListenerAccepted(command)
            true
        }
        is BindCompletedCommand -> {
            handleBindCompleted(command)
            true
        }
        is BindRetryDueCommand -> {
            handleBindRetryDue(command)
            true
        }
        is ListenerFailedCommand -> {
            handleListenerFailed(command)
            true
        }
        is RelayFinishedCommand -> {
            handleRelayFinished(command)
            true
        }
        StopSessionCommand -> {
            handleStopSession()
            false
        }
    }

    private suspend fun handleEnsureVisitor(config: NormalizedVisitorConfig): FrpcEnsureVisitorResult {
        if (phase == PHASE_FAILED || phase == PHASE_CLOSED || phase == PHASE_STOPPING) {
            throw runtimeFailure(
                if (phase == PHASE_FAILED) KotlinFrpcStcpVisitorFailure.CONTROL_FAILED
                else KotlinFrpcStcpVisitorFailure.SESSION_CLOSED,
            )
        }
        val existing = visitors[config.name]
        if (existing == null && visitors.size >= MAXIMUM_VISITORS_PER_SESSION) {
            throw runtimeFailure(KotlinFrpcStcpVisitorFailure.VISITOR_LIMIT)
        }
        if (visitors.values.any { visitor ->
                visitor.config.name != config.name && visitor.config.bindPort == config.bindPort
            }) {
            throw runtimeFailure(KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT)
        }

        val nextRevision = nextConfigRevision()
        configRevision = nextRevision
        if (existing != null && existing.config == config) {
            publishSnapshot()
            val identity = currentIdentity
            if (phase == PHASE_OPEN && identity != null && existing.bindTask == null &&
                existing.phase != VISITOR_PHASE_READY
            ) {
                if (existing.phase == VISITOR_PHASE_FAILED) scheduleBindRetry(existing, identity)
                else scheduleBindAttempt(existing, identity)
            }
            if (stopRequested.get()) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
            return FrpcEnsureVisitorResult(
                bindPort = existing.bindPort,
                desiredRevision = existing.desiredRevision,
            )
        }

        if (existing != null) closeVisitorRuntime(existing)
        val desired = RuntimeVisitor(
            config = config,
            desiredRevision = nextRevision,
            bindPort = config.bindPort,
            phase = VISITOR_PHASE_PENDING,
        )
        visitors[config.name] = desired
        publishSnapshot()
        val identity = currentIdentity
        if (phase == PHASE_OPEN && identity != null) scheduleBindAttempt(desired, identity)
        if (stopRequested.get()) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
        return FrpcEnsureVisitorResult(
            bindPort = desired.bindPort,
            desiredRevision = desired.desiredRevision,
        )
    }

    private suspend fun handleStopVisitor(visitorName: String) {
        val visitor = visitors[visitorName] ?: return
        configRevision = nextConfigRevision()
        visitors.remove(visitorName)
        visitor.phase = VISITOR_PHASE_STOPPED
        closeVisitorRuntime(visitor)
        publishSnapshot()
    }

    private suspend fun handleControlState(state: FrpControlState) {
        if (state.generation != generation || stopRequested.get()) return
        when (state) {
            is FrpControlState.Open -> handleControlOpen(state.identity)
            is FrpControlState.Connecting -> transitionControlUnavailable(PHASE_CONNECTING, null, null)
            is FrpControlState.Retrying -> transitionControlUnavailable(
                PHASE_RECONNECTING,
                null,
                state.failure,
            )
            is FrpControlState.Failed -> transitionControlUnavailable(
                PHASE_FAILED,
                KotlinFrpcStcpVisitorFailure.CONTROL_FAILED,
                state.failure,
            )
            is FrpControlState.Closed -> transitionControlUnavailable(PHASE_CLOSED, null, null)
        }
        lifecycleHooks.afterControlState(state)
    }

    private suspend fun handleControlOpen(identity: FrpControlIdentity) {
        if (identity.generation != generation || identity.transportIdentity <= highestTransportIdentity) return
        currentIdentity = null
        invalidateAllRuntimeResources(
            visitorPhase = VISITOR_PHASE_PENDING,
            visitorFailure = null,
        )
        highestTransportIdentity = identity.transportIdentity
        currentIdentity = identity
        controlEpoch = identity.controlEpoch
        phase = PHASE_OPEN
        lastFailure = null
        lastControlFailure = null
        publishSnapshot()
        visitors.values.toList().forEach { visitor ->
            scheduleBindAttempt(visitor, identity)
        }
    }

    private suspend fun transitionControlUnavailable(
        nextPhase: String,
        failure: KotlinFrpcStcpVisitorFailure?,
        controlFailure: FrpControlFailure?,
    ) {
        currentIdentity = null
        invalidateAllRuntimeResources(
            visitorPhase = when {
                failure != null -> VISITOR_PHASE_FAILED
                nextPhase == PHASE_CLOSED -> VISITOR_PHASE_STOPPED
                else -> VISITOR_PHASE_PENDING
            },
            visitorFailure = failure,
            preserveExistingFailures = failure != null,
        )
        phase = nextPhase
        lastFailure = failure
        lastControlFailure = controlFailure
        publishSnapshot()
    }

    private suspend fun scheduleBindAttempt(visitor: RuntimeVisitor, identity: FrpControlIdentity) {
        if (!canBind(visitor, identity)) return
        cancelAndJoinBindTask(visitor)
        val key = RuntimeBindKey(
            identity = identity,
            visitorName = visitor.config.name,
            desiredRevision = visitor.desiredRevision,
            attemptOrdinal = ++listenerAttemptSequence,
        )
        visitor.phase = VISITOR_PHASE_STARTING
        visitor.listenerBound = false
        visitor.boundIdentity = null
        visitor.lastFailure = null
        publishSnapshot()
        val pendingCommand = AtomicReference<BindCompletedCommand?>()
        val job = launchOwned("KotlinFrpcVisitorBind") {
            runBindAttempt(key, visitor.config.bindPort, pendingCommand)
        }
        visitor.bindTask = RuntimeBindTask(key, job, pendingCommand)
    }

    private fun scheduleBindRetry(visitor: RuntimeVisitor, identity: FrpControlIdentity) {
        if (!canBind(visitor, identity) || visitor.bindTask != null) return
        val key = RuntimeBindKey(
            identity = identity,
            visitorName = visitor.config.name,
            desiredRevision = visitor.desiredRevision,
            attemptOrdinal = ++listenerAttemptSequence,
        )
        // A port held by another app rarely frees itself within seconds; back off instead of probing it forever.
        val retryDelayMillis = (VISITOR_BIND_RETRY_DELAY_MILLIS shl visitor.bindRetries.coerceAtMost(5))
            .coerceAtMost(MAXIMUM_VISITOR_BIND_RETRY_DELAY_MILLIS)
        visitor.bindRetries = (visitor.bindRetries + 1).coerceAtMost(Int.MAX_VALUE - 1)
        val job = launchOwned("KotlinFrpcVisitorBindRetry") {
            try {
                runtimeDelayer.delayMillis(retryDelayMillis)
                mailbox.send(BindRetryDueCommand(key))
            } catch (failure: CancellationException) {
                throw failure
            } catch (failure: Error) {
                sendBindFatal(key, failure)
            } catch (_: Exception) {
                sendBindFailure(key, KotlinFrpcStcpVisitorFailure.LISTENER_BIND_FAILED)
            }
        }
        visitor.bindTask = RuntimeBindTask(key, job, AtomicReference())
    }

    private suspend fun runBindAttempt(
        key: RuntimeBindKey,
        bindPort: Int,
        pendingCommand: AtomicReference<BindCompletedCommand?>,
    ) {
        var listener: FrpLocalListener? = null
        var command: BindCompletedCommand? = null
        try {
            lifecycleHooks.beforeVisitorBind(key.desiredRevision, key.identity)
            listener = listenerFactory.bind(IPV4_LOOPBACK, bindPort)
            lifecycleHooks.afterVisitorBind(key.desiredRevision, key.identity)
            command = BindCompletedCommand(key, listener, null, null)
            pendingCommand.set(command)
            mailbox.send(command)
            currentCoroutineContext().ensureActive()
            listener = null
            command = null
        } catch (failure: CancellationException) {
            command?.fail(failure)
            if (command != null) listener = null
            throw failure
        } catch (failure: Error) {
            command?.fail(failure)
            if (command != null) listener = null
            sendBindFatal(key, failure)
        } catch (_: BindException) {
            command?.fail(runtimeFailure(KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT))
            if (command != null) listener = null
            sendBindFailure(key, KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT)
        } catch (_: Exception) {
            command?.fail(runtimeFailure(KotlinFrpcStcpVisitorFailure.LISTENER_BIND_FAILED))
            if (command != null) listener = null
            sendBindFailure(key, KotlinFrpcStcpVisitorFailure.LISTENER_BIND_FAILED)
        } finally {
            listener?.let(::closeListenerExpected)
        }
    }

    private suspend fun sendBindFailure(
        key: RuntimeBindKey,
        failure: KotlinFrpcStcpVisitorFailure,
    ) {
        try {
            mailbox.send(
                BindCompletedCommand(
                    key = key,
                    listener = null,
                    failure = failure,
                    fatal = null,
                ),
            )
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            // Session cleanup owns the invalidated bind key.
        }
    }

    private suspend fun sendBindFatal(key: RuntimeBindKey, fatal: Throwable) {
        try {
            mailbox.send(BindCompletedCommand(key, null, null, fatal))
        } catch (_: CancellationException) {
            throw fatal
        } catch (_: Error) {
            throw fatal
        } catch (_: Exception) {
            throw fatal
        }
    }

    private suspend fun handleBindCompleted(command: BindCompletedCommand) {
        val visitor = visitors[command.key.visitorName]
        if (visitor == null) {
            command.listener?.let(::closeListenerExpected)
            command.fatal?.let { throw it }
            return
        }
        val task = visitor.bindTask
        val taskMatches = task?.key == command.key
        if (taskMatches) {
            checkNotNull(task).pendingCommand.compareAndSet(command, null)
            checkNotNull(task).job.join()
            visitor.bindTask = null
        }
        if (!taskMatches || !canBind(visitor, command.key.identity) ||
            visitor.desiredRevision != command.key.desiredRevision
        ) {
            command.listener?.let(::closeListenerExpected)
            command.fatal?.let { throw it }
            return
        }
        command.fatal?.let { throw it }
        val listener = command.listener
        if (listener == null || command.failure != null || listener.bindPort != visitor.config.bindPort) {
            listener?.let(::closeListenerExpected)
            visitor.phase = VISITOR_PHASE_FAILED
            visitor.listenerBound = false
            visitor.boundIdentity = null
            visitor.lastFailure = command.failure ?: KotlinFrpcStcpVisitorFailure.LISTENER_BIND_FAILED
            publishSnapshot()
            scheduleBindRetry(visitor, command.key.identity)
            return
        }
        val owner = RuntimeListenerOwner(command.key, listener) {
            synchronized(resourceLock) { relayVersion.value += 1 }
        }
        visitor.listener = owner
        visitor.bindPort = listener.bindPort
        visitor.phase = VISITOR_PHASE_READY
        visitor.listenerBound = true
        visitor.boundIdentity = command.key.identity
        visitor.lastFailure = null
        visitor.bindRetries = 0
        registerListener(owner)
        val acceptJob = launchOwned("KotlinFrpcVisitorAccept") { runAcceptLoop(owner) }
        owner.attach(acceptJob)
        publishSnapshot()
    }

    private suspend fun handleBindRetryDue(command: BindRetryDueCommand) {
        val visitor = visitors[command.key.visitorName] ?: return
        val task = visitor.bindTask ?: return
        if (task.key != command.key) return
        task.job.join()
        visitor.bindTask = null
        if (canBind(visitor, command.key.identity) && visitor.desiredRevision == command.key.desiredRevision) {
            scheduleBindAttempt(visitor, command.key.identity)
        }
    }

    private fun canBind(visitor: RuntimeVisitor, identity: FrpControlIdentity): Boolean =
        visitors[visitor.config.name] === visitor && currentIdentity == identity && phase == PHASE_OPEN &&
            isControlIdentityOpen(identity) && !stopRequested.get()

    private suspend fun runAcceptLoop(owner: RuntimeListenerOwner) {
        var terminalFatal: Throwable? = null
        var notifyFailure = false
        try {
            while (!owner.isClosed && !stopRequested.get()) {
                val connection = owner.listener.accept()
                val command = ListenerAcceptedCommand(owner, connection)
                val result = mailbox.trySend(command)
                if (!result.isSuccess) {
                    command.fail(runtimeFailure(KotlinFrpcStcpVisitorFailure.MAILBOX_FULL))
                    notifyFailure = !result.isClosed
                    break
                }
            }
        } catch (failure: CancellationException) {
            if (!owner.isClosed && !stopRequested.get()) terminalFatal = failure
        } catch (failure: Error) {
            terminalFatal = failure
        } catch (_: Exception) {
            notifyFailure = !owner.isClosed && !stopRequested.get()
        } finally {
            if ((notifyFailure || terminalFatal != null) && !stopRequested.get()) {
                invalidateListenerFromAccept(owner, terminalFatal)
            }
        }
    }

    private fun invalidateListenerFromAccept(owner: RuntimeListenerOwner, fatal: Throwable?) {
        var selectedFatal = fatal
        try {
            owner.close()
        } catch (failure: CancellationException) {
            selectedFatal = preferRuntimeFatal(selectedFatal, failure)
        } catch (failure: Error) {
            selectedFatal = preferRuntimeFatal(selectedFatal, failure)
        }
        launchOwned("KotlinFrpcListenerFailure") {
            try {
                mailbox.send(ListenerFailedCommand(owner, selectedFatal))
            } catch (failure: CancellationException) {
                if (selectedFatal != null) throw checkNotNull(selectedFatal)
                throw failure
            } catch (failure: Error) {
                throw preferRuntimeFatal(selectedFatal, failure)
            } catch (_: Exception) {
                selectedFatal?.let { throw it }
            }
        }
    }

    private suspend fun handleListenerAccepted(command: ListenerAcceptedCommand) {
        if (stopRequested.get()) {
            command.fail(runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED))
            return
        }
        val owner = command.owner
        val visitor = visitors[owner.visitorName]
        if (visitor == null || visitor.listener !== owner || owner.isClosed ||
            visitor.desiredRevision != owner.desiredRevision || currentIdentity != owner.identity ||
            !isControlIdentityOpen(owner.identity) || visitor.phase != VISITOR_PHASE_READY
        ) {
            command.fail(runtimeFailure(KotlinFrpcStcpVisitorFailure.VISITOR_FAILED))
            return
        }
        pruneCompletedRelays()
        if (relays.size >= MAXIMUM_RELAYS_PER_SESSION) {
            command.fail(runtimeFailure(KotlinFrpcStcpVisitorFailure.RELAY_LIMIT))
            return
        }
        val relayPermit = relayBudget.tryAcquire()
        if (relayPermit == null) {
            command.fail(runtimeFailure(KotlinFrpcStcpVisitorFailure.RELAY_LIMIT))
            return
        }
        val relay = FrpVisitorRelay(
            visitorRevision = visitor.desiredRevision,
            listenerAttempt = owner.attempt,
            control = control,
            identity = owner.identity,
            visitor = FrpRelayVisitorConfig(
                serverName = visitor.config.serverName,
                serverUser = visitor.config.serverUser,
                sessionUser = sessionConfig.user.orEmpty(),
                secretKey = visitor.config.secretKey,
                useEncryption = visitor.config.useEncryption,
                useCompression = visitor.config.useCompression,
            ),
            local = command.connection,
            parentScope = scope,
            unixTimeSource = unixTimeSource,
            ioDispatcher = ioDispatcher,
            beforeHandshakeCommit = lifecycleHooks.beforeVisitorHandshakeCommit,
            handshakeTimeoutMillis = lifecycleHooks.visitorHandshakeTimeoutMillis,
        )
        val record = RuntimeRelayRecord(owner.visitorName, owner, relay, relayPermit)
        relays += record
        registerRelay(record)
        try {
            relay.start()
        } catch (failure: CancellationException) {
            relays.remove(record)
            unregisterRelay(record)
            record.releasePermit()
            relay.close()
            throw failure
        } catch (failure: Error) {
            relays.remove(record)
            unregisterRelay(record)
            record.releasePermit()
            relay.close()
            throw failure
        } catch (failure: Exception) {
            relays.remove(record)
            unregisterRelay(record)
            record.releasePermit()
            relay.close()
            return
        }
        launchOwned("KotlinFrpcRelayWatcher", start = CoroutineStart.UNDISPATCHED) {
            var fatal: Throwable? = null
            try {
                withContext(NonCancellable) { relay.awaitStopped() }
            } catch (failure: CancellationException) {
                fatal = failure
            } catch (failure: Error) {
                fatal = failure
            } catch (_: Exception) {
                // Ordinary relay failure is isolated.
            }
            unregisterRelay(record)
            record.releasePermit()
            try {
                lifecycleHooks.afterRelayCompletion()
            } catch (failure: CancellationException) {
                if (!stopRequested.get()) fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Test-only observation cannot retain relay ownership.
            }
            fatal?.let(::recordRelayFatal)
            try {
                mailbox.send(RelayFinishedCommand(record, fatal))
            } catch (failure: CancellationException) {
                if (fatal != null) throw checkNotNull(fatal)
                throw failure
            } catch (failure: Error) {
                throw preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Session cleanup still owns the registered relay record.
            }
        }
        lifecycleHooks.afterRelayWatcherScheduled()
    }

    private suspend fun handleListenerFailed(command: ListenerFailedCommand) {
        command.fatal?.let { throw it }
        if (stopRequested.get()) return
        val visitor = visitors[command.owner.visitorName] ?: return
        if (visitor.listener !== command.owner) return
        closeVisitorRuntime(visitor)
        visitor.phase = VISITOR_PHASE_FAILED
        visitor.lastFailure = KotlinFrpcStcpVisitorFailure.VISITOR_FAILED
        publishSnapshot()
        currentIdentity?.takeIf { canBind(visitor, it) }?.let { scheduleBindRetry(visitor, it) }
    }

    private suspend fun handleRelayFinished(command: RelayFinishedCommand) {
        relays.remove(command.record)
        unregisterRelay(command.record)
        command.record.releasePermit()
        command.fatal?.let { throw it }
    }

    private suspend fun handleStopSession() {
        cleanupPhase.set(KotlinFrpcSessionCleanupPhase.ACTOR_INVALIDATING)
        phase = PHASE_STOPPING
        currentIdentity = null
        lastFailure = null
        invalidateAllRuntimeResources(VISITOR_PHASE_STOPPED, null)
        publishSnapshot()
        controlCollector.get()?.cancel()
        try {
            control.close()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            // Control shutdown remains owned and awaited below.
        }
        cleanupPhase.set(KotlinFrpcSessionCleanupPhase.ACTOR_WAITING_FOR_CONTROL)
        try {
            control.awaitStopped()
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Error) {
            throw failure
        } catch (_: Exception) {
            // Runtime is locally closed even if transport cleanup reports ordinary I/O.
        }
        phase = PHASE_CLOSED
        publishSnapshot()
        stopCleanupCompleted.set(true)
        cleanupPhase.set(KotlinFrpcSessionCleanupPhase.ACTOR_COMPLETE)
    }

    private suspend fun emergencyCleanup() {
        cleanupPhase.set(KotlinFrpcSessionCleanupPhase.EMERGENCY)
        withContext(NonCancellable) {
            var fatal: Throwable? = null
            fatal = preferRuntimeFatalOrNull(
                fatal,
                closeMailboxAndDrain(runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)),
            )
            fatal = preferRuntimeFatalOrNull(fatal, signalEmergencyResourceClosure())
            controlCollector.get()?.cancel()
            actorJob.get()?.cancel()
            try {
                actorJob.get()?.join()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            }
            fatal = preferRuntimeFatalOrNull(fatal, signalEmergencyResourceClosure())
            try {
                control.close()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Continue joining every owned job and resource.
            }
            ownerJob.cancel()
            try {
                joinAllSessionJobs()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            }
            fatal = preferRuntimeFatalOrNull(fatal, observedRelayFatal())
            controlCollector.set(null)
            try {
                invalidateAllRuntimeResources(VISITOR_PHASE_STOPPED, null)
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Continue invalidating the control owner.
            }
            try {
                control.awaitStopped()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Best effort after an actor failure.
            }
            currentIdentity = null
            phase = PHASE_CLOSED
            lastFailure = null
            publishSnapshot()
            stopCleanupCompleted.set(true)
            fatal?.let { throw it }
        }
    }

    private suspend fun finalizeNormalCleanup() {
        withContext(NonCancellable) {
            var fatal: Throwable? = null
            controlCollector.get()?.cancel()
            ownerJob.cancel()
            cleanupPhase.set(KotlinFrpcSessionCleanupPhase.FINALIZING_JOBS)
            try {
                joinAllSessionJobs()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            }
            controlCollector.set(null)
            cleanupPhase.set(KotlinFrpcSessionCleanupPhase.FINALIZING_CONTROL)
            try {
                control.awaitStopped()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Logical cleanup is complete even if transport shutdown reports ordinary I/O.
            }
            cleanupPhase.set(KotlinFrpcSessionCleanupPhase.FINALIZING_MAILBOX)
            fatal = preferRuntimeFatalOrNull(
                fatal,
                closeMailboxAndDrain(runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)),
            )
            fatal = preferRuntimeFatalOrNull(fatal, observedRelayFatal())
            fatal?.let { throw it }
        }
    }

    private suspend fun invalidateAllRuntimeResources(
        visitorPhase: String,
        visitorFailure: KotlinFrpcStcpVisitorFailure?,
        preserveExistingFailures: Boolean = false,
    ) {
        var fatal: Throwable? = null
        try {
            closeRelays { true }
        } catch (failure: CancellationException) {
            fatal = preferRuntimeFatal(fatal, failure)
        } catch (failure: Error) {
            fatal = preferRuntimeFatal(fatal, failure)
        }
        visitors.values.forEach { visitor ->
            val preservedFailure = if (preserveExistingFailures && visitor.phase == VISITOR_PHASE_FAILED) {
                visitor.lastFailure
            } else {
                null
            }
            try {
                cancelAndJoinBindTask(visitor)
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            }
            try {
                closeListener(visitor)
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            }
            visitor.phase = if (preservedFailure != null) VISITOR_PHASE_FAILED else visitorPhase
            visitor.listenerBound = false
            visitor.boundIdentity = null
            visitor.lastFailure = preservedFailure ?: visitorFailure
        }
        fatal?.let { throw it }
    }

    private suspend fun closeVisitorRuntime(visitor: RuntimeVisitor) {
        var fatal: Throwable? = null
        try {
            closeRelays { record -> record.visitorName == visitor.config.name }
        } catch (failure: CancellationException) {
            fatal = preferRuntimeFatal(fatal, failure)
        } catch (failure: Error) {
            fatal = preferRuntimeFatal(fatal, failure)
        }
        try {
            cancelAndJoinBindTask(visitor)
        } catch (failure: CancellationException) {
            fatal = preferRuntimeFatal(fatal, failure)
        } catch (failure: Error) {
            fatal = preferRuntimeFatal(fatal, failure)
        }
        try {
            closeListener(visitor)
        } catch (failure: CancellationException) {
            fatal = preferRuntimeFatal(fatal, failure)
        } catch (failure: Error) {
            fatal = preferRuntimeFatal(fatal, failure)
        }
        visitor.listenerBound = false
        visitor.boundIdentity = null
        fatal?.let { throw it }
    }

    private suspend fun closeListener(visitor: RuntimeVisitor) {
        val owner = visitor.listener ?: return
        visitor.listener = null
        var fatal: Throwable? = null
        try {
            owner.close()
        } catch (failure: CancellationException) {
            fatal = preferRuntimeFatal(fatal, failure)
        } catch (failure: Error) {
            fatal = preferRuntimeFatal(fatal, failure)
        }
        try {
            owner.awaitStopped()
        } catch (failure: CancellationException) {
            fatal = preferRuntimeFatal(fatal, failure)
        } catch (failure: Error) {
            fatal = preferRuntimeFatal(fatal, failure)
        }
        unregisterListener(owner)
        fatal?.let { throw it }
    }

    private suspend fun closeRelays(predicate: (RuntimeRelayRecord) -> Boolean) {
        val selected = relays.filter(predicate)
        relays.removeAll(selected.toSet())
        var fatal: Throwable? = null
        selected.forEach { record ->
            try {
                record.relay.close()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Every relay has still been synchronously invalidated.
            }
        }
        selected.forEach { record ->
            try {
                record.relay.awaitLocallyStopped()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Ordinary relay I/O cannot fail listener or session cleanup.
            }
        }
        fatal?.let { throw it }
    }

    private suspend fun cancelAndJoinBindTask(visitor: RuntimeVisitor) {
        val task = visitor.bindTask ?: return
        visitor.bindTask = null
        task.job.cancel()
        task.job.join()
        task.pendingCommand.getAndSet(null)?.fail(
            runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED),
        )
    }

    private fun launchOwned(
        name: String,
        start: CoroutineStart = CoroutineStart.DEFAULT,
        block: suspend CoroutineScope.() -> Unit,
    ): Job {
        val job = scope.launch(CoroutineName(name), start = start, block = block)
        synchronized(resourceLock) { ownedJobs += job }
        job.invokeOnCompletion { synchronized(resourceLock) { ownedJobs -= job } }
        return job
    }

    private fun registerListener(owner: RuntimeListenerOwner) {
        synchronized(resourceLock) { ownedListeners += owner }
    }

    private fun unregisterListener(owner: RuntimeListenerOwner) {
        synchronized(resourceLock) { ownedListeners -= owner }
    }

    private fun registerRelay(record: RuntimeRelayRecord) {
        synchronized(resourceLock) { ownedRelays += record; relayVersion.value += 1 }
    }

    private fun unregisterRelay(record: RuntimeRelayRecord) {
        synchronized(resourceLock) { ownedRelays -= record; relayVersion.value += 1 }
    }

    private fun recordRelayFatal(failure: Throwable) {
        synchronized(resourceLock) {
            observedRelayFatal = preferRuntimeFatal(observedRelayFatal, failure)
        }
    }

    private fun observedRelayFatal(): Throwable? = synchronized(resourceLock) { observedRelayFatal }

    private fun signalEmergencyResourceClosure(): Throwable? {
        val jobs: List<Job>
        val listeners: List<RuntimeListenerOwner>
        val activeRelays: List<RuntimeRelayRecord>
        synchronized(resourceLock) {
            jobs = ownedJobs.toList()
            listeners = ownedListeners.toList()
            activeRelays = ownedRelays.toList()
        }
        var fatal: Throwable? = null
        jobs.forEach(Job::cancel)
        listeners.forEach { owner ->
            try {
                owner.close()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            }
        }
        activeRelays.forEach { record ->
            try {
                record.relay.close()
            } catch (failure: CancellationException) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (failure: Error) {
                fatal = preferRuntimeFatal(fatal, failure)
            } catch (_: Exception) {
                // Logical relay authority is already invalidated.
            }
        }
        return fatal
    }

    private suspend fun joinAllSessionJobs() {
        while (true) {
            val jobs = buildList {
                actorJob.get()?.let(::add)
                controlCollector.get()?.let(::add)
                synchronized(resourceLock) { addAll(ownedJobs) }
            }.distinct().filter { !it.isCompleted }
            if (jobs.isEmpty()) return
            jobs.joinAll()
        }
    }

    private fun closeMailboxAndDrain(failure: Throwable): Throwable? {
        mailbox.close()
        var fatal: Throwable? = null
        while (true) {
            val command = mailbox.tryReceive().getOrNull() ?: break
            try {
                command.fail(failure)
            } catch (candidate: CancellationException) {
                fatal = preferRuntimeFatal(fatal, candidate)
            } catch (candidate: Error) {
                fatal = preferRuntimeFatal(fatal, candidate)
            } catch (_: Exception) {
                // Reply and resource failures are already represented by the selected failure.
            }
        }
        return fatal
    }

    fun cleanupDiagnostics(): KotlinFrpcSessionCleanupDiagnostics = synchronized(resourceLock) {
        val relayPhases = ownedRelays.groupingBy { it.relay.cleanupPhase }.eachCount()
        KotlinFrpcSessionCleanupDiagnostics(
            phase = cleanupPhase.get(),
            activeOwnedJobs = ownedJobs.count { !it.isCompleted },
            activeListeners = ownedListeners.size,
            activeRelays = ownedRelays.size,
            openingRelays = relayPhases[FrpVisitorRelayCleanupPhase.OPENING] ?: 0,
            relayingRelays = relayPhases[FrpVisitorRelayCleanupPhase.RELAYING] ?: 0,
            resettingLeaseRelays = relayPhases[FrpVisitorRelayCleanupPhase.RESETTING_LEASE] ?: 0,
            resettingAdapterRelays = relayPhases[FrpVisitorRelayCleanupPhase.RESETTING_ADAPTER] ?: 0,
            actorCompleted = actorJob.get()?.isCompleted == true,
            controlCollectorCompleted = controlCollector.get()?.isCompleted != false,
        )
    }

    private fun isControlIdentityOpen(identity: FrpControlIdentity): Boolean =
        (control.state.value as? FrpControlState.Open)?.identity == identity

    private suspend fun pruneCompletedRelays() {
        val completed = relays.filter { it.relay.isCompleted }
        relays.removeAll(completed.toSet())
        completed.forEach { record ->
            try {
                record.relay.awaitStopped()
            } catch (failure: CancellationException) {
                throw failure
            } catch (failure: Error) {
                throw failure
            } catch (_: Exception) {
                // An old relay's ordinary I/O failure cannot reject the next accepted socket.
            } finally {
                unregisterRelay(record)
                record.releasePermit()
            }
        }
    }

    private fun publishSnapshot() {
        val publicVisitors = LinkedHashMap<String, FrpcVisitorRuntimeState>(visitors.size)
        val waiterVisitors = LinkedHashMap<String, WaitVisitorState>(visitors.size)
        visitors.forEach { (name, visitor) ->
            val boundEpoch = visitor.boundIdentity?.controlEpoch ?: 0L
            publicVisitors[name] = FrpcVisitorRuntimeState(
                desiredRevision = visitor.desiredRevision,
                phase = visitor.phase,
                listenerBound = visitor.listenerBound,
                boundControlEpoch = boundEpoch,
                lastError = visitor.lastFailure?.description,
                activeConnections = synchronized(resourceLock) {
                    ownedRelays.count { it.visitorName == name }
                },
            )
            waiterVisitors[name] = WaitVisitorState(
                desiredRevision = visitor.desiredRevision,
                phase = visitor.phase,
                listenerBound = visitor.listenerBound,
                boundIdentity = visitor.boundIdentity,
                failure = visitor.lastFailure,
                listenerOwner = visitor.listener,
            )
        }
        mutableSnapshot.value = RuntimeSnapshot(
            publicState = FrpcStcpVisitorState(
                sessionId = sessionId,
                phase = phase,
                configRevision = configRevision,
                controlEpoch = controlEpoch,
                lastError = lastControlFailure?.description ?: lastFailure?.description,
                visitors = publicVisitors,
            ),
            currentIdentity = currentIdentity,
            waiterVisitors = waiterVisitors,
        )
    }

    private fun nextConfigRevision(): Long {
        if (configRevision == Long.MAX_VALUE) {
            throw runtimeFailure(KotlinFrpcStcpVisitorFailure.VISITOR_LIMIT)
        }
        return configRevision + 1L
    }

    private companion object {
        const val SESSION_MAILBOX_CAPACITY = 64
        const val MAXIMUM_VISITORS_PER_SESSION = 32
        const val MAXIMUM_RELAYS_PER_SESSION = 100
        const val VISITOR_BIND_RETRY_DELAY_MILLIS = 10_000L
        const val MAXIMUM_VISITOR_BIND_RETRY_DELAY_MILLIS = 5 * 60_000L
    }
}

private sealed interface RuntimeCommand {
    val kind: String
        get() = javaClass.simpleName

    fun fail(failure: Throwable) = Unit
}

private class EnsureVisitorCommand(
    val config: NormalizedVisitorConfig,
    val reply: CompletableDeferred<FrpcEnsureVisitorResult>,
) : RuntimeCommand {
    override fun fail(failure: Throwable) {
        reply.completeExceptionally(failure)
    }

    override fun toString(): String = "EnsureVisitorCommand(config=$config, completed=${reply.isCompleted})"
}

private class StopVisitorCommand(
    @Suppress("unused") val visitorName: String,
    val reply: CompletableDeferred<Unit>,
) : RuntimeCommand {
    override fun fail(failure: Throwable) {
        reply.completeExceptionally(failure)
    }

    override fun toString(): String = "StopVisitorCommand(completed=${reply.isCompleted})"
}

private class ControlChangedCommand(val state: FrpControlState) : RuntimeCommand {
    override fun toString(): String = "ControlChangedCommand(state=$state)"
}

private class ListenerAcceptedCommand(
    val owner: RuntimeListenerOwner,
    val connection: FrpLocalConnection,
) : RuntimeCommand {
    override fun fail(failure: Throwable) {
        closeConnectionExpected(connection)
    }

    override fun toString(): String = "ListenerAcceptedCommand(owner=$owner, connectionPresent=true)"
}

private class BindCompletedCommand(
    val key: RuntimeBindKey,
    val listener: FrpLocalListener?,
    val failure: KotlinFrpcStcpVisitorFailure?,
    val fatal: Throwable?,
) : RuntimeCommand {
    override fun fail(failure: Throwable) {
        var selectedFatal = fatal
        try {
            listener?.let(::closeListenerExpected)
        } catch (candidate: CancellationException) {
            selectedFatal = preferRuntimeFatal(selectedFatal, candidate)
        } catch (candidate: Error) {
            selectedFatal = preferRuntimeFatal(selectedFatal, candidate)
        }
        selectedFatal?.let { throw it }
    }

    override fun toString(): String =
        "BindCompletedCommand(key=$key, listenerPresent=${listener != null}, " +
            "failurePresent=${this.failure != null}, fatalPresent=${fatal != null})"
}

private class BindRetryDueCommand(val key: RuntimeBindKey) : RuntimeCommand {
    override fun toString(): String = "BindRetryDueCommand(key=$key)"
}

private class ListenerFailedCommand(
    val owner: RuntimeListenerOwner,
    val fatal: Throwable?,
) : RuntimeCommand {
    override fun fail(failure: Throwable) {
        fatal?.let { throw it }
    }

    override fun toString(): String = "ListenerFailedCommand(owner=$owner, fatalPresent=${fatal != null})"
}

private class RelayFinishedCommand(
    val record: RuntimeRelayRecord,
    val fatal: Throwable?,
) : RuntimeCommand {
    override fun fail(failure: Throwable) {
        fatal?.let { throw it }
    }

    override fun toString(): String = "RelayFinishedCommand(record=$record, fatalPresent=${fatal != null})"
}

private data object StopSessionCommand : RuntimeCommand

private data class RuntimeBindKey(
    val identity: FrpControlIdentity,
    val visitorName: String,
    val desiredRevision: Long,
    val attemptOrdinal: Long,
) {
    override fun toString(): String =
        "RuntimeBindKey(identity=$identity, desiredRevision=$desiredRevision, " +
            "attemptOrdinal=$attemptOrdinal, namePresent=${visitorName.isNotEmpty()})"
}

private data class RuntimeBindTask(
    val key: RuntimeBindKey,
    val job: Job,
    val pendingCommand: AtomicReference<BindCompletedCommand?>,
)

private data class RuntimeVisitor(
    val config: NormalizedVisitorConfig,
    val desiredRevision: Long,
    var bindPort: Int,
    var phase: String,
    var listenerBound: Boolean = false,
    var boundIdentity: FrpControlIdentity? = null,
    var lastFailure: KotlinFrpcStcpVisitorFailure? = null,
    var listener: RuntimeListenerOwner? = null,
    var bindTask: RuntimeBindTask? = null,
    var bindRetries: Int = 0,
) {
    override fun toString(): String =
        "RuntimeVisitor(config=$config, desiredRevision=$desiredRevision, bindPort=$bindPort, " +
            "phase=$phase, listenerBound=$listenerBound, boundIdentityPresent=${boundIdentity != null}, " +
            "lastErrorPresent=${lastFailure != null}, listenerPresent=${listener != null}, " +
            "bindTaskPresent=${bindTask != null})"
}

private class RuntimeListenerOwner(
    val key: RuntimeBindKey,
    val listener: FrpLocalListener,
    private val onStateChanged: () -> Unit,
) {
    private val closed = AtomicBoolean(false)
    private val acceptJob = AtomicReference<Job?>()

    val isClosed: Boolean
        get() = closed.get()

    val isLive: Boolean
        get() = !closed.get() && acceptJob.get()?.isActive == true

    val visitorName: String
        get() = key.visitorName

    val desiredRevision: Long
        get() = key.desiredRevision

    val identity: FrpControlIdentity
        get() = key.identity

    val attempt: Long
        get() = key.attemptOrdinal

    fun attach(job: Job) {
        if (!acceptJob.compareAndSet(null, job)) {
            job.cancel()
            throw IllegalStateException("listener accept job already attached")
        }
        if (closed.get()) job.cancel()
        job.invokeOnCompletion { onStateChanged() }
    }

    fun close() {
        if (!closed.compareAndSet(false, true)) return
        onStateChanged()
        var fatal: Throwable? = null
        try {
            listener.close()
        } catch (failure: CancellationException) {
            fatal = failure
        } catch (failure: Error) {
            fatal = failure
        } catch (_: Exception) {
            // Closing a listener is best effort after authority is invalidated.
        }
        acceptJob.get()?.cancel()
        fatal?.let { throw it }
    }

    suspend fun awaitStopped() {
        acceptJob.get()?.join()
    }

    override fun toString(): String =
        "RuntimeListenerOwner(desiredRevision=$desiredRevision, identity=$identity, attempt=$attempt, " +
            "closed=${closed.get()}, jobAttached=${acceptJob.get() != null})"
}

private data class RuntimeRelayRecord(
    val visitorName: String,
    val listenerOwner: RuntimeListenerOwner,
    val relay: FrpVisitorRelay,
    private val permit: RuntimeRelayPermit,
) {
    fun releasePermit() {
        permit.release()
    }

    override fun toString(): String = "RuntimeRelayRecord(listenerOwner=$listenerOwner, relay=$relay)"
}

private class RuntimeRelayBudget(maximumRelays: Int) {
    private val semaphore = Semaphore(maximumRelays)
    private val maximumRelays = maximumRelays
    private val rejected = AtomicInteger(0)

    val activeCount: Int
        get() = maximumRelays - semaphore.availablePermits

    val rejectedCount: Int
        get() = rejected.get()

    fun tryAcquire(): RuntimeRelayPermit? {
        if (!semaphore.tryAcquire()) {
            rejected.incrementAndGet()
            return null
        }
        return RuntimeRelayPermit(semaphore)
    }
}

private class RuntimeRelayPermit(private val semaphore: Semaphore) {
    private val released = AtomicBoolean(false)

    fun release() {
        if (released.compareAndSet(false, true)) semaphore.release()
    }
}

private data class RuntimeSnapshot(
    val publicState: FrpcStcpVisitorState,
    val currentIdentity: FrpControlIdentity?,
    val waiterVisitors: Map<String, WaitVisitorState>,
) {
    fun readyResult(
        visitorName: String,
        desiredRevision: Long,
        controlState: FrpControlState,
        stopping: Boolean,
    ): FrpcVisitorReadyResult? {
        val visitor = waiterVisitors[visitorName]
            ?: throw runtimeFailure(KotlinFrpcStcpVisitorFailure.VISITOR_NOT_FOUND)
        if (visitor.desiredRevision != desiredRevision) {
            if (visitor.desiredRevision > desiredRevision) {
                throw runtimeFailure(KotlinFrpcStcpVisitorFailure.VISITOR_SUPERSEDED)
            }
            return null
        }
        if (visitor.phase == VISITOR_PHASE_FAILED) {
            throw runtimeFailure(visitor.failure ?: KotlinFrpcStcpVisitorFailure.VISITOR_FAILED)
        }
        if (visitor.phase == VISITOR_PHASE_STOPPED) {
            throw runtimeFailure(KotlinFrpcStcpVisitorFailure.VISITOR_NOT_FOUND)
        }
        if (visitor.listenerBound && visitor.listenerOwner?.isLive != true) {
            throw runtimeFailure(KotlinFrpcStcpVisitorFailure.VISITOR_FAILED)
        }
        if (stopping) throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
        when (controlState) {
            is FrpControlState.Failed -> throw runtimeFailure(KotlinFrpcStcpVisitorFailure.CONTROL_FAILED)
            is FrpControlState.Closed -> throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
            else -> Unit
        }
        when (publicState.phase) {
            PHASE_FAILED -> throw runtimeFailure(KotlinFrpcStcpVisitorFailure.CONTROL_FAILED)
            PHASE_STOPPING, PHASE_CLOSED -> throw runtimeFailure(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
        }
        val boundIdentity = visitor.boundIdentity
        val liveIdentity = (controlState as? FrpControlState.Open)?.identity
        if (publicState.phase != PHASE_OPEN || visitor.phase != VISITOR_PHASE_READY ||
            !visitor.listenerBound || boundIdentity == null || boundIdentity != currentIdentity ||
            boundIdentity != liveIdentity
        ) {
            return null
        }
        return FrpcVisitorReadyResult(
            name = visitorName,
            desiredRevision = desiredRevision,
            phase = VISITOR_PHASE_READY,
            listenerBound = true,
            boundControlEpoch = boundIdentity.controlEpoch,
            lastError = null,
        )
    }

    fun publicStateFor(
        controlState: FrpControlState,
        stopping: Boolean,
        reconnectEstablishmentFailure: FrpControlFailure?,
        reconnectCleanupFailure: FrpControlFailure?,
        reconnectDiagnostics: FrpControlReconnectDiagnostics,
    ): FrpcStcpVisitorState {
        val livePublicState = publicState.copy(
            visitors = publicState.visitors.mapValues { (name, visitor) ->
                val ownerLive = waiterVisitors[name]?.listenerOwner?.isLive == true
                if (visitor.listenerBound && !ownerLive) {
                    visitor.copy(
                        phase = VISITOR_PHASE_FAILED,
                        listenerBound = false,
                        boundControlEpoch = 0L,
                        lastError = KotlinFrpcStcpVisitorFailure.VISITOR_FAILED.description,
                    )
                } else {
                    visitor
                }
            },
        )
        if (stopping && publicState.phase != PHASE_CLOSED) {
            return unavailablePublicState(livePublicState, PHASE_STOPPING, null, VISITOR_PHASE_STOPPED)
        }
        return when (controlState) {
            is FrpControlState.Connecting -> unavailablePublicState(livePublicState, PHASE_CONNECTING, null)
            is FrpControlState.Retrying -> unavailablePublicState(
                livePublicState,
                PHASE_RECONNECTING,
                null,
                sessionError = reconnectCleanupFailure?.description
                    ?: reconnectEstablishmentFailure
                        ?.withReconnectTimerDiagnostics(reconnectDiagnostics)
                        ?.takeIf { failure ->
                            controlState.attempt <= 1 ||
                                failure == FrpControlFailure.RECONNECT_ABORTING_LOGIN_TIMEOUT ||
                                failure == FrpControlFailure.RECONNECT_WAITING_LOGIN_AFTER_TIMEOUT
                        }
                        ?.description
                    ?: controlState.failure.description,
            )
            is FrpControlState.Failed -> unavailablePublicState(
                livePublicState,
                PHASE_FAILED,
                KotlinFrpcStcpVisitorFailure.CONTROL_FAILED,
                sessionError = controlState.failure.description,
            )
            is FrpControlState.Closed -> if (publicState.phase == PHASE_CLOSED) {
                livePublicState
            } else {
                unavailablePublicState(livePublicState, PHASE_CLOSED, null, VISITOR_PHASE_STOPPED)
            }
            is FrpControlState.Open -> if (publicState.phase != PHASE_OPEN || controlState.identity == currentIdentity) {
                livePublicState
            } else {
                unavailablePublicState(livePublicState, PHASE_RECONNECTING, null)
            }
        }
    }

    private fun FrpControlFailure.withReconnectTimerDiagnostics(
        diagnostics: FrpControlReconnectDiagnostics,
    ): FrpControlFailure {
        if (this != FrpControlFailure.RECONNECT_WAITING_LOGIN || diagnostics.currentAttempt <= 0L) return this
        return when (diagnostics.currentAttempt) {
            diagnostics.loginTimerFiredAttempt -> FrpControlFailure.RECONNECT_LOGIN_TIMER_FIRED
            diagnostics.loginTimerScheduledAttempt -> FrpControlFailure.RECONNECT_LOGIN_TIMER_SCHEDULED
            else -> this
        }
    }

    override fun toString(): String =
        "RuntimeSnapshot(phase=${publicState.phase}, configRevision=${publicState.configRevision}, " +
            "controlEpoch=${publicState.controlEpoch}, currentIdentityPresent=${currentIdentity != null}, " +
            "visitorCount=${waiterVisitors.size})"

    private fun unavailablePublicState(
        base: FrpcStcpVisitorState,
        phase: String,
        failure: KotlinFrpcStcpVisitorFailure?,
        visitorPhase: String = if (failure == null) VISITOR_PHASE_PENDING else VISITOR_PHASE_FAILED,
        sessionError: String? = failure?.description,
    ): FrpcStcpVisitorState = base.copy(
        phase = phase,
        lastError = sessionError,
        visitors = base.visitors.mapValues { (_, visitor) ->
            visitor.copy(
                phase = visitorPhase,
                listenerBound = false,
                boundControlEpoch = 0L,
                lastError = failure?.let { visitor.lastError ?: it.description },
            )
        },
    )

    companion object {
        fun initial(sessionId: String): RuntimeSnapshot = RuntimeSnapshot(
            publicState = FrpcStcpVisitorState(sessionId = sessionId, phase = PHASE_CONNECTING),
            currentIdentity = null,
            waiterVisitors = emptyMap(),
        )
    }
}

private data class WaitVisitorState(
    val desiredRevision: Long,
    val phase: String,
    val listenerBound: Boolean,
    val boundIdentity: FrpControlIdentity?,
    val failure: KotlinFrpcStcpVisitorFailure?,
    val listenerOwner: RuntimeListenerOwner?,
) {
    override fun toString(): String =
        "WaitVisitorState(desiredRevision=$desiredRevision, phase=$phase, " +
            "listenerBound=$listenerBound, boundIdentityPresent=${boundIdentity != null}, " +
            "failurePresent=${failure != null}, listenerLive=${listenerOwner?.isLive == true})"
}

private data class NormalizedVisitorConfig(
    val name: String,
    val serverName: String,
    val serverUser: String,
    val secretKey: String,
    val bindPort: Int,
    val useEncryption: Boolean,
    val useCompression: Boolean,
) {
    override fun toString(): String =
        "NormalizedVisitorConfig(serverUserPresent=${serverUser.isNotEmpty()}, secretKey=<redacted>, " +
            "bindPort=$bindPort, useEncryption=$useEncryption, useCompression=$useCompression)"
}

private fun normalizeSessionConfig(config: FrpcSessionConfig): FrpcSessionConfig {
    val serverAddr = config.serverAddr.trim()
    val user = config.user?.trim()?.takeIf(String::isNotEmpty)
    val tlsServerName = config.tlsServerName?.trim()?.takeIf(String::isNotEmpty)
    val transport = config.transportProtocol.trim().ifEmpty { "tcp" }
    val wire = config.wireProtocol.trim().ifEmpty { "v1" }
    if (serverAddr.isEmpty() || serverAddr.length > MAXIMUM_SERVER_ADDRESS_LENGTH ||
        serverAddr.contains('\u0000') || config.serverPort !in 1..65_535 ||
        config.authToken.length > MAXIMUM_SECRET_LENGTH || config.authToken.contains('\u0000') ||
        user?.let { it.length > MAXIMUM_FRP_NAME_LENGTH || it.contains('\u0000') } == true ||
        tlsServerName?.let { it.length > MAXIMUM_SERVER_ADDRESS_LENGTH || it.contains('\u0000') } == true
    ) {
        throw runtimeFailure(KotlinFrpcStcpVisitorFailure.INVALID_CONFIGURATION)
    }
    if (transport != "tcp" || (wire != "v1" && wire != "v2")) {
        throw runtimeFailure(KotlinFrpcStcpVisitorFailure.UNSUPPORTED_CONFIGURATION)
    }
    return config.copy(
        serverAddr = serverAddr,
        user = user,
        tlsServerName = tlsServerName,
        transportProtocol = transport,
        wireProtocol = wire,
    )
}

private fun normalizeVisitorConfig(config: FrpcStcpVisitorConfig): NormalizedVisitorConfig {
    val name = normalizeVisitorName(config.name)
    val serverName = config.serverName.trim()
    val serverUser = config.serverUser?.trim().orEmpty()
    val secretKey = config.secretKey.trim()
    if (serverName.isEmpty() || serverName.length > MAXIMUM_FRP_NAME_LENGTH || serverName.contains('\u0000') ||
        serverUser.length > MAXIMUM_FRP_NAME_LENGTH || serverUser.contains('\u0000') ||
        secretKey.isEmpty() || secretKey.length > MAXIMUM_SECRET_LENGTH || secretKey.contains('\u0000') ||
        config.bindPort !in 1..65_535
    ) {
        throw runtimeFailure(KotlinFrpcStcpVisitorFailure.INVALID_CONFIGURATION)
    }
    return NormalizedVisitorConfig(
        name = name,
        serverName = serverName,
        serverUser = serverUser,
        secretKey = secretKey,
        bindPort = config.bindPort,
        useEncryption = config.useEncryption,
        useCompression = config.useCompression,
    )
}

private fun normalizeVisitorName(value: String): String {
    val normalized = value.trim()
    if (normalized.isEmpty() || normalized.length > MAXIMUM_FRP_NAME_LENGTH || normalized.contains('\u0000')) {
        throw runtimeFailure(KotlinFrpcStcpVisitorFailure.INVALID_CONFIGURATION)
    }
    return normalized
}

private fun validateTimeout(timeoutMillis: Long) {
    if (timeoutMillis !in MINIMUM_TIMEOUT_MILLIS..MAXIMUM_TIMEOUT_MILLIS) {
        throw runtimeFailure(KotlinFrpcStcpVisitorFailure.INVALID_CONFIGURATION)
    }
}

private fun normalizeStopTimeout(timeoutMillis: Long): Long {
    if (timeoutMillis <= 0L) return FrpcStcpVisitorClient.DEFAULT_STOP_TIMEOUT_MILLIS
    validateTimeout(timeoutMillis)
    return timeoutMillis
}

private fun mapRuntimeFailure(
    failure: Exception,
    fallback: KotlinFrpcStcpVisitorFailure,
): KotlinFrpcStcpVisitorException = when (failure) {
    is KotlinFrpcStcpVisitorException -> failure
    else -> KotlinFrpcStcpVisitorException(fallback, failure)
}

private fun runtimeFailure(failure: KotlinFrpcStcpVisitorFailure): KotlinFrpcStcpVisitorException =
    KotlinFrpcStcpVisitorException(failure)

private fun preferRuntimeFatal(current: Throwable?, candidate: Throwable): Throwable {
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

private fun preferRuntimeFatalOrNull(current: Throwable?, candidate: Throwable?): Throwable? = when {
    candidate == null -> current
    else -> preferRuntimeFatal(current, candidate)
}

private fun closeListenerExpected(listener: FrpLocalListener) {
    try {
        listener.close()
    } catch (failure: CancellationException) {
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (_: Exception) {
        // Authority has already been rejected.
    }
}

private fun closeConnectionExpected(connection: FrpLocalConnection) {
    try {
        connection.close()
    } catch (failure: CancellationException) {
        throw failure
    } catch (failure: Error) {
        throw failure
    } catch (_: Exception) {
        // Authority has already been rejected.
    }
}

private val IPV4_LOOPBACK: InetAddress = InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
private const val MINIMUM_TIMEOUT_MILLIS = 1L
private const val MAXIMUM_TIMEOUT_MILLIS = 120_000L
private const val MAXIMUM_SERVER_ADDRESS_LENGTH = 2_048
private const val MAXIMUM_FRP_NAME_LENGTH = 1_024
private const val MAXIMUM_SECRET_LENGTH = 8_192
private const val PHASE_CONNECTING = "connecting"
private const val PHASE_OPEN = "open"
private const val PHASE_RECONNECTING = "reconnecting"
private const val PHASE_FAILED = "failed"
private const val PHASE_STOPPING = "stopping"
private const val PHASE_CLOSED = "closed"
private const val VISITOR_PHASE_PENDING = "pending"
private const val VISITOR_PHASE_STARTING = "starting"
private const val VISITOR_PHASE_READY = "ready"
private const val VISITOR_PHASE_FAILED = "failed"
private const val VISITOR_PHASE_STOPPED = "stopped"
