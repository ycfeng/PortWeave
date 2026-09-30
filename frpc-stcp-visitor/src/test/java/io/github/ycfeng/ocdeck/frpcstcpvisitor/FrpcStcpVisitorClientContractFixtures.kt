package io.github.ycfeng.ocdeck.frpcstcpvisitor

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpClientStreamLease
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlIdentity
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control.FrpControlState
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalConnection
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalListener
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpLocalListenerFactory
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpSessionControl
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.runtime.FrpSessionControlFactory
import java.net.BindException
import java.net.InetAddress
import java.util.Collections
import java.util.LinkedHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

internal const val K4_SESSION_ENDPOINT = "k4-frps-fixture.invalid"
internal const val K4_SESSION_TOKEN = "k4-obviously-synthetic-auth-token"
internal const val K4_SESSION_USER = "k4-session-user"
internal const val K4_PRIMARY_VISITOR = "k4-primary-visitor"
internal const val K4_CONFLICT_VISITOR = "k4-conflict-visitor"
internal const val K4_SERVER_NAME = "k4-remote-service"
internal const val K4_SERVER_USER = "k4-server-user"
internal const val K4_PRIMARY_SECRET = "k4-obviously-synthetic-primary-secret"
internal const val K4_REPLACEMENT_SECRET = "k4-obviously-synthetic-replacement-secret"
internal const val K4_CONFLICT_SECRET = "k4-obviously-synthetic-conflict-secret"

internal fun k4SessionConfig(): FrpcSessionConfig = FrpcSessionConfig(
    serverAddr = "  $K4_SESSION_ENDPOINT  ",
    serverPort = 7_100,
    authToken = K4_SESSION_TOKEN,
    user = "  $K4_SESSION_USER  ",
)

internal fun k4VisitorConfig(
    name: String,
    secret: String,
    bindPort: Int,
): FrpcStcpVisitorConfig = FrpcStcpVisitorConfig(
    name = "  $name  ",
    serverName = "  $K4_SERVER_NAME  ",
    serverUser = "  $K4_SERVER_USER  ",
    secretKey = "  $secret  ",
    bindAddr = "0.0.0.0",
    bindPort = bindPort,
)

internal data class K4BindObservation(
    val loopback: Boolean,
    val requestedPort: Int,
    val listenerPort: Int,
    val listenerCreated: Boolean,
)

internal interface FrpcStcpVisitorClientContractFixture {
    val client: FrpcStcpVisitorClient

    suspend fun openControl(controlEpoch: Long)

    suspend fun awaitBindAttempt(expectedPort: Int): K4BindObservation

    fun failNextBindWithConflict()

    fun armFutureReadinessWait()

    suspend fun awaitFutureReadinessWait()

    fun releaseFutureReadinessWait()

    suspend fun shutdown()
}

/** Host-JVM bridge seam fixture; it does not load or execute the generated GoMobile AAR. */
internal class GoAdapterContractFixture : FrpcStcpVisitorClientContractFixture {
    private val json = Json {
        ignoreUnknownKeys = true
        explicitNulls = false
    }
    private val bridge = StatefulContractBridge(json)

    override val client: FrpcStcpVisitorClient = GoMobileFrpcStcpVisitorClient(json, bridge, Unit)

    override suspend fun openControl(controlEpoch: Long) {
        bridge.openControl(controlEpoch)
    }

    override suspend fun awaitBindAttempt(expectedPort: Int): K4BindObservation =
        bridge.awaitBindAttempt(expectedPort)

    override fun failNextBindWithConflict() {
        bridge.failNextBindWithConflict()
    }

    override fun armFutureReadinessWait() {
        bridge.armFutureReadinessWait()
    }

    override suspend fun awaitFutureReadinessWait() {
        bridge.awaitFutureReadinessWait()
    }

    override fun releaseFutureReadinessWait() {
        bridge.releaseFutureReadinessWait()
    }

    override suspend fun shutdown() {
        bridge.shutdown()
    }
}

internal class KotlinRuntimeContractFixture : FrpcStcpVisitorClientContractFixture {
    private val parentScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val controlFactory = ContractControlFactory()
    private val listenerFactory = ContractListenerFactory()
    private val sessionSequence = AtomicInteger(0)
    private val runtimeClient = KotlinFrpcStcpVisitorClient(
        parentScope = parentScope,
        controlFactory = controlFactory,
        listenerFactory = listenerFactory,
        sessionIdSource = FrpSessionIdSource {
            "k4-kotlin-session-${sessionSequence.incrementAndGet()}"
        },
        ioDispatcher = Dispatchers.IO,
        testOnly = Unit,
    )

    override val client: FrpcStcpVisitorClient = runtimeClient

    override suspend fun openControl(controlEpoch: Long) {
        controlFactory.single().open(controlEpoch)
    }

    override suspend fun awaitBindAttempt(expectedPort: Int): K4BindObservation =
        listenerFactory.awaitBindAttempt(expectedPort)

    override fun failNextBindWithConflict() {
        listenerFactory.failNextBindWithConflict()
    }

    override fun armFutureReadinessWait() = Unit

    override suspend fun awaitFutureReadinessWait() {
        yield()
    }

    override fun releaseFutureReadinessWait() = Unit

    override suspend fun shutdown() {
        runtimeClient.close()
        withTimeout(K4_FIXTURE_TIMEOUT_MILLIS) {
            while (runtimeClient.diagnostics().activeSessions != 0) delay(1L)
        }
        parentScope.cancel()
    }
}

private class StatefulContractBridge(
    private val json: Json,
) : GoMobileFrpcStcpVisitorBridge {
    private val lock = Any()
    private val sessionSequence = AtomicInteger(0)
    private val sessions = LinkedHashMap<String, BridgeSession>()
    private val bindAttempts = Channel<K4BindObservation>(Channel.UNLIMITED)
    private val nextBindConflict = AtomicBoolean(false)
    private val futureWaitGate = AtomicReference<FutureWaitGate?>()

    override fun startSession(configJson: String): String {
        val config = json.decodeFromString<FrpcSessionConfig>(configJson).normalizedForContract()
        val sessionId = "k4-go-adapter-session-${sessionSequence.incrementAndGet()}"
        synchronized(lock) {
            sessions[sessionId] = BridgeSession(sessionId, config)
        }
        return sessionId
    }

    override fun ensureVisitor(sessionId: String, visitorJson: String): String {
        val config = json.decodeFromString<FrpcStcpVisitorConfig>(visitorJson).normalizedForContract()
        val result = synchronized(lock) {
            val session = sessions[sessionId] ?: throw IllegalStateException("frpc session is not found")
            if (session.visitors.values.any { it.config.name != config.name && it.config.bindPort == config.bindPort }) {
                throw IllegalStateException("local bind port is already configured")
            }

            session.configRevision += 1L
            val existing = session.visitors[config.name]
            if (existing != null && existing.config == config) {
                FrpcEnsureVisitorResult(existing.config.bindPort, existing.desiredRevision)
            } else {
                val visitor = BridgeVisitor(
                    config = config,
                    desiredRevision = session.configRevision,
                    phase = K4_VISITOR_PHASE_PENDING,
                )
                session.visitors[config.name] = visitor
                if (session.phase == K4_SESSION_PHASE_OPEN) bindVisitor(session, visitor)
                FrpcEnsureVisitorResult(config.bindPort, visitor.desiredRevision)
            }
        }
        return json.encodeToString(result)
    }

    override fun waitVisitorReady(
        sessionId: String,
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ): String {
        if (timeoutMillis <= 0L) throw IllegalArgumentException("timeout must be positive")
        val normalizedName = visitorName.trim()
        var ready: FrpcVisitorReadyResult? = null
        var bindConflictMessage: String? = null
        var pending = false
        synchronized(lock) {
            val session = sessions[sessionId] ?: throw IllegalStateException("frpc session is not found")
            val visitor = session.visitors[normalizedName]
                ?: throw IllegalStateException("frpc visitor is not found")
            when {
                visitor.desiredRevision > desiredRevision ->
                    throw IllegalStateException("frpc visitor revision was superseded")
                visitor.desiredRevision < desiredRevision -> pending = true
                visitor.bindConflict -> bindConflictMessage =
                    "listen failed: address already in use: ${session.config.authToken} ${visitor.config.secretKey}"
                visitor.phase == K4_VISITOR_PHASE_READY -> ready = FrpcVisitorReadyResult(
                    name = visitor.config.name,
                    desiredRevision = visitor.desiredRevision,
                    phase = visitor.phase,
                    listenerBound = visitor.listenerBound,
                    boundControlEpoch = visitor.boundControlEpoch,
                )
                else -> pending = true
            }
        }
        bindConflictMessage?.let { throw IllegalStateException(it) }
        ready?.let { return json.encodeToString(it) }
        if (pending) {
            val gate = futureWaitGate.get()
                ?: throw IllegalStateException("visitor readiness remains pending")
            gate.entered.countDown()
            if (!gate.release.await(K4_FIXTURE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw AssertionError("Timed out releasing the bridge readiness wait")
            }
            throw IllegalStateException("visitor readiness remains pending")
        }
        throw AssertionError("Unreachable readiness state")
    }

    override fun stopVisitor(sessionId: String, visitorName: String) {
        synchronized(lock) {
            val session = sessions[sessionId] ?: return
            if (session.visitors.remove(visitorName.trim()) != null) session.configRevision += 1L
        }
    }

    override fun stopSession(sessionId: String, timeoutMillis: Long): String {
        if (timeoutMillis <= 0L) throw IllegalArgumentException("timeout must be positive")
        synchronized(lock) { sessions.remove(sessionId) }
        return json.encodeToString(FrpcStopSessionResult(sessionId, K4_SESSION_PHASE_CLOSED))
    }

    override fun getState(sessionId: String): String {
        val state = synchronized(lock) {
            val session = sessions[sessionId]
                ?: return@synchronized FrpcStcpVisitorState(sessionId, K4_SESSION_PHASE_CLOSED)
            FrpcStcpVisitorState(
                sessionId = session.id,
                phase = session.phase,
                configRevision = session.configRevision,
                controlEpoch = session.controlEpoch,
                visitors = session.visitors.mapValues { (_, visitor) ->
                    FrpcVisitorRuntimeState(
                        desiredRevision = visitor.desiredRevision,
                        phase = visitor.phase,
                        listenerBound = visitor.listenerBound,
                        boundControlEpoch = visitor.boundControlEpoch,
                        lastError = visitor.lastError,
                    )
                },
            )
        }
        return json.encodeToString(state)
    }

    override fun getRuntimeDiagnostics(): String = synchronized(lock) {
        json.encodeToString(
            FrpcRuntimeDiagnostics(
                goroutineCount = 0,
                sessionCount = sessions.size,
                ownerCounts = mapOf("contract.sessions" to sessions.size.toLong()),
                runtimeCounts = mapOf("contract.goroutines" to 0L),
            ),
        )
    }

    fun openControl(controlEpoch: Long) {
        synchronized(lock) {
            sessions.values.forEach { session ->
                session.phase = K4_SESSION_PHASE_OPEN
                session.controlEpoch = controlEpoch
                session.visitors.values
                    .filter { it.phase != K4_VISITOR_PHASE_READY }
                    .forEach { bindVisitor(session, it) }
            }
        }
    }

    suspend fun awaitBindAttempt(expectedPort: Int): K4BindObservation =
        withTimeout(K4_FIXTURE_TIMEOUT_MILLIS) {
            bindAttempts.receive().also { observation ->
                if (observation.requestedPort != expectedPort) {
                    throw AssertionError(
                        "Expected bind port $expectedPort but observed ${observation.requestedPort}",
                    )
                }
            }
        }

    fun failNextBindWithConflict() {
        check(nextBindConflict.compareAndSet(false, true))
    }

    fun armFutureReadinessWait() {
        check(futureWaitGate.compareAndSet(null, FutureWaitGate()))
    }

    suspend fun awaitFutureReadinessWait() {
        val gate = futureWaitGate.get() ?: throw AssertionError("Future readiness wait is not armed")
        val entered = withContext(Dispatchers.IO) {
            gate.entered.await(K4_FIXTURE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        }
        if (!entered) throw AssertionError("Timed out waiting for the bridge readiness call")
    }

    fun releaseFutureReadinessWait() {
        futureWaitGate.getAndSet(null)?.release?.countDown()
    }

    fun shutdown() {
        releaseFutureReadinessWait()
        synchronized(lock) { sessions.clear() }
        bindAttempts.close()
    }

    private fun bindVisitor(session: BridgeSession, visitor: BridgeVisitor) {
        val failed = nextBindConflict.compareAndSet(true, false)
        check(
            bindAttempts.trySend(
                K4BindObservation(
                    loopback = visitor.config.bindAddr == K4_LOOPBACK_ADDRESS,
                    requestedPort = visitor.config.bindPort,
                    listenerPort = visitor.config.bindPort,
                    listenerCreated = !failed,
                ),
            ).isSuccess,
        )
        if (failed) {
            visitor.phase = K4_VISITOR_PHASE_FAILED
            visitor.listenerBound = false
            visitor.boundControlEpoch = 0L
            visitor.bindConflict = true
            visitor.lastError = "local bind port is already configured"
        } else {
            visitor.phase = K4_VISITOR_PHASE_READY
            visitor.listenerBound = true
            visitor.boundControlEpoch = session.controlEpoch
            visitor.bindConflict = false
            visitor.lastError = null
        }
    }
}

private data class BridgeSession(
    val id: String,
    val config: FrpcSessionConfig,
    var phase: String = K4_SESSION_PHASE_CONNECTING,
    var configRevision: Long = 0L,
    var controlEpoch: Long = 0L,
    val visitors: LinkedHashMap<String, BridgeVisitor> = LinkedHashMap(),
)

private data class BridgeVisitor(
    val config: NormalizedContractVisitorConfig,
    val desiredRevision: Long,
    var phase: String,
    var listenerBound: Boolean = false,
    var boundControlEpoch: Long = 0L,
    var bindConflict: Boolean = false,
    var lastError: String? = null,
)

private data class NormalizedContractVisitorConfig(
    val name: String,
    val serverName: String,
    val serverUser: String,
    val secretKey: String,
    val bindAddr: String,
    val bindPort: Int,
    val useEncryption: Boolean,
    val useCompression: Boolean,
) {
    override fun toString(): String =
        "NormalizedContractVisitorConfig(serverUserPresent=${serverUser.isNotEmpty()}, " +
            "secretKey=<redacted>, bindPort=$bindPort, useEncryption=$useEncryption, " +
            "useCompression=$useCompression)"
}

private class FutureWaitGate(
    val entered: CountDownLatch = CountDownLatch(1),
    val release: CountDownLatch = CountDownLatch(1),
)

private fun FrpcSessionConfig.normalizedForContract(): FrpcSessionConfig = copy(
    serverAddr = serverAddr.trim(),
    user = user?.trim()?.takeIf(String::isNotEmpty),
    transportProtocol = transportProtocol.trim().ifEmpty { "tcp" },
    wireProtocol = wireProtocol.trim().ifEmpty { "v1" },
)

private fun FrpcStcpVisitorConfig.normalizedForContract(): NormalizedContractVisitorConfig =
    NormalizedContractVisitorConfig(
        name = name.trim(),
        serverName = serverName.trim(),
        serverUser = serverUser?.trim().orEmpty(),
        secretKey = secretKey.trim(),
        bindAddr = K4_LOOPBACK_ADDRESS,
        bindPort = bindPort,
        useEncryption = useEncryption,
        useCompression = useCompression,
    )

private class ContractControlFactory : FrpSessionControlFactory {
    private val controls = Collections.synchronizedList(mutableListOf<ContractSessionControl>())

    override fun create(
        config: FrpcSessionConfig,
        parentScope: CoroutineScope,
        generation: Long,
    ): FrpSessionControl = ContractSessionControl(generation).also(controls::add)

    fun single(): ContractSessionControl = synchronized(controls) {
        controls.singleOrNull() ?: throw AssertionError("Expected exactly one Kotlin runtime control")
    }
}

private class ContractSessionControl(
    private val generation: Long,
) : FrpSessionControl {
    private val mutableState = MutableStateFlow<FrpControlState>(
        FrpControlState.Connecting(generation, attempt = 0),
    )
    private val stopped = CompletableDeferred<Unit>()
    private val closed = AtomicBoolean(false)

    override val state: StateFlow<FrpControlState> = mutableState

    override fun start() = Unit

    fun open(controlEpoch: Long) {
        mutableState.value = FrpControlState.Open(
            FrpControlIdentity(
                generation = generation,
                controlEpoch = controlEpoch,
                transportIdentity = controlEpoch,
            ),
        )
    }

    override suspend fun openClientStream(expectedIdentity: FrpControlIdentity): FrpClientStreamLease =
        throw AssertionError("The K4 lifecycle contract does not open relay streams")

    override suspend fun awaitStopped() {
        stopped.await()
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        mutableState.value = FrpControlState.Closed(generation)
        stopped.complete(Unit)
    }

    override fun toString(): String = "ContractSessionControl(generation=$generation, closed=${closed.get()})"
}

private class ContractListenerFactory : FrpLocalListenerFactory {
    private val attempts = Channel<K4BindObservation>(Channel.UNLIMITED)
    private val nextBindConflict = AtomicBoolean(false)

    override suspend fun bind(address: InetAddress, port: Int): FrpLocalListener {
        val failed = nextBindConflict.compareAndSet(true, false)
        attempts.send(
            K4BindObservation(
                loopback = address.hostAddress == K4_LOOPBACK_ADDRESS,
                requestedPort = port,
                listenerPort = port,
                listenerCreated = !failed,
            ),
        )
        if (failed) throw BindException("synthetic K4 bind conflict")
        return ContractLocalListener(port)
    }

    suspend fun awaitBindAttempt(expectedPort: Int): K4BindObservation =
        withTimeout(K4_FIXTURE_TIMEOUT_MILLIS) {
            attempts.receive().also { observation ->
                if (observation.requestedPort != expectedPort) {
                    throw AssertionError(
                        "Expected bind port $expectedPort but observed ${observation.requestedPort}",
                    )
                }
            }
        }

    fun failNextBindWithConflict() {
        check(nextBindConflict.compareAndSet(false, true))
    }
}

private class ContractLocalListener(
    override val bindPort: Int,
) : FrpLocalListener {
    private val closed = AtomicBoolean(false)
    private val closeSignal = CompletableDeferred<Unit>()

    override suspend fun accept(): FrpLocalConnection {
        closeSignal.await()
        throw KotlinFrpcStcpVisitorException(KotlinFrpcStcpVisitorFailure.SESSION_CLOSED)
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        closeSignal.complete(Unit)
    }

    override fun toString(): String = "ContractLocalListener(closed=${closed.get()})"
}

private const val K4_FIXTURE_TIMEOUT_MILLIS = 5_000L
private const val K4_LOOPBACK_ADDRESS = "127.0.0.1"
private const val K4_SESSION_PHASE_CONNECTING = "connecting"
private const val K4_SESSION_PHASE_OPEN = "open"
private const val K4_SESSION_PHASE_CLOSED = "closed"
private const val K4_VISITOR_PHASE_PENDING = "pending"
private const val K4_VISITOR_PHASE_READY = "ready"
private const val K4_VISITOR_PHASE_FAILED = "failed"
