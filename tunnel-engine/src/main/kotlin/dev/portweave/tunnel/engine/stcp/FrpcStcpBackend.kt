package dev.portweave.tunnel.engine.stcp

import dev.portweave.tunnel.engine.RuntimeWorkPolicy
import dev.portweave.tunnel.engine.RecoveringBackendHandle
import dev.portweave.tunnel.engine.BackendAvailability
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorStateObservable
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorState
import kotlinx.coroutines.flow.first
import dev.portweave.core.model.EndpointSource
import dev.portweave.core.model.TunnelConfig
import dev.portweave.core.model.TunnelKind
import dev.portweave.core.model.TunnelSpec
import dev.portweave.tunnel.engine.BackendHandle
import dev.portweave.tunnel.engine.BackendCapabilities
import dev.portweave.tunnel.engine.BackendMetrics
import dev.portweave.tunnel.engine.LocalEndpoint
import dev.portweave.tunnel.engine.TunnelError
import dev.portweave.tunnel.engine.TunnelErrorCode
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcSessionConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorClient
import io.github.ycfeng.ocdeck.frpcstcpvisitor.FrpcStcpVisitorConfig
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorException
import io.github.ycfeng.ocdeck.frpcstcpvisitor.KotlinFrpcStcpVisitorFailure
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.security.cert.CertificateException
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.SSLHandshakeException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow

fun interface FrpcStcpVisitorClientFactory {
    fun create(): FrpcStcpVisitorClient
}

class FrpcStcpBackend(
    private val clientFactory: FrpcStcpVisitorClientFactory,
    private val statePollMillis: Long = 500L,
    private val workPolicy: RuntimeWorkPolicy = RuntimeWorkPolicy(),
) {
    suspend fun start(config: TunnelConfig, credentials: StcpCredentials): BackendHandle {
        val stcp = config.spec as? TunnelSpec.Stcp
            ?: throw StcpBackendException(configError("stcp_config_required"))
        val endpoint = stcp.endpoint as? EndpointSource.Remote
            ?: throw StcpBackendException(configError("stcp_dependency_not_resolved"))
        validate(config, stcp, endpoint)
        val client = clientFactory.create()
        val startedAtNanos = System.nanoTime()
        var sessionId: String? = null
        try {
            val token = credentials.authToken?.decodeToString().orEmpty()
            val secret = credentials.secretKey.decodeToString()
            sessionId = client.startSession(
                FrpcSessionConfig(
                    serverAddr = endpoint.host,
                    serverPort = endpoint.port,
                    authToken = token,
                    user = stcp.user,
                    wireProtocol = stcp.wireProtocol.wireValue,
                    tlsServerName = stcp.identity.logicalHost,
                    tlsServerCertificateSha256 = stcp.tlsLeafPinSha256?.lowercase(),
                ),
            )
            val visitorName = "portweave_${config.id.toString().replace("-", "")}"
            val ensured = client.ensureVisitor(
                sessionId,
                FrpcStcpVisitorConfig(
                    name = visitorName,
                    serverName = stcp.serverName,
                    serverUser = stcp.serverUser,
                    secretKey = secret,
                    bindAddr = Loopback,
                    bindPort = config.localPort,
                    useEncryption = stcp.useEncryption,
                    useCompression = stcp.useCompression,
                ),
            )
            val ready = client.waitVisitorReady(
                sessionId = sessionId,
                visitorName = visitorName,
                desiredRevision = ensured.desiredRevision,
                timeoutMillis = stcp.connectTimeoutSeconds * 1_000L,
            )
            if (!ready.listenerBound || ready.phase != ReadyPhase) {
                throw StcpBackendException(
                    TunnelError(TunnelErrorCode.STCP_VISITOR_FAILED, true, "stcp_visitor_not_ready"),
                )
            }
            return FrpcStcpHandle(
                client = client,
                sessionId = sessionId,
                visitorName = visitorName,
                desiredRevision = ensured.desiredRevision,
                port = ready.let { ensured.bindPort },
                pollMillis = statePollMillis,
                workPolicy = workPolicy,
                autoRecover = config.autoRecover,
                initialEpoch = ready.boundControlEpoch,
                tlsPinConfigured = stcp.tlsLeafPinSha256 != null,
                handshakeLatencyMillis = (System.nanoTime() - startedAtNanos) / 1_000_000L,
            )
        } catch (cancelled: CancellationException) {
            sessionId?.let { runCatching { client.stopSession(it) } }
            closeClient(client)
            throw cancelled
        } catch (failure: StcpBackendException) {
            sessionId?.let { runCatching { client.stopSession(it) } }
            closeClient(client)
            throw failure
        } catch (failure: Exception) {
            val controlError = sessionId?.let { id ->
                runCatching { client.getState(id).lastError }.getOrNull()
            }
            sessionId?.let { runCatching { client.stopSession(it) } }
            closeClient(client)
            throw StcpBackendException(
                failure.toTunnelError(controlError, stcp.tlsLeafPinSha256 != null),
                failure,
            )
        }
    }

    private fun validate(config: TunnelConfig, stcp: TunnelSpec.Stcp, endpoint: EndpointSource.Remote) {
        val tlsPin = stcp.tlsLeafPinSha256
        val invalid = endpoint.host.isBlank() || endpoint.port !in 1..65_535 ||
            stcp.identity.logicalHost.isBlank() || stcp.identity.logicalPort !in 1..65_535 ||
            stcp.serverName.isBlank() || config.localPort !in 1..65_535 ||
            stcp.secretKeyRef == null || stcp.connectTimeoutSeconds !in 1..120 ||
            (tlsPin != null && !TlsPinRegex.matches(tlsPin))
        if (invalid) throw StcpBackendException(configError("stcp_config_invalid"))
    }

    private companion object {
        const val Loopback = "127.0.0.1"
        const val ReadyPhase = "ready"
        val TlsPinRegex = Regex("[0-9a-fA-F]{64}")
    }
}

class StcpCredentials(
    val authToken: ByteArray?,
    val secretKey: ByteArray,
) : AutoCloseable {
    override fun close() {
        authToken?.fill(0)
        secretKey.fill(0)
    }

    override fun toString(): String = "StcpCredentials(<redacted>)"
}

class StcpBackendException(
    val tunnelError: TunnelError,
    cause: Throwable? = null,
) : Exception(tunnelError.messageKey, cause)

private class FrpcStcpHandle(
    private val client: FrpcStcpVisitorClient,
    private val sessionId: String,
    private val visitorName: String,
    private val desiredRevision: Long,
    port: Int,
    private val pollMillis: Long,
    private val workPolicy: RuntimeWorkPolicy,
    private val autoRecover: Boolean,
    initialEpoch: Long,
    private val tlsPinConfigured: Boolean,
    handshakeLatencyMillis: Long,
) : BackendHandle, RecoveringBackendHandle {
    private val mutableAvailability = MutableStateFlow(BackendAvailability(epoch = initialEpoch))
    @Volatile override var failure: TunnelError? = null
        private set
    override val availability = mutableAvailability.asStateFlow()
    private val connected = AtomicBoolean(true)
    private val stopped = AtomicBoolean(false)
    override val kind: TunnelKind = TunnelKind.STCP
    override val localEndpoint: LocalEndpoint = LocalEndpoint("127.0.0.1", port)
    override val isConnected: Boolean get() = connected.get()
    override val capabilities = BackendCapabilities(activeConnectionCount = true, handshakeLatency = true)
    private val mutableMetrics = MutableStateFlow(
        BackendMetrics(activeConnections = 0, peakConnections = 0, handshakeLatencyMillis = handshakeLatencyMillis),
    )
    override val metrics = mutableMetrics.asStateFlow()

    override suspend fun awaitConnectionLost() {
        try {
            val observable = client as? FrpcStcpVisitorStateObservable
            if (observable != null) {
                observable.observeState(sessionId).first { state -> !acceptState(state) }
            } else {
                while (connected.get()) {
                    if (pollMillis == 500L) workPolicy.awaitNextCheck(metrics) else delay(pollMillis)
                    workPolicy.counters.increment("stcpStateReads")
                    if (!acceptState(client.getState(sessionId))) break
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            connected.set(false)
        }
    }

    private fun acceptState(state: FrpcStcpVisitorState): Boolean {
        val visitor = state.visitors[visitorName]
        visitor?.let {
            mutableMetrics.value = BackendMetrics(
                activeConnections = it.activeConnections,
                peakConnections = maxOf(it.activeConnections, mutableMetrics.value.peakConnections ?: 0),
                handshakeLatencyMillis = mutableMetrics.value.handshakeLatencyMillis,
            )
        }
        val valid = visitor != null && visitor.desiredRevision == desiredRevision
        val ready = valid && state.phase == "open" && visitor.phase == "ready" &&
            visitor.listenerBound && visitor.boundControlEpoch == state.controlEpoch
        val recovering = autoRecover && valid &&
            state.phase in setOf("connecting", "reconnecting", "open") &&
            visitor.phase in setOf("pending", "starting", "ready")
        if (!ready && !recovering) {
            failure = KotlinFrpcStcpVisitorException(KotlinFrpcStcpVisitorFailure.CONTROL_FAILED)
                .toTunnelError(state.lastError, tlsPinConfigured)
            connected.set(false)
        }
        mutableAvailability.value = BackendAvailability(ready, state.controlEpoch, !connected.get())
        return connected.get()
    }

    override suspend fun stop() {
        if (!stopped.compareAndSet(false, true)) return
        connected.set(false)
        try {
            runCatching { client.stopVisitor(sessionId, visitorName) }
            client.stopSession(sessionId)
        } finally {
            closeClient(client)
        }
    }

    override fun toString(): String =
        "FrpcStcpHandle(sessionId=<redacted>, visitorName=<redacted>, localEndpoint=$localEndpoint)"
}

private fun configError(key: String) = TunnelError(TunnelErrorCode.CONFIG_INVALID, false, key)

private fun closeClient(client: FrpcStcpVisitorClient) {
    runCatching { (client as? AutoCloseable)?.close() }
}

internal fun Throwable.toTunnelError(
    controlError: String? = null,
    tlsPinConfigured: Boolean = false,
): TunnelError {
    if (tlsPinConfigured && controlError == "transport tls handshake failed") {
        return TunnelError(TunnelErrorCode.TLS_PIN_MISMATCH, false, "stcp_tls_identity_mismatch")
    }
    if (controlError == "login rejected") {
        return TunnelError(TunnelErrorCode.STCP_LOGIN_FAILED, false, "stcp_login_rejected")
    }
    if (controlError == "login protocol failed" || controlError == "control protocol failed") {
        return TunnelError(TunnelErrorCode.PROTOCOL_INCOMPATIBLE, false, "stcp_protocol_incompatible")
    }
    val causes = generateSequence(this) { it.cause }.toList()
    if (causes.any { it is SSLHandshakeException || it is CertificateException }) {
        return TunnelError(TunnelErrorCode.TLS_PIN_MISMATCH, false, "stcp_tls_identity_mismatch")
    }
    val failure = causes.filterIsInstance<KotlinFrpcStcpVisitorException>().firstOrNull()
    if (failure != null) {
        return when (failure.failure) {
            KotlinFrpcStcpVisitorFailure.INVALID_CONFIGURATION -> configError("stcp_config_invalid")
            KotlinFrpcStcpVisitorFailure.UNSUPPORTED_CONFIGURATION ->
                TunnelError(TunnelErrorCode.PROTOCOL_INCOMPATIBLE, false, "stcp_protocol_unsupported")
            KotlinFrpcStcpVisitorFailure.BIND_PORT_CONFLICT,
            KotlinFrpcStcpVisitorFailure.LISTENER_BIND_FAILED,
            -> TunnelError(TunnelErrorCode.PORT_IN_USE, false, "stcp_local_port_unavailable")
            KotlinFrpcStcpVisitorFailure.WAIT_TIMEOUT,
            KotlinFrpcStcpVisitorFailure.VISITOR_HANDSHAKE_TIMEOUT,
            -> TunnelError(TunnelErrorCode.CONNECT_TIMEOUT, true, "stcp_ready_timeout")
            KotlinFrpcStcpVisitorFailure.CONTROL_FAILED ->
                TunnelError(TunnelErrorCode.STCP_LOGIN_FAILED, true, "stcp_control_failed")
            KotlinFrpcStcpVisitorFailure.VISITOR_HANDSHAKE_FAILED,
            KotlinFrpcStcpVisitorFailure.VISITOR_FAILED,
            KotlinFrpcStcpVisitorFailure.RELAY_FAILED,
            KotlinFrpcStcpVisitorFailure.RELAY_LIMIT,
            -> TunnelError(TunnelErrorCode.STCP_VISITOR_FAILED, true, "stcp_visitor_failed")
            KotlinFrpcStcpVisitorFailure.STOP_TIMEOUT ->
                TunnelError(TunnelErrorCode.STOP_TIMEOUT, true, "stcp_stop_timeout")
            KotlinFrpcStcpVisitorFailure.CLIENT_CLOSED,
            KotlinFrpcStcpVisitorFailure.SESSION_CLOSED,
            KotlinFrpcStcpVisitorFailure.SESSION_NOT_FOUND,
            KotlinFrpcStcpVisitorFailure.VISITOR_NOT_FOUND,
            KotlinFrpcStcpVisitorFailure.VISITOR_SUPERSEDED,
            -> TunnelError(TunnelErrorCode.STCP_VISITOR_FAILED, true, "stcp_session_changed")
            KotlinFrpcStcpVisitorFailure.SESSION_LIMIT,
            KotlinFrpcStcpVisitorFailure.MAILBOX_FULL,
            KotlinFrpcStcpVisitorFailure.VISITOR_LIMIT,
            -> TunnelError(TunnelErrorCode.STCP_VISITOR_FAILED, true, "stcp_resource_limit")
        }
    }
    return when (this) {
        is SocketTimeoutException -> TunnelError(TunnelErrorCode.CONNECT_TIMEOUT, true, "stcp_connect_timeout")
        is ConnectException -> TunnelError(TunnelErrorCode.CONNECTION_REFUSED, true, "stcp_connection_refused")
        else -> TunnelError(TunnelErrorCode.INTERNAL, false, "stcp_internal_error")
    }
}
