package dev.portweave.tunnel.engine

import dev.portweave.core.model.TunnelKind
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow

enum class StartStage {
    VALIDATING,
    LOADING_CREDENTIALS,
    CONNECTING,
    AUTHENTICATING,
    BINDING_LOCAL_PORT,
}

sealed interface TunnelState {
    data object Stopped : TunnelState
    data object ResolvingDependencies : TunnelState
    data class WaitingDependency(val parentId: java.util.UUID) : TunnelState
    data class Starting(val stage: StartStage) : TunnelState
    data class Ready(val sinceEpochMillis: Long) : TunnelState
    /** [dormant]: no retry timer is scheduled; the next attempt waits for a network change or the app returning. */
    data class Reconnecting(val attempt: Int, val nextRetryAtEpochMillis: Long?, val dormant: Boolean = false) : TunnelState
    data object Stopping : TunnelState
    data class Blocked(val reason: BlockReason) : TunnelState
    data class Failed(val error: TunnelError) : TunnelState
}

enum class BlockReason {
    HOST_KEY_CONFIRMATION_REQUIRED,
    NETWORK_UNAVAILABLE,
    DEPENDENCY_MISSING,
    DEPENDENCY_CYCLE,
    DEPENDENCY_UNAVAILABLE,
}

enum class TunnelErrorCode {
    CONFIG_INVALID,
    DEPENDENCY_MISSING,
    DEPENDENCY_CYCLE,
    DEPENDENCY_UNAVAILABLE,
    PORT_IN_USE,
    NETWORK_UNAVAILABLE,
    DNS_FAILED,
    CONNECT_TIMEOUT,
    CONNECTION_REFUSED,
    SSH_HOST_KEY_MISMATCH,
    SSH_PRIVATE_KEY_INVALID,
    SSH_PASSPHRASE_REQUIRED,
    SSH_PASSPHRASE_INVALID,
    SSH_ALGORITHM_UNSUPPORTED,
    SSH_AUTH_FAILED,
    SSH_FORWARD_FAILED,
    STCP_LOGIN_FAILED,
    STCP_VISITOR_FAILED,
    TLS_PIN_MISMATCH,
    PROTOCOL_INCOMPATIBLE,
    SECRET_MISSING,
    STOP_TIMEOUT,
    INTERNAL,
}

data class TunnelError(
    val code: TunnelErrorCode,
    val retryable: Boolean,
    val messageKey: String,
)

data class LocalEndpoint(val host: String, val port: Int) {
    override fun toString(): String = "LocalEndpoint(host=<redacted>, port=$port)"
}

data class BackendCapabilities(
    val byteCounters: Boolean = false,
    val activeConnectionCount: Boolean = false,
    val handshakeLatency: Boolean = false,
)

data class BackendMetrics(
    val bytesUploaded: Long? = null,
    val bytesDownloaded: Long? = null,
    val activeConnections: Int? = null,
    val peakConnections: Int? = null,
    val handshakeLatencyMillis: Long? = null,
)

private val EmptyMetrics = MutableStateFlow(BackendMetrics())

interface BackendHandle {
    val kind: TunnelKind
    val localEndpoint: LocalEndpoint
    val isConnected: Boolean
    val capabilities: BackendCapabilities get() = BackendCapabilities()
    val metrics: StateFlow<BackendMetrics> get() = EmptyMetrics
    val failure: TunnelError? get() = null
    suspend fun awaitConnectionLost()
    suspend fun stop()
}
