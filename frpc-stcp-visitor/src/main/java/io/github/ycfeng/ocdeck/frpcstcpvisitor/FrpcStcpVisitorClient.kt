@file:OptIn(ExperimentalSerializationApi::class)

package io.github.ycfeng.ocdeck.frpcstcpvisitor

import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable

/**
 * Stable Kotlin boundary for an frpc STCP visitor backend.
 *
 * Keep this API limited to simple value objects so backend implementations can
 * map them to serialized payloads and primitive return values without exposing frp internals.
 */
interface FrpcStcpVisitorClient {
    suspend fun startSession(config: FrpcSessionConfig): String

    suspend fun ensureVisitor(
        sessionId: String,
        visitor: FrpcStcpVisitorConfig,
    ): FrpcEnsureVisitorResult

    suspend fun waitVisitorReady(
        sessionId: String,
        visitorName: String,
        desiredRevision: Long,
        timeoutMillis: Long,
    ): FrpcVisitorReadyResult

    suspend fun stopVisitor(sessionId: String, visitorName: String)

    suspend fun stopSession(
        sessionId: String,
        timeoutMillis: Long = DEFAULT_STOP_TIMEOUT_MILLIS,
    ): FrpcStopSessionResult

    suspend fun getState(sessionId: String): FrpcStcpVisitorState

    companion object {
        const val DEFAULT_STOP_TIMEOUT_MILLIS = 10_000L
    }
}

/** Emits the current snapshot immediately, including closed for a missing session.
 * Collectors must handle terminal phases and cancel when their owner stops.
 */
interface FrpcStcpVisitorStateObservable {
    suspend fun observeState(sessionId: String): kotlinx.coroutines.flow.Flow<FrpcStcpVisitorState>
}

/** Optional capability for backends that can recycle an active control transport without replacing its session. */
interface FrpcStcpVisitorNetworkChangeAware {
    suspend fun reconnectAfterDefaultNetworkChange()
}

/**
 * Optional host hook that replaces the control connection's built-in reconnect delay, for example to back off
 * further or to pause entirely while the host app is in the background. Without one, reconnect timing stays
 * frpc-compatible (1s doubling up to 20s).
 */
fun interface FrpcReconnectGate {
    /** Suspends until reconnect [attempt] (1-based) may start; [defaultDelayMillis] is the built-in delay. */
    suspend fun awaitReconnect(attempt: Int, defaultDelayMillis: Long)
}

/** Optional aggregate diagnostics used only by bounded backend lifecycle evidence. */
interface FrpcStcpVisitorRuntimeDiagnostics {
    suspend fun captureRuntimeDiagnostics(): FrpcRuntimeDiagnostics
}

@Serializable
data class FrpcRuntimeDiagnostics(
    val goroutineCount: Int,
    val sessionCount: Int,
    val ownerCounts: Map<String, Long> = emptyMap(),
    val runtimeCounts: Map<String, Long> = emptyMap(),
)

@Serializable
data class FrpcSessionConfig(
    val serverAddr: String,
    @EncodeDefault
    val serverPort: Int = 7000,
    val authToken: String,
    val user: String? = null,
    val transportProtocol: String = "tcp",
    val wireProtocol: String = "v1",
    val tlsServerName: String? = null,
    val tlsServerCertificateSha256: String? = null,
) {
    override fun toString(): String =
        "FrpcSessionConfig(serverPort=$serverPort, authToken=<redacted>, userPresent=${user != null}, " +
            "tlsServerNamePresent=${tlsServerName != null}, " +
            "tlsServerCertificateSha256=${if (tlsServerCertificateSha256 == null) "null" else "<redacted>"})"
}

@Serializable
data class FrpcStcpVisitorConfig(
    val name: String,
    val serverName: String,
    val serverUser: String? = null,
    val secretKey: String,
    val bindAddr: String = "127.0.0.1",
    @EncodeDefault
    val bindPort: Int = 4096,
    val useEncryption: Boolean = false,
    val useCompression: Boolean = false,
) {
    override fun toString(): String =
        "FrpcStcpVisitorConfig(secretKey=<redacted>, serverUserPresent=${serverUser != null}, " +
            "bindPort=$bindPort, useEncryption=$useEncryption, useCompression=$useCompression)"
}

@Serializable
data class FrpcEnsureVisitorResult(
    val bindPort: Int,
    val desiredRevision: Long,
)

@Serializable
data class FrpcVisitorReadyResult(
    val name: String,
    val desiredRevision: Long,
    val phase: String,
    val listenerBound: Boolean,
    val boundControlEpoch: Long,
    val lastError: String? = null,
) {
    override fun toString(): String =
        "FrpcVisitorReadyResult(desiredRevision=$desiredRevision, phase=$phase, " +
            "listenerBound=$listenerBound, boundControlEpoch=$boundControlEpoch, " +
            "lastErrorPresent=${lastError != null})"
}

@Serializable
data class FrpcVisitorRuntimeState(
    val desiredRevision: Long,
    val phase: String,
    val listenerBound: Boolean,
    val boundControlEpoch: Long,
    val lastError: String? = null,
    val activeConnections: Int = 0,
) {
    override fun toString(): String =
        "FrpcVisitorRuntimeState(desiredRevision=$desiredRevision, phase=$phase, " +
            "listenerBound=$listenerBound, boundControlEpoch=$boundControlEpoch, " +
        "lastErrorPresent=${lastError != null}, activeConnections=$activeConnections)"
}

@Serializable
data class FrpcStcpVisitorState(
    val sessionId: String,
    val phase: String,
    val configRevision: Long = 0,
    val controlEpoch: Long = 0,
    val lastError: String? = null,
    val visitors: Map<String, FrpcVisitorRuntimeState> = emptyMap(),
) {
    override fun toString(): String =
        "FrpcStcpVisitorState(phase=$phase, configRevision=$configRevision, " +
            "controlEpoch=$controlEpoch, lastErrorPresent=${lastError != null}, " +
            "visitorCount=${visitors.size})"
}

@Serializable
data class FrpcStopSessionResult(
    val sessionId: String,
    val phase: String,
) {
    override fun toString(): String = "FrpcStopSessionResult(phase=$phase)"
}
