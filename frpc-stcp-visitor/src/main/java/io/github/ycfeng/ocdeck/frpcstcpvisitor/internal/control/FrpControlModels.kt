package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.control

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FRP_AEAD_AES_256_GCM
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FRP_AEAD_XCHACHA20_POLY1305
import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpSafeIOException
import java.util.Locale
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.delay

internal enum class FrpWireProtocol {
    V1,
    V2,
}

internal enum class FrpControlFailure(val description: String) {
    INVALID_CONFIGURATION("invalid configuration"),
    TRANSPORT_FAILED("transport failed"),
    TRANSPORT_CONNECT_FAILED("transport connect failed"),
    TRANSPORT_CONNECT_TIMEOUT("transport connect timed out"),
    TRANSPORT_TLS_SETUP_FAILED("transport tls setup failed"),
    TRANSPORT_TLS_HANDSHAKE_FAILED("transport tls handshake failed"),
    TRANSPORT_TLS_HANDSHAKE_TIMEOUT("transport tls handshake timed out"),
    TRANSPORT_READ_FAILED("transport read failed"),
    TRANSPORT_WRITE_FAILED("transport write failed"),
    TRANSPORT_CLOSED("transport closed"),
    TRANSPORT_INVALID_CONFIGURATION("transport configuration invalid"),
    YAMUX_TRANSPORT_FAILED("yamux transport failed"),
    YAMUX_PROTOCOL_FAILED("yamux protocol failed"),
    YAMUX_STREAM_OPEN_TIMEOUT("yamux stream open timed out"),
    YAMUX_TIMEOUT("yamux timed out"),
    YAMUX_FAILED("yamux failed"),
    RECONNECT_CONNECTING_TRANSPORT("control reconnect connecting transport"),
    RECONNECT_OPENING_CONTROL_STREAM("control reconnect opening control stream"),
    RECONNECT_WAITING_LOGIN("control reconnect waiting login"),
    RECONNECT_LOGIN_TIMER_SCHEDULED("control reconnect login timer scheduled"),
    RECONNECT_LOGIN_TIMER_FIRED("control reconnect login timer fired"),
    RECONNECT_ABORTING_LOGIN_TIMEOUT("control reconnect aborting login timeout"),
    RECONNECT_WAITING_LOGIN_AFTER_TIMEOUT("control reconnect waiting login after timeout"),
    RECONNECT_CLOSING_TRANSPORT("control reconnect closing transport"),
    RECONNECT_JOINING_JOBS("control reconnect joining jobs"),
    RECONNECT_CLOSING_WORK("control reconnect closing work"),
    RECONNECT_CLOSING_CLIENT_STREAMS("control reconnect closing client streams"),
    RECONNECT_CLOSING_YAMUX("control reconnect closing yamux"),
    RECONNECT_CLOSING_FAILED_ESTABLISHMENT("control reconnect closing failed establishment"),
    LOGIN_TIMEOUT("login timed out"),
    LOGIN_REJECTED("login rejected"),
    LOGIN_PROTOCOL_FAILED("login protocol failed"),
    PROTOCOL_FAILED("control protocol failed"),
    CRYPTO_FAILED("control crypto failed"),
    CRYPTO_INVALID_KEY("control crypto invalid key"),
    CRYPTO_INVALID_NONCE("control crypto invalid nonce"),
    CRYPTO_TRUNCATED_NONCE("control crypto truncated nonce"),
    CRYPTO_TRUNCATED_HEADER("control crypto truncated header"),
    CRYPTO_TRUNCATED_RECORD("control crypto truncated record"),
    CRYPTO_INVALID_LENGTH("control crypto invalid length"),
    CRYPTO_INTEGRITY_FAILED("control crypto integrity failed"),
    CRYPTO_NONCE_EXHAUSTED("control crypto nonce exhausted"),
    CRYPTO_RECORD_LIMIT("control crypto record limit"),
    CRYPTO_UNSUPPORTED_ALGORITHM("control crypto unsupported algorithm"),
    CRYPTO_IO_FAILED("control crypto io failed"),
    CRYPTO_WRITE_FAILED("control crypto write failed"),
    READ_FAILED("control read failed"),
    WRITE_FAILED("control write failed"),
    HEARTBEAT_TIMEOUT("control heartbeat timed out"),
    QUEUE_OVERFLOW("control queue limit exceeded"),
    WORK_FAILED("work connection failed"),
    CLIENT_STREAM_FAILED("client stream failed"),
    STALE_IDENTITY("control identity is stale"),
    CLOSED("control is closed"),
}

internal data class FrpControlReconnectDiagnostics(
    val establishAttemptCount: Long = 0L,
    val loginTimerScheduledCount: Long = 0L,
    val loginTimerFiredCount: Long = 0L,
    val successfulOpenCount: Long = 0L,
    val currentAttempt: Long = 0L,
    val loginTimerScheduledAttempt: Long = 0L,
    val loginTimerFiredAttempt: Long = 0L,
) {
    override fun toString(): String =
        "FrpControlReconnectDiagnostics(establishAttemptCount=$establishAttemptCount, " +
            "loginTimerScheduledCount=$loginTimerScheduledCount, " +
            "loginTimerFiredCount=$loginTimerFiredCount, successfulOpenCount=$successfulOpenCount, " +
            "currentAttempt=$currentAttempt, loginTimerScheduledAttempt=$loginTimerScheduledAttempt, " +
            "loginTimerFiredAttempt=$loginTimerFiredAttempt)"
}

internal class FrpControlException(
    val failure: FrpControlFailure,
) : FrpSafeIOException("frp control failure: ${failure.description}")

internal data class FrpControlConfig(
    val token: String = "",
    val user: String = "",
    val version: String = "0.69.1",
    val hostname: String = "android",
    val os: String = "android",
    val arch: String = normalizeFrpGoArch(System.getProperty("os.arch").orEmpty()),
    val clientId: String = "",
    val metas: Map<String, String> = emptyMap(),
    val wireProtocol: FrpWireProtocol = FrpWireProtocol.V1,
    val tlsEnabled: Boolean = true,
    val authenticateHeartbeats: Boolean = true,
    val authenticateNewWorkConnections: Boolean = true,
    val loginTimeoutMillis: Long = DEFAULT_LOGIN_TIMEOUT_MILLIS,
    val reconnectLoginTimeoutMillis: Long = DEFAULT_RECONNECT_LOGIN_TIMEOUT_MILLIS,
    val workHandshakeTimeoutMillis: Long = DEFAULT_WORK_HANDSHAKE_TIMEOUT_MILLIS,
    val heartbeatIntervalMillis: Long = DISABLED_HEARTBEAT_MILLIS,
    val heartbeatTimeoutMillis: Long = DISABLED_HEARTBEAT_MILLIS,
    val writerQueueCapacity: Int = DEFAULT_WRITER_QUEUE_CAPACITY,
    val workQueueCapacity: Int = DEFAULT_WORK_QUEUE_CAPACITY,
    val workWorkerCount: Int = DEFAULT_WORK_WORKER_COUNT,
    val maxActiveWorkConnections: Int = DEFAULT_MAX_ACTIVE_WORK_CONNECTIONS,
    val reconnectInitialDelayMillis: Long = DEFAULT_RECONNECT_INITIAL_DELAY_MILLIS,
    val reconnectMaximumDelayMillis: Long = MAXIMUM_RECONNECT_DELAY_MILLIS,
    val reconnectCleanupTimeoutMillis: Long = DEFAULT_RECONNECT_CLEANUP_TIMEOUT_MILLIS,
    val preferredAeadAlgorithms: List<String> = listOf(
        FRP_AEAD_AES_256_GCM,
        FRP_AEAD_XCHACHA20_POLY1305,
    ),
) {
    init {
        val heartbeatEnabled = heartbeatIntervalMillis > 0L
        if (version.isBlank() || os.isBlank() || arch.isBlank() ||
            loginTimeoutMillis <= 0L ||
            reconnectLoginTimeoutMillis <= 0L ||
            workHandshakeTimeoutMillis <= 0L ||
            writerQueueCapacity !in 1..MAXIMUM_QUEUE_CAPACITY ||
            workQueueCapacity !in 1..MAXIMUM_QUEUE_CAPACITY ||
            workWorkerCount !in 1..MAXIMUM_WORK_WORKERS ||
            maxActiveWorkConnections !in 1..MAXIMUM_ACTIVE_WORK_CONNECTIONS ||
            reconnectInitialDelayMillis <= 0L ||
            reconnectMaximumDelayMillis !in reconnectInitialDelayMillis..MAXIMUM_RECONNECT_DELAY_MILLIS ||
            reconnectCleanupTimeoutMillis <= 0L ||
            (heartbeatEnabled && heartbeatTimeoutMillis > 0L && heartbeatTimeoutMillis < heartbeatIntervalMillis) ||
            preferredAeadAlgorithms.isEmpty() ||
            preferredAeadAlgorithms.distinct().size != preferredAeadAlgorithms.size ||
            preferredAeadAlgorithms.any {
                it != FRP_AEAD_AES_256_GCM && it != FRP_AEAD_XCHACHA20_POLY1305
            }
        ) {
            throw FrpControlException(FrpControlFailure.INVALID_CONFIGURATION)
        }
    }

    override fun toString(): String =
        "FrpControlConfig(token=<redacted>, userPresent=${user.isNotEmpty()}, " +
            "versionPresent=${version.isNotEmpty()}, hostnamePresent=${hostname.isNotEmpty()}, " +
            "osPresent=${os.isNotEmpty()}, archPresent=${arch.isNotEmpty()}, " +
            "clientIdPresent=${clientId.isNotEmpty()}, metaCount=${metas.size}, " +
            "wireProtocol=$wireProtocol, tlsEnabled=$tlsEnabled, " +
            "authenticateHeartbeats=$authenticateHeartbeats, " +
            "authenticateNewWorkConnections=$authenticateNewWorkConnections, " +
            "loginTimeoutMillis=$loginTimeoutMillis, " +
            "reconnectLoginTimeoutMillis=$reconnectLoginTimeoutMillis, " +
            "workHandshakeTimeoutMillis=$workHandshakeTimeoutMillis, " +
            "heartbeatEnabled=${heartbeatIntervalMillis > 0L}, " +
            "writerQueueCapacity=$writerQueueCapacity, workQueueCapacity=$workQueueCapacity, " +
            "workWorkerCount=$workWorkerCount, maxActiveWorkConnections=$maxActiveWorkConnections, " +
            "reconnectCleanupTimeoutMillis=$reconnectCleanupTimeoutMillis, " +
            "aeadAlgorithmCount=${preferredAeadAlgorithms.size})"

    internal companion object {
        const val DEFAULT_LOGIN_TIMEOUT_MILLIS = 10_000L
        const val DEFAULT_RECONNECT_LOGIN_TIMEOUT_MILLIS = 60_000L
        const val DEFAULT_WORK_HANDSHAKE_TIMEOUT_MILLIS = 10_000L
        const val DISABLED_HEARTBEAT_MILLIS = -1L
        const val DEFAULT_WRITER_QUEUE_CAPACITY = 32
        const val DEFAULT_WORK_QUEUE_CAPACITY = 32
        const val DEFAULT_WORK_WORKER_COUNT = 1
        const val DEFAULT_MAX_ACTIVE_WORK_CONNECTIONS = 32
        const val DEFAULT_RECONNECT_INITIAL_DELAY_MILLIS = 1_000L
        const val MAXIMUM_RECONNECT_DELAY_MILLIS = 20_000L
        const val DEFAULT_RECONNECT_CLEANUP_TIMEOUT_MILLIS = 10_000L
        private const val MAXIMUM_QUEUE_CAPACITY = 1_024
        private const val MAXIMUM_WORK_WORKERS = 8
        private const val MAXIMUM_ACTIVE_WORK_CONNECTIONS = 32
    }
}

internal fun normalizeFrpGoArch(value: String): String {
    val normalized = value.trim().lowercase(Locale.ROOT)
    return when {
        normalized == "aarch64" || normalized == "arm64-v8a" || normalized.startsWith("arm64") -> "arm64"
        normalized == "arm" || normalized == "armeabi-v7a" || normalized.startsWith("armv7") -> "arm"
        normalized == "x86_64" || normalized == "amd64" -> "amd64"
        normalized == "x86" || normalized.matches(Regex("i[3-6]86")) -> "386"
        else -> normalized
            .replace(Regex("[^a-z0-9._-]+"), "-")
            .trim('-', '.', '_')
            .ifEmpty { "unknown" }
    }
}

internal data class FrpControlIdentity(
    val generation: Long,
    val controlEpoch: Long,
    val transportIdentity: Long,
)

internal sealed interface FrpControlState {
    val generation: Long

    data class Connecting(
        override val generation: Long,
        val attempt: Int,
    ) : FrpControlState

    data class Open(
        val identity: FrpControlIdentity,
    ) : FrpControlState {
        override val generation: Long
            get() = identity.generation
    }

    data class Retrying(
        override val generation: Long,
        val attempt: Int,
        val delayMillis: Long,
        val failure: FrpControlFailure,
    ) : FrpControlState

    data class Failed(
        override val generation: Long,
        val failure: FrpControlFailure,
    ) : FrpControlState

    data class Closed(
        override val generation: Long,
    ) : FrpControlState
}

internal fun interface FrpUnixTimeSource {
    fun nowSeconds(): Long
}

internal object SystemFrpUnixTimeSource : FrpUnixTimeSource {
    override fun nowSeconds(): Long = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())
}

internal fun interface FrpMonotonicTimeSource {
    fun nowMillis(): Long
}

internal object SystemFrpMonotonicTimeSource : FrpMonotonicTimeSource {
    override fun nowMillis(): Long = TimeUnit.NANOSECONDS.toMillis(System.nanoTime())
}

internal fun interface FrpControlDelayer {
    suspend fun delayMillis(milliseconds: Long)
}

internal object CoroutineFrpControlDelayer : FrpControlDelayer {
    override suspend fun delayMillis(milliseconds: Long) {
        delay(milliseconds)
    }
}
