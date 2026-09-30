package io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.transport

import io.github.ycfeng.ocdeck.frpcstcpvisitor.internal.protocol.FrpSafeIOException
import java.io.Closeable
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.Socket

internal enum class FrpTransportFailure(val description: String) {
    INVALID_CONFIGURATION("invalid configuration"),
    CONNECT_FAILED("tcp connect failed"),
    CONNECT_TIMEOUT("tcp connect timed out"),
    TLS_SETUP_FAILED("tls setup failed"),
    TLS_HANDSHAKE_FAILED("tls handshake failed"),
    TLS_HANDSHAKE_TIMEOUT("tls handshake timed out"),
    READ_FAILED("transport read failed"),
    WRITE_FAILED("transport write failed"),
    CLOSED("transport is closed"),
}

internal class FrpTransportException(
    val failure: FrpTransportFailure,
    cause: Throwable? = null,
) : FrpSafeIOException("frp transport failure: ${failure.description}", cause)

internal data class FrpTcpEndpoint(
    val host: String,
    val port: Int,
    val tlsServerName: String? = null,
) {
    init {
        if (host.isBlank() || port !in 1..65_535) {
            throw FrpTransportException(FrpTransportFailure.INVALID_CONFIGURATION)
        }
        if (tlsServerName != null && tlsServerName.isBlank()) {
            throw FrpTransportException(FrpTransportFailure.INVALID_CONFIGURATION)
        }
    }

    val tlsHost: String get() = tlsServerName ?: host

    override fun toString(): String = "FrpTcpEndpoint(configured=true)"
}

internal data class TcpTlsTransportConfig(
    val endpoint: FrpTcpEndpoint,
    val tlsEnabled: Boolean = true,
    val tlsServerCertificateSha256: String? = null,
    val connectTimeoutMillis: Long = DEFAULT_CONNECT_TIMEOUT_MILLIS,
    val tlsHandshakeTimeoutMillis: Long = connectTimeoutMillis,
) {
    init {
        if (connectTimeoutMillis <= 0L ||
            connectTimeoutMillis > Int.MAX_VALUE.toLong() ||
            tlsHandshakeTimeoutMillis <= 0L ||
            (tlsServerCertificateSha256 != null &&
                !TLS_SERVER_CERTIFICATE_SHA256.matches(tlsServerCertificateSha256))
        ) {
            throw FrpTransportException(FrpTransportFailure.INVALID_CONFIGURATION)
        }
    }

    override fun toString(): String =
        "TcpTlsTransportConfig(endpointConfigured=true, tlsEnabled=$tlsEnabled, " +
            "tlsServerCertificateSha256=${if (tlsServerCertificateSha256 == null) "null" else "<redacted>"}, " +
            "connectTimeoutMillis=$connectTimeoutMillis, " +
            "tlsHandshakeTimeoutMillis=$tlsHandshakeTimeoutMillis)"

    internal companion object {
        const val DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000L
        val TLS_SERVER_CERTIFICATE_SHA256 = Regex("[0-9a-f]{64}")
    }
}

internal fun interface TcpSocketFactory {
    fun create(): Socket
}

internal interface TcpDialRoute {
    fun resolve(host: String): List<InetAddress>

    fun createSocket(): Socket
}

internal fun interface TcpDialRouteProvider {
    fun current(): TcpDialRoute
}

internal interface TlsClientSocket : Closeable {
    val inputStream: InputStream

    val outputStream: OutputStream

    fun startHandshake()
}

internal fun interface TlsClientSocketFactory {
    fun create(socket: Socket, endpoint: FrpTcpEndpoint): TlsClientSocket
}

internal interface FrpMuxStream {
    suspend fun read(
        destination: ByteArray,
        offset: Int = 0,
        length: Int = destination.size - offset,
    ): Int

    suspend fun write(
        source: ByteArray,
        offset: Int = 0,
        length: Int = source.size - offset,
    )

    suspend fun closeWrite()

    suspend fun reset()
}

internal interface FrpMuxConnection : Closeable {
    suspend fun openStream(): FrpMuxStream

    suspend fun awaitTermination()

    /** Completes logical cleanup after close has already synchronously released physical I/O. */
    fun forceTerminationAfterClose() = Unit

    /** Must synchronously wake physical and logical I/O. */
    override fun close()
}

internal fun interface FrpMuxConnector {
    suspend fun connect(): FrpMuxConnection
}
