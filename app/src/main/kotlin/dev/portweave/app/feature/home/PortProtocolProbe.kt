package dev.portweave.app.feature.home

import java.net.InetSocketAddress
import java.net.Socket
import java.nio.charset.StandardCharsets
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

enum class PortProtocol(val urlScheme: String) {
    HTTP("http"),
}

fun interface PortProtocolProbe {
    suspend fun detect(port: Int): PortProtocolProbeResult
}

sealed interface PortProtocolProbeResult {
    data class Detected(val protocol: PortProtocol) : PortProtocolProbeResult
    data object NotHttp : PortProtocolProbeResult
    data object Unavailable : PortProtocolProbeResult
}

class SocketPortProtocolProbe(
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val connectTimeoutMillis: Int = 1_000,
    private val readTimeoutMillis: Int = 1_500,
) : PortProtocolProbe {
    override suspend fun detect(port: Int): PortProtocolProbeResult = coroutineScope {
        val socket = Socket()
        val closer = launch(start = CoroutineStart.UNDISPATCHED) {
            try { awaitCancellation() } finally { socket.close() }
        }
        try {
            withContext(ioDispatcher) {
                socket.use {
                    socket.connect(
                        InetSocketAddress(IPV4_LOOPBACK, port),
                        connectTimeoutMillis,
                    )
                    socket.soTimeout = readTimeoutMillis
                    socket.getOutputStream().apply {
                        write(
                            "HEAD / HTTP/1.0\r\nHost: 127.0.0.1:$port\r\nConnection: close\r\n\r\n"
                                .toByteArray(StandardCharsets.US_ASCII),
                        )
                        flush()
                    }

                    val statusLine = buildString {
                        val input = socket.getInputStream()
                        while (length < MAX_STATUS_LINE_LENGTH) {
                            val next = input.read()
                            if (next == -1 || next == '\n'.code) break
                            if (next != '\r'.code) append(next.toChar())
                        }
                    }
                    when {
                        HTTP_STATUS_LINE.matches(statusLine) ->
                            PortProtocolProbeResult.Detected(PortProtocol.HTTP)
                        statusLine.isNotEmpty() -> PortProtocolProbeResult.NotHttp
                        else -> PortProtocolProbeResult.Unavailable
                    }
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            currentCoroutineContext().ensureActive()
            PortProtocolProbeResult.Unavailable
        } finally {
            closer.cancel()
            socket.close()
        }
    }

    private companion object {
        const val MAX_STATUS_LINE_LENGTH = 256
        const val IPV4_LOOPBACK = "127.0.0.1"
        val HTTP_STATUS_LINE = Regex("HTTP/\\d(?:\\.\\d)?[ \\t]+\\d{3}.*", RegexOption.IGNORE_CASE)
    }
}
